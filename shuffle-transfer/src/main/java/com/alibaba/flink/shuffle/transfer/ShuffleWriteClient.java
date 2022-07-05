/*
 * Copyright 2021 The Flink Remote Shuffle Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.flink.shuffle.transfer;

import com.alibaba.flink.shuffle.common.utils.ExceptionUtils;
import com.alibaba.flink.shuffle.core.ids.ChannelID;
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkArgument;
import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkState;
import static com.alibaba.flink.shuffle.common.utils.CommonUtils.randomBytes;
import static com.alibaba.flink.shuffle.common.utils.ProtocolUtils.currentProtocolVersion;
import static com.alibaba.flink.shuffle.common.utils.ProtocolUtils.emptyExtraMessage;

/**
 * Writer client used to send buffers to a remote shuffle worker. It talks with a shuffle worker
 * using Netty connection by language of {@link TransferMessage}. Flow control is guaranteed by a
 * credit based mechanism. The whole process of communication between {@link ShuffleWriteClient} and
 * shuffle worker could be described as below:
 *
 * <ul>
 *   <li>1. Client opens connection and sends {@link TransferMessage.WriteHandshakeRequest};
 *   <li>2. Client sends {@link TransferMessage.WriteRegionStart}, which announces the start of a
 *       writing region and also indicates the number of buffers inside the region, we call it
 *       'backlog';
 *   <li>3. Server sends {@link TransferMessage.WriteAddCredit} to announce how many more buffers it
 *       can accept;
 *   <li>4. Client sends {@link TransferMessage.WriteData} based on server side 'credit';
 *   <li>5. Client sends {@link TransferMessage.WriteRegionFinish} to indicate writing finish of a
 *       region;
 *   <li>6. Repeat from step-2 to step-5;
 *   <li>7. Client sends {@link TransferMessage.WriteFinish} to indicate writing finish;
 *   <li>8. Server sends {@link TransferMessage.WriteFinishCommit} to confirm the writing finish.
 *   <li>9. Client sends {@link TransferMessage.CloseChannel} to server.
 * </ul>
 */
public class ShuffleWriteClient {

    private static final Logger LOG = LoggerFactory.getLogger(ShuffleWriteClient.class);

    /** Address of shuffle worker. */
    private final InetSocketAddress address;

    /** String representation the remote shuffle address. */
    private final String addressStr;

    /** {@link MapPartitionID} of the writing. */
    private final MapPartitionID mapID;

    /** {@link JobID} of the writing. */
    private final JobID jobID;

    /** {@link DataSetID} of the writing. */
    private final DataSetID dataSetID;

    /** Number of subpartitions of the writing. */
    private final int numSubs;

    /** Defines the buffer size used by client. */
    private final int bufferSize;

    /** Target data partition type to write. */
    private final String dataPartitionFactoryName;

    /** Used to set up and release connections. */
    private final ConnectionManager connectionManager;

    /** Lock to protect {@link #currentCredit}. */
    private final Object lock = new Object();

    /** Netty channel. */
    private Channel nettyChannel;

    /** Current view of the sum of credits received from remote shuffle worker for the channel. */
    @GuardedBy("lock")
    private int currentCredit;

    /** Current writing region index, used for outdating credits. */
    @GuardedBy("lock")
    private int currentRegionIdx;

    /** Identifier of the channel. */
    private final ChannelID channelID;

    /** String of channelID. */
    private final String channelIDStr;

    /** {@link WriteClientHandler} back this write-client. */
    private WriteClientHandler writeClientHandler;

    /** Whether task thread is waiting for more credits for sending. */
    private volatile boolean isWaitingForCredit;

    /** Whether task thread is waiting for {@link TransferMessage.WriteFinishCommit}. */
    private volatile boolean isWaitingForFinishCommit;

    /** Whether {@link TransferMessage.WriteFinishCommit} is already received. */
    private volatile boolean finishCommitted;

    /** {@link Throwable} when writing failure. */
    private volatile Throwable cause;

    /** If closed ever. */
    private volatile boolean closed;

    /** Callback when write channel. */
    private final ChannelFutureListenerImpl channelFutureListener =
            new ChannelFutureListenerImpl((channelFuture, cause) -> handleFailure(cause));

    /**
     * @param address Address of shuffle worker.
     * @param jobID {@link JobID} of the writing.
     * @param dataSetID {@link DataSetID} of the writing.
     * @param mapID {@link MapPartitionID} of the writing.
     * @param numSubs Number of subpartitions of the writing.
     * @param connectionManager Manages physical connections.
     */
    public ShuffleWriteClient(
            InetSocketAddress address,
            JobID jobID,
            DataSetID dataSetID,
            MapPartitionID mapID,
            int numSubs,
            int bufferSize,
            String dataPartitionFactoryName,
            ConnectionManager connectionManager) {

        checkArgument(address != null, "Must be not null.");
        checkArgument(jobID != null, "Must be not null.");
        checkArgument(dataSetID != null, "Must be not null.");
        checkArgument(mapID != null, "Must be not null.");
        checkArgument(numSubs > 0, "Must be positive value.");
        checkArgument(bufferSize > 0, "Must be positive value.");
        checkArgument(dataPartitionFactoryName != null, "Must be not null.");
        checkArgument(connectionManager != null, "Must be not null.");

        this.address = address;
        this.addressStr = address.toString();
        this.mapID = mapID;
        this.jobID = jobID;
        this.dataSetID = dataSetID;
        this.numSubs = numSubs;
        this.bufferSize = bufferSize;
        this.dataPartitionFactoryName = dataPartitionFactoryName;
        this.connectionManager = connectionManager;
        this.channelID = new ChannelID(randomBytes(16));
        this.channelIDStr = channelID.toString();
    }

    /** Initialize Netty connection and fire handshake. */
    public void open() throws IOException, InterruptedException {
        LOG.debug("(remote: {}, channel: {}) Connect channel.", address, channelIDStr);
        nettyChannel = connectionManager.getChannel(channelID, address);
        writeClientHandler = nettyChannel.pipeline().get(WriteClientHandler.class);
        if (writeClientHandler == null) {
            throw new IOException(
                    "The network connection is already released for channelID: " + channelIDStr);
        }
        writeClientHandler.register(this);

        TransferMessage.WriteHandshakeRequest msg =
                new TransferMessage.WriteHandshakeRequest(
                        currentProtocolVersion(),
                        channelID,
                        jobID,
                        dataSetID,
                        mapID,
                        numSubs,
                        bufferSize,
                        dataPartitionFactoryName,
                        emptyExtraMessage());
        LOG.debug("(remote: {}, channel: {}) Send {}.", address, channelIDStr, msg);
        writeAndFlush(msg);
    }

    /** Writes a piece of data to a subpartition. */
    public void write(ByteBuf byteBuf, int subIdx) throws InterruptedException {
        synchronized (lock) {
            try {
                healthCheck();
                checkState(
                        currentCredit >= 0,
                        () ->
                                "BUG: credit smaller than 0: "
                                        + currentCredit
                                        + ", channelID="
                                        + channelIDStr);
                if (currentCredit == 0) {
                    isWaitingForCredit = true;
                    while (currentCredit == 0 && cause == null && !closed) {
                        lock.wait();
                    }
                    isWaitingForCredit = false;
                    healthCheck();
                    checkState(
                            currentCredit > 0,
                            () ->
                                    "BUG: credit should be positive, but got "
                                            + currentCredit
                                            + ", channelID="
                                            + channelIDStr);
                }
            } catch (Throwable t) {
                byteBuf.release();
                throw t;
            }

            int size = byteBuf.readableBytes();
            TransferMessage.WriteData writeData =
                    new TransferMessage.WriteData(
                            currentProtocolVersion(),
                            channelID,
                            byteBuf,
                            subIdx,
                            size,
                            false,
                            emptyExtraMessage());
            LOG.trace("(remote: {}, channel: {}) Send {}.", address, channelIDStr, writeData);
            writeAndFlush(writeData);
            currentCredit--;
        }
    }

    /**
     * Indicates the start of a region. A region of buffers guarantees the records inside are
     * completed.
     *
     * @param isBroadcast Whether it's a broadcast region.
     */
    public void regionStart(boolean isBroadcast) {
        synchronized (lock) {
            healthCheck();
            TransferMessage.WriteRegionStart writeRegionStart =
                    new TransferMessage.WriteRegionStart(
                            currentProtocolVersion(),
                            channelID,
                            currentRegionIdx,
                            isBroadcast,
                            emptyExtraMessage());
            LOG.debug(
                    "(remote: {}, channel: {}) Send {}.", address, channelIDStr, writeRegionStart);
            writeAndFlush(writeRegionStart);
        }
    }

    /**
     * Indicates the finish of a region. A region is always bounded by a pair of region-start and
     * region-finish.
     */
    public void regionFinish() {
        synchronized (lock) {
            healthCheck();
            TransferMessage.WriteRegionFinish writeRegionFinish =
                    new TransferMessage.WriteRegionFinish(
                            currentProtocolVersion(), channelID, emptyExtraMessage());
            LOG.debug(
                    "(remote: {}, channel: {}) Region({}) finished, send {}.",
                    address,
                    channelIDStr,
                    currentRegionIdx,
                    writeRegionFinish);
            currentRegionIdx++;
            currentCredit = 0;
            writeAndFlush(writeRegionFinish);
        }
    }

    /** Indicates the writing is finished. */
    public void finish() throws InterruptedException {
        synchronized (lock) {
            healthCheck();
            TransferMessage.WriteFinish writeFinish =
                    new TransferMessage.WriteFinish(
                            currentProtocolVersion(), channelID, emptyExtraMessage());
            LOG.debug("(remote: {}, channel: {}) Send {}.", address, channelIDStr, writeFinish);
            writeAndFlush(writeFinish);
            if (!finishCommitted) {
                isWaitingForFinishCommit = true;
                while (!finishCommitted && cause == null && !closed) {
                    lock.wait();
                }
                isWaitingForFinishCommit = false;
                healthCheck();
                checkState(finishCommitted, "finishCommitted should be true.");
            }
        }
    }

    /** Closes Netty connection. */
    public void close() throws IOException {
        synchronized (lock) {
            closed = true;
            lock.notifyAll();
        }

        LOG.debug("(remote: {}) Close for (dataSetID: {}, mapID: {}).", address, dataSetID, mapID);
        if (writeClientHandler != null) {
            writeClientHandler.unregister(this);
        }

        if (nettyChannel != null) {
            connectionManager.releaseChannel(address, channelID);
        }
    }

    /** Whether task thread is waiting for more credits for sending. */
    public boolean isWaitingForCredit() {
        return isWaitingForCredit;
    }

    /** Whether task thread is waiting for {@link TransferMessage.WriteFinishCommit}. */
    public boolean isWaitingForFinishCommit() {
        return isWaitingForFinishCommit;
    }

    /** Identifier of the channel. */
    public ChannelID getChannelID() {
        return channelID;
    }

    /** Get {@link Throwable} when writing failure. */
    public Throwable getCause() {
        return cause;
    }

    /** Called by Netty thread. */
    public void writeFinishCommitReceived(TransferMessage.WriteFinishCommit commit) {
        LOG.debug("(remote: {}, channel: {}) Received {}.", address, channelIDStr, commit);
        synchronized (lock) {
            finishCommitted = true;
            if (isWaitingForFinishCommit) {
                lock.notifyAll();
            }
        }
    }

    /** Called by Netty thread. */
    public void creditReceived(TransferMessage.WriteAddCredit addCredit) {
        LOG.trace("(remote: {}, channel: {}) Received {}.", address, channelIDStr, addCredit);
        synchronized (lock) {
            if (addCredit.getCredit() > 0 && addCredit.getRegionIdx() == currentRegionIdx) {
                currentCredit += addCredit.getCredit();
                if (isWaitingForCredit) {
                    lock.notifyAll();
                }
            }
        }
    }

    /** Called by Netty thread. */
    public void channelInactive() {
        synchronized (lock) {
            if (!closed) {
                handleFailure(new ClosedChannelException());
            }
        }
    }

    /** Called by Netty thread. */
    public void exceptionCaught(Throwable t) {
        synchronized (lock) {
            handleFailure(t);
        }
    }

    private void healthCheck() {
        if (cause != null) {
            ExceptionUtils.rethrowAsRuntimeException(cause);
        }
        if (closed) {
            throw new IllegalStateException("Write client is already cancelled/closed.");
        }
    }

    private void writeAndFlush(Object obj) {
        nettyChannel.writeAndFlush(obj).addListener(channelFutureListener);
    }

    private void handleFailure(Throwable t) {
        synchronized (lock) {
            if (cause != null) {
                return;
            }
            if (t != null) {
                cause =
                        new IOException(
                                "Shuffle failure on connection to "
                                        + address
                                        + " for channel of "
                                        + channelIDStr,
                                t);
            } else {
                cause =
                        new Exception(
                                "Shuffle failure on connection to "
                                        + address
                                        + " for channel of "
                                        + channelIDStr);
            }
            LOG.error("(remote: {}, channel: {}) Shuffle failure.", address, channelIDStr, cause);
            lock.notifyAll();
        }
    }
}
