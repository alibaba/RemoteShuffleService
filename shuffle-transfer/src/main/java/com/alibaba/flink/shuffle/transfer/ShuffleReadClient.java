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

import com.alibaba.flink.shuffle.core.ids.ChannelID;
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.transfer.TransferMessage.CloseChannel;
import com.alibaba.flink.shuffle.transfer.TransferMessage.ReadAddCredit;
import com.alibaba.flink.shuffle.transfer.TransferMessage.ReadData;
import com.alibaba.flink.shuffle.transfer.TransferMessage.ReadHandshakeRequest;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.function.Consumer;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkArgument;
import static com.alibaba.flink.shuffle.common.utils.CommonUtils.randomBytes;
import static com.alibaba.flink.shuffle.common.utils.ProtocolUtils.currentProtocolVersion;
import static com.alibaba.flink.shuffle.common.utils.ProtocolUtils.emptyExtraMessage;
import static com.alibaba.flink.shuffle.common.utils.ProtocolUtils.emptyOffset;

/**
 * Reader client used to retrieve buffers from a remote shuffle worker to Flink TM. It talks with a
 * shuffle worker using Netty connection by language of {@link TransferMessage}. Flow control is
 * guaranteed by a credit based mechanism. The whole process of communication between {@link
 * ShuffleReadClient} and shuffle worker could be described as below:
 *
 * <ul>
 *   <li>1. Client opens connection and sends {@link ReadHandshakeRequest}, which contains number of
 *       initial credits -- indicates how many buffers it can accept;
 *   <li>2. Server sends {@link ReadData} by the view of the number of credits from client side.
 *       {@link ReadData} contains backlog information -- indicates how many more buffers to send;
 *   <li>3. Client allocates more buffers and sends {@link ReadAddCredit} to notify more credits;
 *   <li>4. Repeat from step-2 to step-3;
 *   <li>5. Client sends {@link CloseChannel} to server;
 * </ul>
 */
public class ShuffleReadClient extends CreditListener {

    private static final Logger LOG = LoggerFactory.getLogger(ShuffleReadClient.class);

    /** Address of shuffle worker. */
    private final InetSocketAddress address;

    /** String representation the remote shuffle address. */
    private final String addressStr;

    /** Used to set up connections. */
    private final ConnectionManager connectionManager;

    /** Pool to allocate buffers. */
    private final TransferBufferPool bufferPool;

    /** Index of the first logic Subpartition to be read (inclusive). */
    private final int startSubIdx;

    /** Index of the last logic Subpartition to be read (inclusive). */
    private final int endSubIdx;

    /** Size of buffer to receive shuffle data. */
    private final int bufferSize;

    /** {@link DataSetID} of the reading. */
    private final DataSetID dataSetID;

    /** {@link MapPartitionID} of the reading. */
    private final MapPartitionID mapID;

    /** Identifier of the channel. */
    private final ChannelID channelID;

    /** String of channelID. */
    private final String channelIDStr;

    /** {@link ReadClientHandler} back this write-client. */
    private volatile ReadClientHandler readClientHandler;

    /** Listener to notify data received. */
    private final Consumer<ByteBuf> dataListener;

    /** Listener to notify failure. */
    private final Consumer<Throwable> failureListener;

    /** Netty channel. */
    private volatile Channel nettyChannel;

    /** {@link Throwable} when failure. */
    private Throwable cause;

    /** Whether the channel is closed. */
    private volatile boolean closed;

    public ShuffleReadClient(
            InetSocketAddress address,
            DataSetID dataSetID,
            MapPartitionID mapID,
            int startSubIdx,
            int endSubIdx,
            int bufferSize,
            TransferBufferPool bufferPool,
            ConnectionManager connectionManager,
            Consumer<ByteBuf> dataListener,
            Consumer<Throwable> failureListener) {

        checkArgument(address != null, "Must be not null.");
        checkArgument(dataSetID != null, "Must be not null.");
        checkArgument(mapID != null, "Must be not null.");
        checkArgument(startSubIdx >= 0, "Must be positive value.");
        checkArgument(endSubIdx >= startSubIdx, "Must be equal or larger than startSubIdx.");
        checkArgument(bufferSize > 0, "Must be positive value.");
        checkArgument(bufferPool != null, "Must be not null.");
        checkArgument(connectionManager != null, "Must be not null.");
        checkArgument(dataListener != null, "Must be not null.");
        checkArgument(failureListener != null, "Must be not null.");

        this.address = address;
        this.addressStr = address.toString();
        this.dataSetID = dataSetID;
        this.mapID = mapID;
        this.startSubIdx = startSubIdx;
        this.endSubIdx = endSubIdx;
        this.bufferSize = bufferSize;
        this.bufferPool = bufferPool;
        this.connectionManager = connectionManager;
        this.dataListener = dataListener;
        this.failureListener = failureListener;
        this.channelID = new ChannelID(randomBytes(16));
        this.channelIDStr = channelID.toString();
    }

    /** Create Netty connection to remote. */
    public void connect() throws IOException, InterruptedException {
        LOG.debug("(remote: {}, channel: {}) Connect channel.", address, channelIDStr);
        nettyChannel = connectionManager.getChannel(channelID, address);
    }

    /** Fire handshake. */
    public void open() throws IOException {
        readClientHandler = nettyChannel.pipeline().get(ReadClientHandler.class);
        if (readClientHandler == null) {
            throw new IOException(
                    "The network connection is already released for channelID: " + channelIDStr);
        }
        readClientHandler.register(this);

        ReadHandshakeRequest handshake =
                new ReadHandshakeRequest(
                        currentProtocolVersion(),
                        channelID,
                        dataSetID,
                        mapID,
                        startSubIdx,
                        endSubIdx,
                        0,
                        bufferSize,
                        emptyOffset(),
                        emptyExtraMessage());
        LOG.debug("(remote: {}) Send {}.", nettyChannel.remoteAddress(), handshake);
        nettyChannel
                .writeAndFlush(handshake)
                .addListener(
                        new ChannelFutureListenerImpl(
                                (ignored, throwable) -> exceptionCaught(throwable)));
    }

    public boolean isOpened() {
        return readClientHandler != null;
    }

    /** Get identifier of the channel. */
    public ChannelID getChannelID() {
        return channelID;
    }

    /** Called by Netty thread. */
    public void dataReceived(ReadData readData) {
        LOG.trace("(remote: {}, channel: {}) Received {}.", address, channelIDStr, readData);
        if (closed) {
            readData.getBuffer().release();
            return;
        }
        dataListener.accept(readData.getBuffer());
    }

    /** Called by Netty thread. */
    public void backlogReceived(int backlog) {
        bufferPool.reserveBuffers(this, backlog);
    }

    /** Called by Netty thread. */
    public void channelInactive() {
        if (closed || cause != null) {
            return;
        }
        cause =
                new IOException(
                        "Shuffle failure on connection to "
                                + address
                                + " for channel of "
                                + channelIDStr,
                        new ClosedChannelException());
        failureListener.accept(cause);
    }

    /** Called by Netty thread. */
    public void exceptionCaught(Throwable t) {
        if (cause != null) {
            return;
        }
        if (t != null) {
            cause =
                    new IOException(
                            "Shuffle failure on connection to "
                                    + address
                                    + " for channel of "
                                    + channelIDStr
                                    + ", cause: "
                                    + t.getMessage(),
                            t);
        } else {
            cause =
                    new IOException(
                            "Shuffle failure on connection to "
                                    + address
                                    + " for channel of "
                                    + channelIDStr);
        }
        failureListener.accept(cause);
    }

    /** Called by Netty thread to request buffer to receive data. */
    public ByteBuf requestBuffer() {
        return bufferPool.requestBuffer();
    }

    public Throwable getCause() {
        return cause;
    }

    /** Closes Netty connection -- called from task thread. */
    public void close() throws IOException {
        closed = true;
        LOG.debug(
                "(remote: {}, channel: {}) Close for (dataSetID: {}, mapID: {}, startSubIdx: {}, endSubIdx: {}).",
                address,
                channelIDStr,
                dataSetID,
                mapID,
                startSubIdx,
                endSubIdx);
        if (nettyChannel != null) {
            connectionManager.releaseChannel(address, channelID);
        }

        if (readClientHandler != null) {
            readClientHandler.unregister(this);
        }
    }

    // Called from both task thread and netty thread.
    @Override
    public void notifyAvailableCredits(int numCredits) {
        ReadAddCredit addCredit =
                new ReadAddCredit(
                        currentProtocolVersion(), channelID, numCredits, emptyExtraMessage());
        readClientHandler.notifyReadCredit(addCredit);
    }
}
