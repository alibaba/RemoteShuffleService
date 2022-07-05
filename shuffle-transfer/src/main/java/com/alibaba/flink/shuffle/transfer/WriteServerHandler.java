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

import com.alibaba.flink.shuffle.common.exception.ShuffleException;
import com.alibaba.flink.shuffle.common.functions.BiConsumerWithException;
import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.common.utils.ExceptionUtils;
import com.alibaba.flink.shuffle.core.ids.ChannelID;
import com.alibaba.flink.shuffle.core.storage.PartitionedDataStore;
import com.alibaba.flink.shuffle.transfer.TransferMessage.CloseChannel;
import com.alibaba.flink.shuffle.transfer.TransferMessage.CloseConnection;
import com.alibaba.flink.shuffle.transfer.TransferMessage.ErrorResponse;
import com.alibaba.flink.shuffle.transfer.TransferMessage.Heartbeat;
import com.alibaba.flink.shuffle.transfer.TransferMessage.WriteAddCredit;
import com.alibaba.flink.shuffle.transfer.TransferMessage.WriteData;
import com.alibaba.flink.shuffle.transfer.TransferMessage.WriteFinish;
import com.alibaba.flink.shuffle.transfer.TransferMessage.WriteFinishCommit;
import com.alibaba.flink.shuffle.transfer.TransferMessage.WriteHandshakeRequest;
import com.alibaba.flink.shuffle.transfer.TransferMessage.WriteRegionFinish;
import com.alibaba.flink.shuffle.transfer.TransferMessage.WriteRegionStart;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.flink.shaded.netty4.io.netty.handler.timeout.IdleStateEvent;

import com.alibaba.metrics.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;
import static com.alibaba.flink.shuffle.common.utils.ProtocolUtils.currentProtocolVersion;
import static com.alibaba.flink.shuffle.common.utils.ProtocolUtils.emptyExtraMessage;

/** A {@link ChannelInboundHandler} serves shuffle write process on server side. */
public class WriteServerHandler extends SimpleChannelInboundHandler<TransferMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(WriteServerHandler.class);

    /** Service logic underground. */
    private final WritingService writingService;

    /** Identifier of current channel under serving. */
    private ChannelID currentChannelID;

    /** Heartbeat interval. */
    private final int heartbeatInterval;

    /** {@link ScheduledFuture} for heartbeat. */
    private ScheduledFuture<?> heartbeatFuture;

    /** If connection closed. */
    private boolean connectionClosed;

    private final Counter numWritingConnections;

    /**
     * @param dataStore Implementation of storage layer.
     * @param heartbeatInterval Heartbeat interval in seconds.
     */
    public WriteServerHandler(PartitionedDataStore dataStore, int heartbeatInterval) {
        this.writingService = new WritingService(dataStore);
        this.currentChannelID = null;
        this.heartbeatInterval = heartbeatInterval;
        this.connectionClosed = false;
        this.numWritingConnections = NetworkMetricsUtil.registerNumWritingConnections();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        if (heartbeatInterval > 0) {
            heartbeatFuture =
                    ctx.executor()
                            .scheduleAtFixedRate(
                                    () -> ctx.writeAndFlush(new Heartbeat()),
                                    0,
                                    heartbeatInterval,
                                    TimeUnit.SECONDS);
        }
        super.channelActive(ctx);
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        super.channelRegistered(ctx);
        numWritingConnections.inc();
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        super.channelUnregistered(ctx);
        numWritingConnections.dec();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, TransferMessage msg) {
        try {
            onMessage(ctx, msg);
        } catch (Throwable t) {
            CommonUtils.runQuietly(() -> onInternalFailure(currentChannelID, ctx, t), true);
        }
    }

    private void onMessage(ChannelHandlerContext ctx, TransferMessage msg) throws Throwable {
        Class<?> msgClazz = msg.getClass();
        if (msgClazz == WriteHandshakeRequest.class) {
            WriteHandshakeRequest handshakeReq = (WriteHandshakeRequest) msg;
            SocketAddress address = ctx.channel().remoteAddress();
            ChannelID channelID = handshakeReq.getChannelID();
            LOG.debug("({}) Received {}.", address, msg);

            currentChannelID = channelID;
            BiConsumer<Integer, Integer> creditListener = getCreditListener(ctx, channelID);
            Consumer<Throwable> failureListener = getFailureListener(ctx, channelID);
            writingService.handshake(
                    channelID,
                    handshakeReq.getJobID(),
                    handshakeReq.getDataSetID(),
                    handshakeReq.getMapID(),
                    handshakeReq.getNumSubs(),
                    handshakeReq.getDataPartitionType(),
                    creditListener,
                    failureListener,
                    address.toString());

        } else if (msgClazz == WriteData.class) {
            WriteData writeData = (WriteData) msg;
            SocketAddress address = ctx.channel().remoteAddress();
            LOG.trace("({}) Received {}.", address, writeData);

            ChannelID channelID = writeData.getChannelID();
            currentChannelID = channelID;
            int subpartitionIndex = writeData.getSubIdx();
            ByteBuf buffer = writeData.getBuffer();
            writingService.write(channelID, subpartitionIndex, buffer);

        } else if (msgClazz == WriteRegionStart.class) {
            WriteRegionStart regionStart = (WriteRegionStart) msg;
            SocketAddress address = ctx.channel().remoteAddress();
            LOG.debug("({}) Received {}.", address, regionStart);

            ChannelID channelID = regionStart.getChannelID();
            currentChannelID = channelID;
            writingService.regionStart(
                    channelID, regionStart.getRegionIdx(), regionStart.isBroadcast());

        } else if (msgClazz == WriteRegionFinish.class) {
            WriteRegionFinish regionFinish = (WriteRegionFinish) msg;
            SocketAddress address = ctx.channel().remoteAddress();
            LOG.debug("({}) Received {}.", address, regionFinish);

            ChannelID channelID = regionFinish.getChannelID();
            currentChannelID = channelID;
            writingService.regionFinish(channelID);

        } else if (msgClazz == WriteFinish.class) {
            WriteFinish writeFinish = (WriteFinish) msg;
            SocketAddress address = ctx.channel().remoteAddress();
            LOG.debug("({}) Received {}.", address, writeFinish);

            ChannelID channelID = writeFinish.getChannelID();
            currentChannelID = channelID;
            writingService.writeFinish(channelID, getFinishCommitListener(ctx, channelID));

        } else if (msgClazz == CloseChannel.class) {
            CloseChannel closeChannel = (CloseChannel) msg;
            SocketAddress address = ctx.channel().remoteAddress();
            LOG.debug("({}) Received {}.", address, closeChannel);

            ChannelID channelID = closeChannel.getChannelID();
            currentChannelID = channelID;
            writingService.closeAbnormallyIfUnderServing(channelID);

            // Ensure both WritingService and ReadingService could receive this message.
            ctx.fireChannelRead(msg);

        } else if (msgClazz == CloseConnection.class) {
            SocketAddress address = ctx.channel().remoteAddress();
            CloseConnection closeConnection = (CloseConnection) msg;
            LOG.info("({}) received {}.", address, closeConnection);
            close(ctx, new ShuffleException("Receive connection close from client."));

            // Ensure both WritingService and ReadingService could receive this message.
            ctx.fireChannelRead(msg);

        } else {
            ctx.fireChannelRead(msg);
        }
    }

    private BiConsumer<Integer, Integer> getCreditListener(
            ChannelHandlerContext ctx, ChannelID channelID) {
        return (credit, regionIdx) ->
                ctx.pipeline()
                        .fireUserEventTriggered(new AddCreditEvent(channelID, credit, regionIdx));
    }

    private Runnable getFinishCommitListener(ChannelHandlerContext ctx, ChannelID channelID) {
        return () -> ctx.pipeline().fireUserEventTriggered(new WriteFinishCommitEvent(channelID));
    }

    private Consumer<Throwable> getFailureListener(ChannelHandlerContext ctx, ChannelID channelID) {
        return t -> {
            if (t instanceof ClosedChannelException) {
                return;
            }
            ctx.pipeline().fireUserEventTriggered(new WritingFailureEvent(channelID, t));
        };
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        Class<?> msgClazz = evt.getClass();
        if (msgClazz == AddCreditEvent.class) {
            AddCreditEvent addCreditEvt = (AddCreditEvent) evt;
            SocketAddress address = ctx.channel().remoteAddress();

            ChannelID channelID = addCreditEvt.channelID;
            WriteAddCredit addCredit =
                    new WriteAddCredit(
                            currentProtocolVersion(),
                            channelID,
                            addCreditEvt.credit,
                            addCreditEvt.regionIdx,
                            emptyExtraMessage());
            LOG.trace("({}) Send {}.", address, addCredit);
            writeAndFlush(ctx, addCredit);

        } else if (msgClazz == WriteFinishCommitEvent.class) {
            WriteFinishCommitEvent finishCommitEvt = (WriteFinishCommitEvent) evt;
            SocketAddress address = ctx.channel().remoteAddress();

            ChannelID channelID = finishCommitEvt.channelID;
            WriteFinishCommit finishCommit =
                    new WriteFinishCommit(currentProtocolVersion(), channelID, emptyExtraMessage());
            LOG.debug("({}) Send {}.", address, finishCommit);
            writeAndFlush(ctx, finishCommit);

        } else if (msgClazz == WritingFailureEvent.class) {
            WritingFailureEvent errRspEvt = (WritingFailureEvent) evt;
            SocketAddress address = ctx.channel().remoteAddress();
            LOG.error("({}) Received {}.", address, errRspEvt);
            ChannelID channelID = errRspEvt.channelID;
            CommonUtils.runQuietly(() -> onInternalFailure(channelID, ctx, errRspEvt.cause), true);

        } else if (evt instanceof IdleStateEvent) {
            IdleStateEvent event = (IdleStateEvent) evt;
            LOG.debug(
                    "({}) Remote seems lost and connection idle -- {}.",
                    ctx.channel().remoteAddress(),
                    event.state());
            if (heartbeatInterval <= 0) {
                return;
            }

            CommonUtils.runQuietly(
                    () -> close(ctx, new ShuffleException("Heartbeat timeout.")), true);
        } else {
            ctx.fireUserEventTriggered(evt);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        close(ctx, new ShuffleException("Channel inactive."));
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        close(ctx, cause);
    }

    public WritingService getWritingService() {
        return writingService;
    }

    private void writeAndFlush(ChannelHandlerContext ctx, Object obj) {
        BiConsumerWithException<ChannelFuture, Throwable, Exception> errorHandler =
                (channelFuture, cause) -> {
                    LOG.error(
                            "Shuffle write Netty failure -- failed to send {}, cause: {}.",
                            obj,
                            cause.getClass().getSimpleName());
                    close(ctx, cause);
                };
        ChannelFutureListenerImpl listener = new ChannelFutureListenerImpl(errorHandler);
        ctx.writeAndFlush(obj).addListener(listener);
    }

    private void onInternalFailure(ChannelID channelID, ChannelHandlerContext ctx, Throwable t) {
        checkNotNull(channelID);
        byte[] errorMessageBytes = ExceptionUtils.summaryErrorMessageStack(t).getBytes();
        ErrorResponse errRsp =
                new ErrorResponse(
                        currentProtocolVersion(),
                        channelID,
                        errorMessageBytes,
                        emptyExtraMessage());
        ctx.writeAndFlush(errRsp).addListener(new CloseChannelWhenFailure());
        writingService.releaseOnError(t, channelID);
    }

    // This method is invoked when:
    // 1. Received CloseConnection from client;
    // 2. Network error --
    //    a. sending message failure;
    //    b. connection inactive;
    //    c. connection exception caught;
    //
    // This method does below things:
    // 1. Triggering errors to corresponding logical channels;
    // 2. Stopping the heartbeat to client;
    // 3. Closing physical connection;
    private void close(ChannelHandlerContext ctx, Throwable throwable) {
        if (writingService.getNumServingChannels() > 0) {
            writingService.releaseOnError(
                    throwable != null ? throwable : new ClosedChannelException(), null);
        }

        if (heartbeatFuture != null) {
            heartbeatFuture.cancel(true);
        }
        ctx.channel().close();
        connectionClosed = true;
    }

    private static class AddCreditEvent {

        ChannelID channelID;

        int credit;

        int regionIdx;

        AddCreditEvent(ChannelID channelID, int credit, int regionIdx) {
            this.channelID = channelID;
            this.credit = credit;
            this.regionIdx = regionIdx;
        }

        @Override
        public String toString() {
            return String.format(
                    "AddCreditEvent [credit: %d, channelID: %s, regionIdx: %d]",
                    credit, channelID, regionIdx);
        }
    }

    private static class WriteFinishCommitEvent {

        ChannelID channelID;

        WriteFinishCommitEvent(ChannelID channelID) {
            this.channelID = channelID;
        }

        @Override
        public String toString() {
            return String.format("WriteFinishCommitEvent [channelID: %s]", channelID);
        }
    }

    static class WritingFailureEvent {

        ChannelID channelID;

        Throwable cause;

        WritingFailureEvent(ChannelID channelID, Throwable cause) {
            this.cause = cause;
            this.channelID = channelID;
        }

        @Override
        public String toString() {
            return String.format(
                    "WritingFailureEvent [channelID: %s, cause: %s]",
                    channelID, cause.getMessage());
        }
    }
}
