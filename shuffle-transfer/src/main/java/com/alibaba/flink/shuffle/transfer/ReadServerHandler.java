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
import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.common.utils.ExceptionUtils;
import com.alibaba.flink.shuffle.core.ids.ChannelID;
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.core.storage.PartitionedDataStore;
import com.alibaba.flink.shuffle.transfer.TransferMessage.BacklogAnnouncement;
import com.alibaba.flink.shuffle.transfer.TransferMessage.CloseChannel;
import com.alibaba.flink.shuffle.transfer.TransferMessage.CloseConnection;
import com.alibaba.flink.shuffle.transfer.TransferMessage.ErrorResponse;
import com.alibaba.flink.shuffle.transfer.TransferMessage.Heartbeat;
import com.alibaba.flink.shuffle.transfer.TransferMessage.ReadAddCredit;
import com.alibaba.flink.shuffle.transfer.TransferMessage.ReadHandshakeRequest;

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
import java.util.function.Consumer;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;
import static com.alibaba.flink.shuffle.common.utils.ProtocolUtils.currentProtocolVersion;
import static com.alibaba.flink.shuffle.common.utils.ProtocolUtils.emptyExtraMessage;

/** A {@link ChannelInboundHandler} serves shuffle read process on server side. */
public class ReadServerHandler extends SimpleChannelInboundHandler<TransferMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(ReadServerHandler.class);

    /** Heartbeat interval. */
    private final int heartbeatInterval;

    /** Service logic underground. */
    private final ReadingService readingService;

    /** Identifier of current channel under serving. */
    private ChannelID currentChannelID;

    /** {@link ScheduledFuture} for heartbeat. */
    private ScheduledFuture<?> heartbeatFuture;

    /** If connection closed. */
    private boolean connectionClosed;

    private final Counter numReadingConnections;

    /**
     * @param dataStore Implementation of storage layer.
     * @param heartbeatInterval Heartbeat interval in seconds.
     */
    public ReadServerHandler(PartitionedDataStore dataStore, int heartbeatInterval) {
        this.readingService = new ReadingService(dataStore);
        this.heartbeatInterval = heartbeatInterval;
        this.connectionClosed = false;
        this.numReadingConnections = NetworkMetricsUtil.registerNumReadingConnections();
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
    protected void channelRead0(ChannelHandlerContext ctx, TransferMessage msg) {
        try {
            onMessage(ctx, msg);
        } catch (Throwable e) {
            CommonUtils.runQuietly(() -> onInternalFailure(currentChannelID, ctx, e), true);
        }
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        super.channelRegistered(ctx);
        numReadingConnections.inc();
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        super.channelUnregistered(ctx);
        numReadingConnections.dec();
    }

    private void onMessage(ChannelHandlerContext ctx, TransferMessage msg) throws Throwable {

        Class<?> msgClazz = msg.getClass();
        if (msgClazz == ReadHandshakeRequest.class) {
            ReadHandshakeRequest handshakeReq = (ReadHandshakeRequest) msg;
            ChannelID channelID = handshakeReq.getChannelID();
            SocketAddress address = ctx.channel().remoteAddress();
            LOG.debug("({}}) received {}.", address, handshakeReq);

            currentChannelID = channelID;
            DataSetID dataSetID = handshakeReq.getDataSetID();
            MapPartitionID mapID = handshakeReq.getMapID();
            int startSubIdx = handshakeReq.getStartSubIdx();
            int endSubIdx = handshakeReq.getEndSubIdx();
            int initCredit = handshakeReq.getInitialCredit();
            Consumer<DataViewReader> dataListener = ctx.pipeline()::fireUserEventTriggered;
            Consumer<Integer> backlogListener = getBacklogListener(ctx, channelID);
            Consumer<Throwable> failureListener = getFailureListener(ctx, channelID);
            readingService.handshake(
                    channelID,
                    dataSetID,
                    mapID,
                    startSubIdx,
                    endSubIdx,
                    dataListener,
                    backlogListener,
                    failureListener,
                    initCredit,
                    address.toString());

        } else if (msgClazz == ReadAddCredit.class) {
            ReadAddCredit addCredit = (ReadAddCredit) msg;
            LOG.trace("({}) Received {}.", ctx.channel().remoteAddress(), addCredit);

            ChannelID channelID = addCredit.getChannelID();
            currentChannelID = channelID;
            int credit = addCredit.getCredit();
            if (credit > 0) {
                readingService.addCredit(channelID, credit);
            }

        } else if (msgClazz == CloseChannel.class) {
            CloseChannel closeChannel = (CloseChannel) msg;
            SocketAddress address = ctx.channel().remoteAddress();
            LOG.debug("({}) Received {}.", address, closeChannel);

            ChannelID channelID = closeChannel.getChannelID();
            currentChannelID = channelID;
            readingService.closeAbnormallyIfUnderServing(channelID);

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

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        Class<?> msgClazz = evt.getClass();
        if (msgClazz == ReadingFailureEvent.class) {
            ReadingFailureEvent errRspEvt = (ReadingFailureEvent) evt;
            LOG.error("({}) Received {}.", ctx.channel().remoteAddress(), errRspEvt);
            CommonUtils.runQuietly(
                    () -> onInternalFailure(errRspEvt.channelID, ctx, errRspEvt.cause), true);

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
        } else if (evt instanceof BacklogAnnouncement) {
            BacklogAnnouncement backlog = (BacklogAnnouncement) evt;
            ctx.writeAndFlush(backlog).addListener(new CloseChannelWhenFailure());
            LOG.trace("({}) Announce backlog {}.", ctx.channel().remoteAddress(), backlog);
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

    public ReadingService getReadingService() {
        return readingService;
    }

    private Consumer<Throwable> getFailureListener(ChannelHandlerContext ctx, ChannelID channelID) {
        return t -> {
            if (t instanceof ClosedChannelException) {
                return;
            }
            ctx.pipeline().fireUserEventTriggered(new ReadingFailureEvent(t, channelID));
        };
    }

    private Consumer<Integer> getBacklogListener(ChannelHandlerContext ctx, ChannelID channelID) {
        return (backlog) ->
                ctx.pipeline()
                        .fireUserEventTriggered(
                                new TransferMessage.BacklogAnnouncement(channelID, backlog));
    }

    private void onInternalFailure(ChannelID channelID, ChannelHandlerContext ctx, Throwable t) {
        checkNotNull(channelID);
        LOG.error("(ch: {}) Internal shuffle read failure.", channelID, t);
        byte[] errorMessageBytes = ExceptionUtils.summaryErrorMessageStack(t).getBytes();
        ErrorResponse errRsp =
                new ErrorResponse(
                        currentProtocolVersion(),
                        channelID,
                        errorMessageBytes,
                        emptyExtraMessage());
        ctx.writeAndFlush(errRsp).addListener(new CloseChannelWhenFailure());
        readingService.releaseOnError(t, channelID);
    }

    // This method is invoked when:
    // 1. Received CloseConnection from client;
    // 2. Network error --
    //    a. sending failure message;
    //    b. connection inactive;
    //    c. connection exception caught;
    //
    // This method does below things:
    // 1. Triggering errors to corresponding logical channels;
    // 2. Stopping the heartbeat to client;
    // 3. Closing physical connection;
    private void close(ChannelHandlerContext ctx, Throwable throwable) {
        if (readingService.getNumServingChannels() > 0) {
            readingService.releaseOnError(
                    throwable != null ? throwable : new ClosedChannelException(), null);
        }

        if (heartbeatFuture != null) {
            heartbeatFuture.cancel(true);
        }
        ctx.channel().close();
        connectionClosed = true;
    }

    static class ReadingFailureEvent {

        Throwable cause;

        ChannelID channelID;

        ReadingFailureEvent(Throwable cause, ChannelID channelID) {
            this.cause = cause;
            this.channelID = channelID;
        }

        @Override
        public String toString() {
            return String.format(
                    "ReadingFailureEvent [channelID: %s, cause: %s]",
                    channelID, cause.getMessage());
        }
    }
}
