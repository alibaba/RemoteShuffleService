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
import com.alibaba.flink.shuffle.core.ids.ChannelID;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.flink.shaded.netty4.io.netty.handler.timeout.IdleStateEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;
import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkState;

/**
 * A {@link ChannelInboundHandlerAdapter} shared by multiple {@link ShuffleReadClient} for shuffle
 * read.
 */
public class ReadClientHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(ReadClientHandler.class);

    /** Heartbeat interval. */
    private final int heartbeatInterval;

    /** The sharing {@link ShuffleReadClient}s by {@link ChannelID}s. */
    private final Map<ChannelID, ShuffleReadClient> readClientsByChannelID =
            new ConcurrentHashMap<>();

    /** {@link ScheduledFuture} for heartbeat. */
    private volatile ScheduledFuture<?> heartbeatFuture;

    /** Whether heartbeat future canceled. */
    private boolean heartbeatFutureCanceled;

    /** Channel handler context for user event processing. */
    private volatile ChannelHandlerContext channelHandlerContext;

    /**
     * @param heartbeatInterval Heartbeat interval -- client & server send heartbeat with each other
     *     to confirm existence.
     */
    public ReadClientHandler(int heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
    }

    /** Register a {@link ShuffleReadClient}. */
    public void register(ShuffleReadClient shuffleReadClient) {
        readClientsByChannelID.put(shuffleReadClient.getChannelID(), shuffleReadClient);
    }

    /** Unregister a {@link ShuffleReadClient}. */
    public void unregister(ShuffleReadClient shuffleReadClient) {
        readClientsByChannelID.remove(shuffleReadClient.getChannelID());
    }

    public void notifyReadCredit(TransferMessage.ReadAddCredit addCredit) {
        channelHandlerContext
                .executor()
                .execute(() -> channelHandlerContext.pipeline().fireUserEventTriggered(addCredit));
    }

    /** Get buffer suppliers to decode shuffle read data. */
    public Function<ChannelID, Supplier<ByteBuf>> bufferSuppliers() {
        return channelID ->
                () -> {
                    ShuffleReadClient shuffleReadClient = readClientsByChannelID.get(channelID);
                    if (shuffleReadClient == null) {
                        throw new ShuffleException(
                                "Channel of " + channelID + " is already released.");
                    }
                    return shuffleReadClient.requestBuffer();
                };
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        if (heartbeatInterval > 0) {
            heartbeatFuture =
                    ctx.executor()
                            .scheduleAtFixedRate(
                                    () -> ctx.writeAndFlush(new TransferMessage.Heartbeat()),
                                    0,
                                    heartbeatInterval,
                                    TimeUnit.SECONDS);
        }

        if (channelHandlerContext == null) {
            channelHandlerContext = ctx;
        }

        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        LOG.debug("({}) Connection inactive.", ctx.channel().remoteAddress());
        for (ShuffleReadClient shuffleReadClient : readClientsByChannelID.values()) {
            shuffleReadClient.channelInactive();
        }

        if (heartbeatFuture != null && !heartbeatFutureCanceled) {
            heartbeatFuture.cancel(true);
            heartbeatFutureCanceled = true;
        }
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOG.debug("({}) Connection exception caught.", ctx.channel().remoteAddress(), cause);
        exceptionCaught(cause);
    }

    private void exceptionCaught(Throwable cause) {
        for (ShuffleReadClient shuffleReadClient : readClientsByChannelID.values()) {
            shuffleReadClient.exceptionCaught(cause);
        }

        if (heartbeatFuture != null && !heartbeatFutureCanceled) {
            heartbeatFuture.cancel(true);
            heartbeatFutureCanceled = true;
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {

        ChannelID currentChannelID = null;
        try {
            if (msg.getClass() == TransferMessage.ReadData.class) {
                TransferMessage.ReadData readData = (TransferMessage.ReadData) msg;
                SocketAddress address = ctx.channel().remoteAddress();
                ChannelID channelID = readData.getChannelID();
                currentChannelID = channelID;
                LOG.trace("({}) Received {}.", address, readData);

                ShuffleReadClient shuffleReadClient = readClientsByChannelID.get(channelID);
                if (shuffleReadClient == null) {
                    readData.getBuffer().release();
                    throw new IllegalStateException(
                            "Read channel has been unregistered -- " + channelID);
                } else {
                    shuffleReadClient.dataReceived(readData);
                }
            } else if (msg.getClass() == TransferMessage.ErrorResponse.class) {
                TransferMessage.ErrorResponse errorRsp = (TransferMessage.ErrorResponse) msg;
                SocketAddress address = ctx.channel().remoteAddress();
                ChannelID channelID = errorRsp.getChannelID();
                currentChannelID = channelID;
                LOG.debug("({}) Received {}.", address, errorRsp);
                ShuffleReadClient shuffleReadClient = readClientsByChannelID.get(channelID);
                assertChannelExists(channelID, shuffleReadClient);
                String errorMsg = new String(errorRsp.getErrorMessageBytes());
                shuffleReadClient.exceptionCaught(new IOException(errorMsg));

            } else if (msg.getClass() == TransferMessage.BacklogAnnouncement.class) {
                TransferMessage.BacklogAnnouncement backlog =
                        (TransferMessage.BacklogAnnouncement) msg;
                SocketAddress address = ctx.channel().remoteAddress();
                ChannelID channelID = backlog.getChannelID();
                currentChannelID = channelID;
                LOG.trace("({}) Received {}.", address, backlog);
                ShuffleReadClient shuffleReadClient = readClientsByChannelID.get(channelID);
                assertChannelExists(channelID, shuffleReadClient);
                shuffleReadClient.backlogReceived(backlog.getBacklog());
            } else {
                ctx.fireChannelRead(msg);
            }
        } catch (Throwable t) {
            SocketAddress address = ctx.channel().remoteAddress();
            LOG.debug("({}, ch: {}) Exception caught.", address, currentChannelID, t);
            if (readClientsByChannelID.containsKey(currentChannelID)) {
                readClientsByChannelID.get(currentChannelID).exceptionCaught(t);
            }
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent event = (IdleStateEvent) evt;
            LOG.debug(
                    "({}) Remote seems lost and connection idle -- {}.",
                    ctx.channel().remoteAddress(),
                    event.state());
            if (heartbeatInterval <= 0) {
                return;
            }

            CommonUtils.runQuietly(
                    () ->
                            exceptionCaught(
                                    ctx,
                                    new Exception("Connection idle, state is " + event.state())),
                    true);
        } else if (evt instanceof ClientReadingFailureEvent) {
            ClientReadingFailureEvent event = (ClientReadingFailureEvent) evt;
            LOG.debug("({}) Received {}.", ctx.channel().remoteAddress(), event);
            ShuffleReadClient shuffleReadClient = readClientsByChannelID.get(event.channelID);
            if (shuffleReadClient != null) {
                shuffleReadClient.exceptionCaught(event.cause);
            }
            // Otherwise, the client is already released, thus no need to propagate.
        } else if (evt instanceof TransferMessage.ReadAddCredit) {
            TransferMessage.ReadAddCredit addCredit = (TransferMessage.ReadAddCredit) evt;
            if (LOG.isTraceEnabled()) {
                LOG.trace(
                        "(remote: {}, channel: {}) Send {}.",
                        ctx.channel().remoteAddress(),
                        addCredit.getChannelID(),
                        addCredit);
            }
            ctx.channel()
                    .writeAndFlush(addCredit)
                    .addListener(
                            new ChannelFutureListenerImpl(
                                    (future, throwable) -> exceptionCaught(throwable)));
        } else {
            ctx.fireUserEventTriggered(evt);
        }
    }

    private void assertChannelExists(ChannelID channelID, ShuffleReadClient shuffleReadClient) {
        checkState(
                shuffleReadClient != null,
                () -> "Read channel has been unregistered -- " + channelID);
    }

    static class ClientReadingFailureEvent {

        private final ChannelID channelID;
        private final Throwable cause;

        ClientReadingFailureEvent(ChannelID channelID, Throwable cause) {
            this.channelID = checkNotNull(channelID);
            this.cause = checkNotNull(cause);
        }

        @Override
        public String toString() {
            return String.format(
                    "ClientReadingFailureEvent [channelID: %s, cause: %s]",
                    channelID, cause.getMessage());
        }
    }
}
