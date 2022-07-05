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

import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.core.ids.ChannelID;
import com.alibaba.flink.shuffle.transfer.TransferMessage.ErrorResponse;
import com.alibaba.flink.shuffle.transfer.TransferMessage.Heartbeat;
import com.alibaba.flink.shuffle.transfer.TransferMessage.WriteAddCredit;
import com.alibaba.flink.shuffle.transfer.TransferMessage.WriteFinishCommit;

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

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkState;

/**
 * A {@link ChannelInboundHandlerAdapter} shared by multiple {@link ShuffleWriteClient} for shuffle
 * write.
 */
public class WriteClientHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(WriteClientHandler.class);

    /** Heartbeat interval. */
    private final int heartbeatInterval;

    /** The sharing {@link ShuffleWriteClient}s by {@link ChannelID}s. */
    private final Map<ChannelID, ShuffleWriteClient> writeClientsByChannelID;

    /** {@link ScheduledFuture} for heartbeat. */
    private ScheduledFuture<?> heartbeatFuture;

    /** Whether heartbeat future canceled. */
    private boolean heartbeatFutureCanceled;

    /**
     * @param heartbeatInterval Heartbeat interval -- client & server send heartbeat with each other
     *     to confirm existence.
     */
    public WriteClientHandler(int heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
        this.writeClientsByChannelID = new ConcurrentHashMap<>();
        this.heartbeatFutureCanceled = false;
    }

    /** Register a {@link ShuffleWriteClient}. */
    public void register(ShuffleWriteClient shuffleWriteClient) {
        writeClientsByChannelID.put(shuffleWriteClient.getChannelID(), shuffleWriteClient);
    }

    /** Unregister a {@link ShuffleWriteClient}. */
    public void unregister(ShuffleWriteClient shuffleWriteClient) {
        writeClientsByChannelID.remove(shuffleWriteClient.getChannelID());
    }

    /** Whether a {@link ChannelID} is registered ever. */
    public boolean isRegistered(ChannelID channelID) {
        return writeClientsByChannelID.containsKey(channelID);
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
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        LOG.debug("({}) Connection inactive.", ctx.channel().remoteAddress());
        writeClientsByChannelID.values().forEach(ShuffleWriteClient::channelInactive);

        if (heartbeatFuture != null && !heartbeatFutureCanceled) {
            heartbeatFuture.cancel(true);
            heartbeatFutureCanceled = true;
        }
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOG.debug("({}) Connection exception caught.", ctx.channel().remoteAddress(), cause);
        writeClientsByChannelID.values().forEach(writeClient -> writeClient.exceptionCaught(cause));

        if (heartbeatFuture != null && !heartbeatFutureCanceled) {
            heartbeatFuture.cancel(true);
            heartbeatFutureCanceled = true;
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        ChannelID currentChannelID = null;

        try {
            if (msg.getClass() == WriteAddCredit.class) {
                WriteAddCredit addCredit = (WriteAddCredit) msg;
                ChannelID channelID = addCredit.getChannelID();
                currentChannelID = channelID;
                assertChannelExists(channelID);
                writeClientsByChannelID.get(channelID).creditReceived(addCredit);

            } else if (msg.getClass() == WriteFinishCommit.class) {
                WriteFinishCommit finishCommit = (WriteFinishCommit) msg;
                ChannelID channelID = finishCommit.getChannelID();
                currentChannelID = channelID;
                assertChannelExists(channelID);
                writeClientsByChannelID.get(channelID).writeFinishCommitReceived(finishCommit);

            } else if (msg.getClass() == ErrorResponse.class) {
                ErrorResponse errorRsp = (ErrorResponse) msg;
                SocketAddress addr = ctx.channel().remoteAddress();
                LOG.debug("({}) Received {}.", addr, errorRsp);
                ChannelID channelID = errorRsp.getChannelID();
                currentChannelID = channelID;
                assertChannelExists(channelID);
                String errorMsg = new String(errorRsp.getErrorMessageBytes());
                writeClientsByChannelID.get(channelID).exceptionCaught(new IOException(errorMsg));

            } else {
                ctx.fireChannelRead(msg);
            }
        } catch (Throwable t) {
            SocketAddress address = ctx.channel().remoteAddress();
            LOG.debug("({}, ch: {}) Exception caught.", address, currentChannelID, t);
            if (writeClientsByChannelID.containsKey(currentChannelID)) {
                writeClientsByChannelID.get(currentChannelID).exceptionCaught(t);
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
        } else {
            ctx.fireUserEventTriggered(evt);
        }
    }

    private void assertChannelExists(ChannelID channelID) {
        checkState(
                writeClientsByChannelID.containsKey(channelID),
                "Unexpected (might already unregistered) channelID -- " + channelID);
    }
}
