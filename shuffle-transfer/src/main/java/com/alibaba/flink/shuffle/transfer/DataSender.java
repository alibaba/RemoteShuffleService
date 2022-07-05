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
import com.alibaba.flink.shuffle.core.memory.Buffer;
import com.alibaba.flink.shuffle.core.storage.BufferWithBacklog;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;

import com.alibaba.metrics.Meter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;

import static com.alibaba.flink.shuffle.common.utils.ProtocolUtils.currentProtocolVersion;
import static com.alibaba.flink.shuffle.common.utils.ProtocolUtils.emptyExtraMessage;
import static com.alibaba.flink.shuffle.common.utils.ProtocolUtils.emptyOffset;

/** A {@link ChannelInboundHandlerAdapter} sending shuffle read data. */
public class DataSender extends ChannelInboundHandlerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(DataSender.class);

    private final ReadingService readingService;

    private final ChannelFutureListenerImpl channelFutureListener;

    private final Meter readingThroughputBytes;

    public DataSender(ReadingService readingService) {
        this.readingService = readingService;
        this.channelFutureListener =
                new ChannelFutureListenerImpl(
                        (channelFuture, cause) -> {
                            if (readingService.getNumServingChannels() > 0) {
                                readingService.releaseOnError(cause, null);
                            }
                            ChannelFutureListener.CLOSE.operationComplete(channelFuture);
                        });
        this.readingThroughputBytes = NetworkMetricsUtil.registerReadingThroughputBytes();
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object msg) {
        if (msg.getClass() == DataViewReader.class) {
            DataViewReader viewReader = (DataViewReader) msg;
            SocketAddress addr = ctx.channel().remoteAddress();
            ChannelID channelID = viewReader.getChannelID();
            try {
                LOG.debug("({}) Received {}.", addr, viewReader);

                while (true) {
                    BufferWithBacklog bufferWithBacklog = viewReader.getNextBuffer();
                    if (bufferWithBacklog == null) {
                        break;
                    }

                    Buffer buffer = bufferWithBacklog.getBuffer();
                    int backlog = (int) bufferWithBacklog.getBacklog();
                    readingThroughputBytes.mark(buffer.readableBytes());
                    TransferMessage.ReadData readData =
                            new TransferMessage.ReadData(
                                    currentProtocolVersion(),
                                    channelID,
                                    backlog,
                                    buffer.readableBytes(),
                                    emptyOffset(),
                                    buffer,
                                    emptyExtraMessage());
                    writeAndFlush(ctx, readData);
                }

                if (viewReader.isEOF()) {
                    LOG.info("{} finished.", viewReader);
                    readingService.readFinish(viewReader.getChannelID());
                }
            } catch (Throwable t) {
                ctx.pipeline()
                        .fireUserEventTriggered(
                                new ReadServerHandler.ReadingFailureEvent(t, channelID));
            }

        } else {
            ctx.fireUserEventTriggered(msg);
        }
    }

    private void writeAndFlush(ChannelHandlerContext ctx, Object obj) {
        ctx.writeAndFlush(obj).addListener(channelFutureListener);
    }
}
