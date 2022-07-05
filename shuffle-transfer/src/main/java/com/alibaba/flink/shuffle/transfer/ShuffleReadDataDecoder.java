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

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;

import java.util.function.Function;
import java.util.function.Supplier;

/** {@link TransferMessageDecoder} for {@link TransferMessage.ReadData}. */
public class ShuffleReadDataDecoder extends TransferMessageDecoder {

    private ByteBuf headerByteBuf;

    private boolean headerInitialized;

    private final Function<ChannelID, Supplier<ByteBuf>> bufferSuppliers;

    private ByteBuf body;

    private TransferMessage.ReadData shuffleReadData;

    /** @param bufferSuppliers Supplies buffers to accommodate network buffers. */
    public ShuffleReadDataDecoder(Function<ChannelID, Supplier<ByteBuf>> bufferSuppliers) {
        this.bufferSuppliers = bufferSuppliers;
        this.isClosed = false;
    }

    @Override
    public void onNewMessageReceived(ChannelHandlerContext ctx, int msgId, int messageLength) {
        super.onNewMessageReceived(ctx, msgId, messageLength);
        headerByteBuf = ctx.alloc().directBuffer(messageLength);
    }

    @Override
    public DecodingResult onChannelRead(ByteBuf byteBuf) {
        CommonUtils.checkState(!isClosed, "Decoder has been closed.");

        if (!headerInitialized) {
            boolean accumulationFinished =
                    DecodingUtil.accumulate(
                            headerByteBuf, byteBuf, messageLength, headerByteBuf.readableBytes());
            if (!accumulationFinished) {
                return DecodingResult.NOT_FINISHED;
            }
            shuffleReadData = TransferMessage.ReadData.initByHeader(headerByteBuf);
            headerInitialized = true;
        }
        try {
            if (body == null) {
                body = bufferSuppliers.apply(shuffleReadData.getChannelID()).get();
            }

            if (body.capacity() < shuffleReadData.getBufferSize()) {
                throw new IllegalArgumentException(
                        String.format(
                                "Buffer size of write data (%d) is bigger than that can be accepted (%d)",
                                shuffleReadData.getBufferSize(), body.capacity()));
            }

            boolean accumulationFinished =
                    DecodingUtil.accumulate(
                            body, byteBuf, shuffleReadData.getBufferSize(), body.readableBytes());
            if (accumulationFinished) {
                shuffleReadData.setBuffer(body);
                DecodingResult res = DecodingResult.fullMessage(shuffleReadData);
                headerInitialized = false;
                body = null;
                return res;
            } else {
                return DecodingResult.NOT_FINISHED;
            }
        } catch (Throwable t) {
            throw new ReadingExceptionWithChannelID(shuffleReadData.getChannelID(), t);
        }
    }

    @Override
    public void close() {
        if (isClosed) {
            return;
        }
        if (headerByteBuf != null) {
            headerByteBuf.release();
        }
        if (body != null) {
            body.release();
        }
        isClosed = true;
    }
}
