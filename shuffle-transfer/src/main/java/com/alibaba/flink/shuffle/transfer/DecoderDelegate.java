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
import com.alibaba.flink.shuffle.transfer.ReadClientHandler.ClientReadingFailureEvent;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.function.Function;
import java.util.function.Supplier;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;
import static com.alibaba.flink.shuffle.transfer.TransferMessage.CloseChannel;
import static com.alibaba.flink.shuffle.transfer.TransferMessage.CloseConnection;
import static com.alibaba.flink.shuffle.transfer.TransferMessage.FRAME_HEADER_LENGTH;
import static com.alibaba.flink.shuffle.transfer.TransferMessage.Heartbeat;
import static com.alibaba.flink.shuffle.transfer.TransferMessage.MAGIC_NUMBER;
import static com.alibaba.flink.shuffle.transfer.TransferMessage.ReadAddCredit;
import static com.alibaba.flink.shuffle.transfer.TransferMessage.ReadData;
import static com.alibaba.flink.shuffle.transfer.TransferMessage.ReadHandshakeRequest;
import static com.alibaba.flink.shuffle.transfer.TransferMessage.WriteData;
import static com.alibaba.flink.shuffle.transfer.TransferMessage.WriteFinish;
import static com.alibaba.flink.shuffle.transfer.TransferMessage.WriteHandshakeRequest;
import static com.alibaba.flink.shuffle.transfer.TransferMessage.WriteRegionFinish;
import static com.alibaba.flink.shuffle.transfer.TransferMessage.WriteRegionStart;

/**
 * A {@link ChannelInboundHandlerAdapter} wrapping {@link TransferMessageDecoder}s to decode
 * different kinds of {@link TransferMessage}s.
 */
public class DecoderDelegate extends ChannelInboundHandlerAdapter {

    private TransferMessageDecoder currentDecoder;

    private ByteBuf frameHeaderBuffer;

    private final Function<Byte, TransferMessageDecoder> messageDecoders;

    public DecoderDelegate(Function<Byte, TransferMessageDecoder> messageDecoders) {
        this.messageDecoders = messageDecoders;
    }

    public static DecoderDelegate writeClientDecoderDelegate() {
        Function<Byte, TransferMessageDecoder> decoderMap =
                msgID -> new CommonTransferMessageDecoder();
        return new DecoderDelegate(decoderMap);
    }

    public static DecoderDelegate readClientDecoderDelegate(
            Function<ChannelID, Supplier<ByteBuf>> bufferSuppliers) {
        Function<Byte, TransferMessageDecoder> decoderMap =
                msgID ->
                        msgID == ReadData.ID
                                ? new ShuffleReadDataDecoder(bufferSuppliers)
                                : new CommonTransferMessageDecoder();
        return new DecoderDelegate(decoderMap);
    }

    public static DecoderDelegate serverDecoderDelegate(
            Function<ChannelID, Supplier<ByteBuf>> bufferSuppliers) {
        Function<Byte, TransferMessageDecoder> decoderMap =
                msgID -> {
                    switch (msgID) {
                        case WriteHandshakeRequest.ID:
                        case WriteRegionStart.ID:
                        case WriteRegionFinish.ID:
                        case WriteFinish.ID:
                        case ReadHandshakeRequest.ID:
                        case ReadAddCredit.ID:
                        case CloseChannel.ID:
                        case CloseConnection.ID:
                        case Heartbeat.ID:
                            return new CommonTransferMessageDecoder();
                        case WriteData.ID:
                            return new ShuffleWriteDataDecoder(bufferSuppliers);
                        default:
                            throw new RuntimeException("No decoder found for message ID: " + msgID);
                    }
                };
        return new DecoderDelegate(decoderMap);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        frameHeaderBuffer = ctx.alloc().directBuffer(FRAME_HEADER_LENGTH);
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (currentDecoder != null) {
            currentDecoder.close();
        }
        frameHeaderBuffer.release();
        super.channelInactive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (!(msg instanceof ByteBuf)) {
            ctx.fireChannelRead(msg);
            return;
        }
        ByteBuf byteBuf = (ByteBuf) msg;
        try {
            while (byteBuf.isReadable()) {
                if (currentDecoder != null) {
                    TransferMessageDecoder.DecodingResult result =
                            currentDecoder.onChannelRead(byteBuf);
                    if (!result.isFinished()) {
                        break;
                    }

                    ctx.fireChannelRead(result.getMessage());

                    currentDecoder.close();
                    currentDecoder = null;
                    frameHeaderBuffer.clear();
                }
                decodeHeader(ctx, byteBuf);
            }
        } catch (Throwable e) {
            if (currentDecoder != null) {
                currentDecoder.close();
            }
            if (e instanceof WritingExceptionWithChannelID) {
                WritingExceptionWithChannelID ec = (WritingExceptionWithChannelID) e;
                WriteServerHandler.WritingFailureEvent evt =
                        new WriteServerHandler.WritingFailureEvent(
                                ec.getChannelID(), ec.getCause());
                ctx.pipeline().fireUserEventTriggered(evt);

            } else if (e instanceof ReadingExceptionWithChannelID) {
                ReadingExceptionWithChannelID ec = (ReadingExceptionWithChannelID) e;
                ClientReadingFailureEvent evt =
                        new ClientReadingFailureEvent(ec.getChannelID(), ec.getCause());
                ctx.pipeline().fireUserEventTriggered(evt);

            } else {
                ctx.fireExceptionCaught(e);
            }
        } finally {
            byteBuf.release();
        }
    }

    // For testing.
    void setCurrentDecoder(TransferMessageDecoder decoder) {
        currentDecoder = decoder;
    }

    private void decodeHeader(ChannelHandlerContext ctx, ByteBuf data) {
        boolean accumulated =
                DecodingUtil.accumulate(
                        frameHeaderBuffer,
                        data,
                        FRAME_HEADER_LENGTH,
                        frameHeaderBuffer.readableBytes());
        if (!accumulated) {
            return;
        }
        int frameLength = frameHeaderBuffer.readInt();
        int magicNumber = frameHeaderBuffer.readInt();
        if (magicNumber != MAGIC_NUMBER) {
            throw new RuntimeException("BUG: magic number unexpected.");
        }
        byte msgID = frameHeaderBuffer.readByte();
        currentDecoder = checkNotNull(messageDecoders.apply(msgID));
        currentDecoder.onNewMessageReceived(ctx, msgID, frameLength - FRAME_HEADER_LENGTH);
    }
}
