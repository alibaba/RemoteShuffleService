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

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;

import javax.annotation.Nullable;

/** Decoding harness under {@link DecoderDelegate}. */
public abstract class TransferMessageDecoder {

    /** ID of the message under decoding. */
    protected int msgId;

    /** Length of the message under decoding. */
    protected int messageLength;

    /** Whether the decoder is closed ever. */
    protected boolean isClosed;

    /**
     * Notifies that a new message is to be decoded.
     *
     * @param ctx Channel context.
     * @param msgId The type of the message to be decoded.
     * @param messageLength The length of the message to be decoded.
     */
    public void onNewMessageReceived(ChannelHandlerContext ctx, int msgId, int messageLength) {
        this.msgId = msgId;
        this.messageLength = messageLength;
    }

    /**
     * Notifies that more data is received to continue decoding.
     *
     * @param data The data received.
     * @return The result of decoding received data.
     */
    public abstract DecodingResult onChannelRead(ByteBuf data) throws Exception;

    /** Close this decoder and release relevant resource. */
    public abstract void close();

    /** The result of decoding one {@link ByteBuf}. */
    public static class DecodingResult {

        public static final DecodingResult NOT_FINISHED = new DecodingResult(false, null);

        public static final DecodingResult UNKNOWN_MESSAGE = new DecodingResult(true, null);

        private final boolean finished;

        @Nullable private final TransferMessage message;

        private DecodingResult(boolean finished, @Nullable TransferMessage message) {
            this.finished = finished;
            this.message = message;
        }

        public static DecodingResult fullMessage(TransferMessage message) {
            return new DecodingResult(true, message);
        }

        public boolean isFinished() {
            return finished;
        }

        @Nullable
        public TransferMessage getMessage() {
            return message;
        }
    }
}
