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

import com.alibaba.flink.shuffle.transfer.TransferMessage.BacklogAnnouncement;
import com.alibaba.flink.shuffle.transfer.TransferMessage.CloseChannel;
import com.alibaba.flink.shuffle.transfer.TransferMessage.CloseConnection;
import com.alibaba.flink.shuffle.transfer.TransferMessage.ErrorResponse;
import com.alibaba.flink.shuffle.transfer.TransferMessage.Heartbeat;
import com.alibaba.flink.shuffle.transfer.TransferMessage.ReadAddCredit;
import com.alibaba.flink.shuffle.transfer.TransferMessage.ReadHandshakeRequest;
import com.alibaba.flink.shuffle.transfer.TransferMessage.WriteAddCredit;
import com.alibaba.flink.shuffle.transfer.TransferMessage.WriteFinish;
import com.alibaba.flink.shuffle.transfer.TransferMessage.WriteFinishCommit;
import com.alibaba.flink.shuffle.transfer.TransferMessage.WriteHandshakeRequest;
import com.alibaba.flink.shuffle.transfer.TransferMessage.WriteRegionFinish;
import com.alibaba.flink.shuffle.transfer.TransferMessage.WriteRegionStart;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A {@link TransferMessageDecoder} to decode messages which doesn't carry shuffle data. */
public class CommonTransferMessageDecoder extends TransferMessageDecoder {

    private static final Logger LOG = LoggerFactory.getLogger(CommonTransferMessageDecoder.class);

    private ByteBuf messageBuffer;

    @Override
    public void onNewMessageReceived(ChannelHandlerContext ctx, int msgId, int messageLength) {
        super.onNewMessageReceived(ctx, msgId, messageLength);
        messageBuffer = ctx.alloc().directBuffer(messageLength);
        messageBuffer.clear();
        ensureBufferCapacity();
    }

    @Override
    public TransferMessageDecoder.DecodingResult onChannelRead(ByteBuf data) throws Exception {
        boolean accumulationFinished =
                DecodingUtil.accumulate(
                        messageBuffer, data, messageLength, messageBuffer.readableBytes());
        if (!accumulationFinished) {
            return TransferMessageDecoder.DecodingResult.NOT_FINISHED;
        }

        switch (msgId) {
            case ErrorResponse.ID:
                return TransferMessageDecoder.DecodingResult.fullMessage(
                        ErrorResponse.readFrom(messageBuffer));
            case WriteHandshakeRequest.ID:
                return TransferMessageDecoder.DecodingResult.fullMessage(
                        WriteHandshakeRequest.readFrom(messageBuffer));
            case WriteAddCredit.ID:
                return TransferMessageDecoder.DecodingResult.fullMessage(
                        WriteAddCredit.readFrom(messageBuffer));
            case WriteRegionStart.ID:
                return TransferMessageDecoder.DecodingResult.fullMessage(
                        WriteRegionStart.readFrom(messageBuffer));
            case WriteRegionFinish.ID:
                return TransferMessageDecoder.DecodingResult.fullMessage(
                        WriteRegionFinish.readFrom(messageBuffer));
            case WriteFinish.ID:
                return TransferMessageDecoder.DecodingResult.fullMessage(
                        WriteFinish.readFrom(messageBuffer));
            case WriteFinishCommit.ID:
                return TransferMessageDecoder.DecodingResult.fullMessage(
                        WriteFinishCommit.readFrom(messageBuffer));
            case ReadHandshakeRequest.ID:
                return TransferMessageDecoder.DecodingResult.fullMessage(
                        ReadHandshakeRequest.readFrom(messageBuffer));
            case ReadAddCredit.ID:
                return TransferMessageDecoder.DecodingResult.fullMessage(
                        ReadAddCredit.readFrom(messageBuffer));
            case CloseChannel.ID:
                return TransferMessageDecoder.DecodingResult.fullMessage(
                        CloseChannel.readFrom(messageBuffer));
            case CloseConnection.ID:
                return TransferMessageDecoder.DecodingResult.fullMessage(
                        CloseConnection.readFrom(messageBuffer));
            case Heartbeat.ID:
                return TransferMessageDecoder.DecodingResult.fullMessage(
                        Heartbeat.readFrom(messageBuffer));
            case BacklogAnnouncement.ID:
                return TransferMessageDecoder.DecodingResult.fullMessage(
                        BacklogAnnouncement.readFrom(messageBuffer));
            default:
                // not throw any exception to keep better compatibility
                LOG.debug("Received unknown message from producer: " + msgId);
                return DecodingResult.UNKNOWN_MESSAGE;
        }
    }

    /**
     * Ensures the message header accumulation buffer has enough capacity for the current message.
     */
    private void ensureBufferCapacity() {
        if (messageBuffer.capacity() < messageLength) {
            messageBuffer.capacity(messageLength);
        }
    }

    @Override
    public void close() {
        if (isClosed) {
            return;
        }
        if (messageBuffer != null) {
            messageBuffer.release();
        }
        isClosed = true;
    }
}
