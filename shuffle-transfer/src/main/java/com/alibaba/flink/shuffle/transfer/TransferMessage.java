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
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelOutboundInvoker;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelPromise;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkArgument;
import static com.alibaba.flink.shuffle.common.utils.ProtocolUtils.currentProtocolVersion;
import static com.alibaba.flink.shuffle.common.utils.ProtocolUtils.emptyExtraMessageBytes;
import static com.alibaba.flink.shuffle.common.utils.StringUtils.stringToBytes;

/**
 * Communication protocol between shuffle worker and client. Extension defines specific fields and
 * how they are serialized and deserialized. A message is wrapped in a 'frame' with a header defines
 * frame length, magic number and message type.
 */
public abstract class TransferMessage {

    public int getContentLength() {
        return 0;
    }

    /** Method to define message serialization. */
    public abstract void write(
            ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator);

    /** Feedback when error on server. */
    public static class ErrorResponse extends TransferMessage {

        public static final byte ID = -1;

        private final int version;

        private final ChannelID channelID;

        private final byte[] errorMessageBytes;

        private final byte[] extraInfo;

        public ErrorResponse(
                int version, ChannelID channelID, byte[] errorMessageBytes, String extraInfo) {
            this.version = version;
            this.channelID = channelID;
            this.errorMessageBytes = errorMessageBytes;
            this.extraInfo = stringToBytes(extraInfo);
        }

        @Override
        public void write(
                ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator) {
        }
    }

    /** Handshake message send by client when start shuffle write. */
    public static class WriteHandshakeRequest extends TransferMessage {

        public static final byte ID = 0;

        private final int version;

        private final ChannelID channelID;

        private final JobID jobID;

        private final DataSetID dataSetID;

        private final MapPartitionID mapID;

        private final int numSubs;

        private final int bufferSize;

        // Specify the factory name of data partition type
        private final byte[] dataPartitionType;

        private final byte[] extraInfo;

        public WriteHandshakeRequest(
                int version,
                ChannelID channelID,
                JobID jobID,
                DataSetID dataSetID,
                MapPartitionID mapPartitionID,
                int numSubs,
                int bufferSize,
                String dataPartitionType,
                String extraInfo) {

            this.version = version;
            this.channelID = channelID;
            this.jobID = jobID;
            this.dataSetID = dataSetID;
            this.mapID = mapPartitionID;
            this.numSubs = numSubs;
            this.bufferSize = bufferSize;
            this.dataPartitionType = stringToBytes(dataPartitionType);
            this.extraInfo = stringToBytes(extraInfo);
        }

        @Override
        public void write(
                ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator) {
        }
    }

    /** Message send by server to announce more credits thus to accept more shuffle write data. */
    public static class WriteAddCredit extends TransferMessage {

        public static final byte ID = 1;

        private final int version;

        private final ChannelID channelID;

        private final int credit;

        private final int regionIdx;

        private final byte[] extraInfo;

        public WriteAddCredit(
                int version, ChannelID channelID, int credit, int regionIdx, String extraInfo) {
            this.version = version;
            this.channelID = channelID;
            this.credit = credit;
            this.regionIdx = regionIdx;
            this.extraInfo = stringToBytes(extraInfo);
        }

        @Override
        public void write(
                ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator) {
        }
    }

    /** Shuffle write data send by client. */
    public static class WriteData extends TransferMessage {

        public static final byte ID = 2;

        private final int version;

        private final ChannelID channelID;

        private final int subIdx;

        private final int bufferSize;

        private final boolean isRegionFinish;

        private ByteBuf body;

        private final byte[] extraInfo;

        public WriteData(
                int version,
                ChannelID channelID,
                ByteBuf body,
                int subIdx,
                int bufferSize,
                boolean isRegionFinish,
                String extraInfo) {

            this.version = version;
            this.channelID = channelID;
            this.body = body;
            this.subIdx = subIdx;
            this.bufferSize = bufferSize;
            this.isRegionFinish = isRegionFinish;
            this.extraInfo = stringToBytes(extraInfo);
        }

        @Override
        public void write(
                ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator) {
        }
    }

    /** Message to indicate the start of a region, which guarantees records inside are complete. */
    public static class WriteRegionStart extends TransferMessage {

        public static final byte ID = 3;

        private final int version;

        private final ChannelID channelID;

        private final int regionIdx;

        private final boolean isBroadcast;

        private final byte[] extraInfo;

        public WriteRegionStart(
                int version,
                ChannelID channelID,
                int regionIdx,
                boolean isBroadcast,
                String extraInfo) {
            this.version = version;
            this.channelID = channelID;
            this.regionIdx = regionIdx;
            this.isBroadcast = isBroadcast;
            this.extraInfo = stringToBytes(extraInfo);
        }

        @Override
        public void write(
                ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator) {
        }
    }

    /** Message to indicate the finish of a region, which guarantees records inside are complete. */
    public static class WriteRegionFinish extends TransferMessage {

        public static final byte ID = 4;

        private final int version;

        private final ChannelID channelID;

        private final byte[] extraInfo;

        public WriteRegionFinish(int version, ChannelID channelID, String extraInfo) {
            this.version = version;
            this.channelID = channelID;
            this.extraInfo = stringToBytes(extraInfo);
        }

        @Override
        public void write(
                ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator) {
        }
    }

    /** Message indicates finish of a shuffle write. */
    public static class WriteFinish extends TransferMessage {

        public static final byte ID = 5;

        private final int version;

        private final ChannelID channelID;

        private final byte[] extraInfo;

        public WriteFinish(int version, ChannelID channelID, String extraInfo) {
            this.version = version;
            this.channelID = channelID;
            this.extraInfo = stringToBytes(extraInfo);
        }

        @Override
        public void write(
                ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator) {
        }
    }

    /**
     * Message send from server to confirm {@link WriteFinish}. A shuffle write process could be
     * regarded as successful only when this message is received.
     */
    public static class WriteFinishCommit extends TransferMessage {

        public static final byte ID = 6;

        private final int version;

        private final ChannelID channelID;

        private final byte[] extraInfo;

        public WriteFinishCommit(int version, ChannelID channelID, String extraInfo) {
            this.version = version;
            this.channelID = channelID;
            this.extraInfo = stringToBytes(extraInfo);
        }

        @Override
        public void write(
                ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator) {
        }
    }

    /** Handshake message send from client when start shuffle read. */
    public static class ReadHandshakeRequest extends TransferMessage {

        public static final byte ID = 7;

        private final int version;

        private final ChannelID channelID;

        private final DataSetID dataSetID;

        private final MapPartitionID mapID;

        private final int startSubIdx;

        private final int endSubIdx;

        private final int initialCredit;

        private final int bufferSize;

        private final long offset;

        private final byte[] extraInfo;

        public ReadHandshakeRequest(
                int version,
                ChannelID channelID,
                DataSetID dataSetID,
                MapPartitionID mapID,
                int startSubIdx,
                int endSubIdx,
                int initialCredit,
                int bufferSize,
                long offset,
                String extraInfo) {

            this.version = version;
            this.channelID = channelID;
            this.dataSetID = dataSetID;
            this.mapID = mapID;
            this.startSubIdx = startSubIdx;
            this.endSubIdx = endSubIdx;
            this.initialCredit = initialCredit;
            this.bufferSize = bufferSize;
            this.offset = offset;
            this.extraInfo = stringToBytes(extraInfo);
        }

        @Override
        public void write(
                ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator) {
        }
    }

    /** Message send from client to announce more credits to accept more data. */
    public static class ReadAddCredit extends TransferMessage {

        public static final byte ID = 8;

        private final int version;

        private final ChannelID channelID;

        private final int credit;

        private final byte[] extraInfo;

        public ReadAddCredit(int version, ChannelID channelID, int credit, String extraInfo) {
            this.version = version;
            this.channelID = channelID;
            this.credit = credit;
            this.extraInfo = stringToBytes(extraInfo);
        }

        @Override
        public void write(
                ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator) {
        }
    }

    /** Shuffle read data send from server. */
    public static class ReadData extends TransferMessage {

        public static final byte ID = 9;

        private final int version;

        private final ChannelID channelID;

        private final int backlog;

        private final int bufferSize;

        private final long offset;

        private ByteBuf body;

        private final byte[] extraInfo;

        public ReadData(
                int version,
                ChannelID channelID,
                int backlog,
                int bufferSize,
                long offset,
                ByteBuf body,
                String extraInfo) {
            this.version = version;
            this.channelID = channelID;
            this.backlog = backlog;
            this.bufferSize = bufferSize;
            this.offset = offset;
            this.body = body;
            this.extraInfo = stringToBytes(extraInfo);
        }

        @Override
        public void write(
                ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator) {
        }
    }

    /** Message send from client to ask server to close logical channel. */
    public static class CloseChannel extends TransferMessage {

        public static final byte ID = 10;

        private final int version;

        private final ChannelID channelID;

        private final byte[] extraInfo;

        public CloseChannel(int version, ChannelID channelID, String extraInfo) {
            this.version = version;
            this.channelID = channelID;
            this.extraInfo = stringToBytes(extraInfo);
        }

        @Override
        public void write(
                ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator) {
        }
    }

    /** Message send from client to ask server to close physical connection. */
    public static class CloseConnection extends TransferMessage {

        public static final byte ID = 11;

        private final int version;

        private final byte[] extraInfo;

        public CloseConnection() {
            this.version = currentProtocolVersion();
            this.extraInfo = emptyExtraMessageBytes();
        }

        @Override
        public void write(
                ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator) {
        }
    }

    /** Heartbeat message send from server and client, thus to keep alive between each other. */
    public static class Heartbeat extends TransferMessage {

        public static final byte ID = 12;

        private final int version;

        private final byte[] extraInfo;

        public Heartbeat() {
            this.version = currentProtocolVersion();
            this.extraInfo = emptyExtraMessageBytes();
        }

        @Override
        public void write(
                ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator) {
        }
    }

    /** Backlog announcement for the data sender to the data receiver. */
    public static class BacklogAnnouncement extends TransferMessage {

        public static final byte ID = 13;

        private final ChannelID channelID;

        private final int backlog;

        private final int version;

        private final byte[] extraInfo;

        public BacklogAnnouncement(ChannelID channelID, int backlog) {
            checkArgument(channelID != null, "Must be not null.");
            checkArgument(backlog > 0, "Must be positive.");

            this.channelID = channelID;
            this.backlog = backlog;
            this.version = currentProtocolVersion();
            this.extraInfo = emptyExtraMessageBytes();
        }

        @Override
        public void write(
                ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator) {
        }
    }
}
