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

import java.util.Arrays;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkArgument;
import static com.alibaba.flink.shuffle.common.utils.ProtocolUtils.currentProtocolVersion;
import static com.alibaba.flink.shuffle.common.utils.ProtocolUtils.emptyExtraMessageBytes;
import static com.alibaba.flink.shuffle.common.utils.StringUtils.bytesToString;
import static com.alibaba.flink.shuffle.common.utils.StringUtils.stringToBytes;

/**
 * Communication protocol between shuffle worker and client. Extension defines specific fields and
 * how they are serialized and deserialized. A message is wrapped in a 'frame' with a header defines
 * frame length, magic number and message type.
 */
public abstract class TransferMessage {

    /** Length of frame header -- frame length (4) + magic number (4) + msg ID (1). */
    public static final int FRAME_HEADER_LENGTH = 4 + 4 + 1;

    public static final int MAGIC_NUMBER = 0xBADC0FEF;

    private static ByteBuf allocateBuffer(
            ByteBufAllocator allocator, byte messageID, int contentLength) {

        checkArgument(contentLength <= Integer.MAX_VALUE - FRAME_HEADER_LENGTH);

        ByteBuf buffer = allocator.directBuffer(FRAME_HEADER_LENGTH + contentLength);
        buffer.writeInt(FRAME_HEADER_LENGTH + contentLength);
        buffer.writeInt(MAGIC_NUMBER);
        buffer.writeByte(messageID);

        return buffer;
    }

    /** Content length of the frame other than header. */
    public abstract int getContentLength();

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

        public static ErrorResponse readFrom(ByteBuf byteBuf) {
            int version = byteBuf.readInt();
            ChannelID channelID = ChannelID.readFrom(byteBuf);
            int errorMessageLength = byteBuf.readInt();
            byte[] errorMessageBytes = new byte[errorMessageLength];
            byteBuf.readBytes(errorMessageBytes);
            int extraInfoLen = byteBuf.readInt();
            byte[] extraInfoBytes = new byte[extraInfoLen];
            byteBuf.readBytes(extraInfoBytes);
            String extraInfo = bytesToString(extraInfoBytes);
            return new ErrorResponse(version, channelID, errorMessageBytes, extraInfo);
        }

        @Override
        public int getContentLength() {
            return 4
                    + channelID.getFootprint()
                    + 4
                    + errorMessageBytes.length
                    + 4
                    + extraInfo.length;
        }

        @Override
        public void write(
                ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator) {

            ByteBuf byteBuf = allocateBuffer(allocator, ID, getContentLength());
            byteBuf.writeInt(version);
            channelID.writeTo(byteBuf);
            byteBuf.writeInt(errorMessageBytes.length);
            byteBuf.writeBytes(errorMessageBytes);
            byteBuf.writeInt(extraInfo.length);
            byteBuf.writeBytes(extraInfo);
            out.write(byteBuf, promise);
        }

        public int getVersion() {
            return version;
        }

        public ChannelID getChannelID() {
            return channelID;
        }

        public byte[] getErrorMessageBytes() {
            return errorMessageBytes;
        }

        public String getExtraInfo() {
            return bytesToString(extraInfo);
        }

        @Override
        public String toString() {
            return String.format(
                    "ErrorResponse{%d, %s, msg=%s, extraInfo=%s}",
                    version, channelID, new String(errorMessageBytes), bytesToString(extraInfo));
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

        public static WriteHandshakeRequest readFrom(ByteBuf byteBuf) {
            int version = byteBuf.readInt();
            ChannelID channelID = ChannelID.readFrom(byteBuf);
            JobID jobID = JobID.readFrom(byteBuf);
            DataSetID dataSetID = DataSetID.readFrom(byteBuf);
            MapPartitionID mapID = MapPartitionID.readFrom(byteBuf);
            int numSubs = byteBuf.readInt();
            int bufferSize = byteBuf.readInt();
            int partitionTypeLen = byteBuf.readInt();
            byte[] dataPartitionTypeBytes = new byte[partitionTypeLen];
            byteBuf.readBytes(dataPartitionTypeBytes);
            String dataPartitionType = bytesToString(dataPartitionTypeBytes);
            int extraInfoLen = byteBuf.readInt();
            byte[] extraInfoBytes = new byte[extraInfoLen];
            byteBuf.readBytes(extraInfoBytes);
            String extraInfo = bytesToString(extraInfoBytes);
            return new WriteHandshakeRequest(
                    version,
                    channelID,
                    jobID,
                    dataSetID,
                    mapID,
                    numSubs,
                    bufferSize,
                    dataPartitionType,
                    extraInfo);
        }

        @Override
        public int getContentLength() {
            return 4
                    + channelID.getFootprint()
                    + jobID.getFootprint()
                    + dataSetID.getFootprint()
                    + mapID.getFootprint()
                    + 4
                    + 4
                    + 4
                    + dataPartitionType.length
                    + 4
                    + extraInfo.length;
        }

        @Override
        public void write(
                ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator) {

            ByteBuf byteBuf = allocateBuffer(allocator, ID, getContentLength());
            byteBuf.writeInt(version);
            channelID.writeTo(byteBuf);
            jobID.writeTo(byteBuf);
            dataSetID.writeTo(byteBuf);
            mapID.writeTo(byteBuf);
            byteBuf.writeInt(numSubs);
            byteBuf.writeInt(bufferSize);
            byteBuf.writeInt(dataPartitionType.length);
            byteBuf.writeBytes(dataPartitionType);
            byteBuf.writeInt(extraInfo.length);
            byteBuf.writeBytes(extraInfo);
            out.write(byteBuf, promise);
        }

        public int getVersion() {
            return version;
        }

        public ChannelID getChannelID() {
            return channelID;
        }

        public JobID getJobID() {
            return jobID;
        }

        public DataSetID getDataSetID() {
            return dataSetID;
        }

        public MapPartitionID getMapID() {
            return mapID;
        }

        public int getNumSubs() {
            return numSubs;
        }

        public int getBufferSize() {
            return bufferSize;
        }

        public String getDataPartitionType() {
            return bytesToString(dataPartitionType);
        }

        public String getExtraInfo() {
            return bytesToString(extraInfo);
        }

        @Override
        public String toString() {
            return String.format(
                    "WriteHandshakeRequest{%d, %s, %s, %s, %s, numSubs=%d, "
                            + "bufferSize=%d, dataPartitionType=%s, extraInfo=%s}",
                    version,
                    channelID,
                    jobID,
                    dataSetID,
                    mapID,
                    numSubs,
                    bufferSize,
                    bytesToString(dataPartitionType),
                    bytesToString(extraInfo));
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

        public static WriteAddCredit readFrom(ByteBuf byteBuf) {
            int version = byteBuf.readInt();
            ChannelID channelID = ChannelID.readFrom(byteBuf);
            int credit = byteBuf.readInt();
            int regionIdx = byteBuf.readInt();
            int extraInfoLen = byteBuf.readInt();
            byte[] extraInfoBytes = new byte[extraInfoLen];
            byteBuf.readBytes(extraInfoBytes);
            String extraInfo = bytesToString(extraInfoBytes);
            return new WriteAddCredit(version, channelID, credit, regionIdx, extraInfo);
        }

        @Override
        public int getContentLength() {
            return 4 + channelID.getFootprint() + 4 + 4 + 4 + extraInfo.length;
        }

        @Override
        public void write(
                ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator) {

            ByteBuf byteBuf = allocateBuffer(allocator, ID, getContentLength());
            byteBuf.writeInt(version);
            channelID.writeTo(byteBuf);
            byteBuf.writeInt(credit);
            byteBuf.writeInt(regionIdx);
            byteBuf.writeInt(extraInfo.length);
            byteBuf.writeBytes(extraInfo);
            out.write(byteBuf, promise);
        }

        public int getVersion() {
            return version;
        }

        public ChannelID getChannelID() {
            return channelID;
        }

        public int getCredit() {
            return credit;
        }

        public int getRegionIdx() {
            return regionIdx;
        }

        public String getExtraInfo() {
            return bytesToString(extraInfo);
        }

        @Override
        public String toString() {
            return String.format(
                    "WriteAddCredit{%d, %s, credit=%d, regionIdx=%d, extraInfo=%s}",
                    version, channelID, credit, regionIdx, bytesToString(extraInfo));
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

        public static WriteData initByHeader(ByteBuf byteBuf) {
            int version = byteBuf.readInt();
            ChannelID channelID = ChannelID.readFrom(byteBuf);
            int subIdx = byteBuf.readInt();
            int bufferSize = byteBuf.readInt();
            boolean isRegionFinish = byteBuf.readBoolean();
            int extraInfoLen = byteBuf.readInt();
            byte[] extraInfoBytes = new byte[extraInfoLen];
            byteBuf.readBytes(extraInfoBytes);
            String extraInfo = bytesToString(extraInfoBytes);
            return new WriteData(
                    version, channelID, null, subIdx, bufferSize, isRegionFinish, extraInfo);
        }

        @Override
        public int getContentLength() {
            throw new UnsupportedOperationException("Unsupported and should not be called.");
        }

        private int footprintExceptData() {
            return 4 + channelID.getFootprint() + 4 + 4 + 1 + 4 + extraInfo.length;
        }

        @Override
        public void write(
                ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator) {

            ByteBuf byteBuf = allocateBuffer(allocator, ID, footprintExceptData());
            byteBuf.writeInt(version);
            channelID.writeTo(byteBuf);
            byteBuf.writeInt(subIdx);
            byteBuf.writeInt(bufferSize);
            byteBuf.writeBoolean(false);
            byteBuf.writeInt(extraInfo.length);
            byteBuf.writeBytes(extraInfo);
            out.write(byteBuf);
            out.write(body, promise);
        }

        public int getVersion() {
            return version;
        }

        public ChannelID getChannelID() {
            return channelID;
        }

        public int getSubIdx() {
            return subIdx;
        }

        public ByteBuf getBuffer() {
            return body;
        }

        public void setBuffer(ByteBuf body) {
            this.body = body;
        }

        public int getBufferSize() {
            return bufferSize;
        }

        public String getExtraInfo() {
            return bytesToString(extraInfo);
        }

        public boolean isRegionFinish() {
            return isRegionFinish;
        }

        @Override
        public String toString() {
            return String.format(
                    "WriteData{%d, %s, bufferSize=%d, subIdx=%d, isRegionFinish=%s, extraInfo=%s}",
                    version,
                    channelID,
                    bufferSize,
                    subIdx,
                    isRegionFinish,
                    bytesToString(extraInfo));
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

        public static WriteRegionStart readFrom(ByteBuf byteBuf) {
            int version = byteBuf.readInt();
            ChannelID channelID = ChannelID.readFrom(byteBuf);
            int regionIdx = byteBuf.readInt();
            boolean isBroadcast = byteBuf.readBoolean();
            int extraInfoLen = byteBuf.readInt();
            byte[] extraInfoBytes = new byte[extraInfoLen];
            byteBuf.readBytes(extraInfoBytes);
            String extraInfo = bytesToString(extraInfoBytes);
            return new WriteRegionStart(version, channelID, regionIdx, isBroadcast, extraInfo);
        }

        @Override
        public int getContentLength() {
            return 4 + channelID.getFootprint() + 4 + 1 + 4 + extraInfo.length;
        }

        @Override
        public void write(
                ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator) {

            ByteBuf byteBuf = allocateBuffer(allocator, ID, getContentLength());
            byteBuf.writeInt(version);
            channelID.writeTo(byteBuf);
            byteBuf.writeInt(regionIdx);
            byteBuf.writeBoolean(isBroadcast);
            byteBuf.writeInt(extraInfo.length);
            byteBuf.writeBytes(extraInfo);
            out.write(byteBuf, promise);
        }

        public int getVersion() {
            return version;
        }

        public ChannelID getChannelID() {
            return channelID;
        }

        public int getRegionIdx() {
            return regionIdx;
        }

        public boolean isBroadcast() {
            return isBroadcast;
        }

        public String getExtraInfo() {
            return bytesToString(extraInfo);
        }

        @Override
        public String toString() {
            return String.format(
                    "WriteRegionStart{%d, %s, regionIdx=%d, broadcast=%b, extraInfo=%s}",
                    version, channelID, regionIdx, isBroadcast, bytesToString(extraInfo));
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

        public static WriteRegionFinish readFrom(ByteBuf byteBuf) {
            int version = byteBuf.readInt();
            ChannelID channelID = ChannelID.readFrom(byteBuf);
            int extraInfoLen = byteBuf.readInt();
            byte[] extraInfoBytes = new byte[extraInfoLen];
            byteBuf.readBytes(extraInfoBytes);
            String extraInfo = bytesToString(extraInfoBytes);
            return new WriteRegionFinish(version, channelID, extraInfo);
        }

        @Override
        public int getContentLength() {
            return 4 + channelID.getFootprint() + 4 + extraInfo.length;
        }

        @Override
        public void write(
                ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator) {

            ByteBuf byteBuf = allocateBuffer(allocator, ID, getContentLength());
            byteBuf.writeInt(version);
            channelID.writeTo(byteBuf);
            byteBuf.writeInt(extraInfo.length);
            byteBuf.writeBytes(extraInfo);
            out.write(byteBuf, promise);
        }

        public int getVersion() {
            return version;
        }

        public ChannelID getChannelID() {
            return channelID;
        }

        public String getExtraInfo() {
            return bytesToString(extraInfo);
        }

        @Override
        public String toString() {
            return String.format(
                    "WriteRegionFinish{%d, %s, extraInfo=%s}",
                    version, channelID, bytesToString(extraInfo));
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

        public static WriteFinish readFrom(ByteBuf byteBuf) {
            int version = byteBuf.readInt();
            ChannelID channelID = ChannelID.readFrom(byteBuf);
            int extraInfoLen = byteBuf.readInt();
            byte[] extraInfoBytes = new byte[extraInfoLen];
            byteBuf.readBytes(extraInfoBytes);
            String extraInfo = bytesToString(extraInfoBytes);
            return new WriteFinish(version, channelID, extraInfo);
        }

        @Override
        public int getContentLength() {
            return 4 + channelID.getFootprint() + 4 + extraInfo.length;
        }

        @Override
        public void write(
                ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator) {

            ByteBuf byteBuf = allocateBuffer(allocator, ID, getContentLength());
            byteBuf.writeInt(version);
            channelID.writeTo(byteBuf);
            byteBuf.writeInt(extraInfo.length);
            byteBuf.writeBytes(extraInfo);
            out.write(byteBuf, promise);
        }

        public int getVersion() {
            return version;
        }

        public ChannelID getChannelID() {
            return channelID;
        }

        public String getExtraInfo() {
            return bytesToString(extraInfo);
        }

        @Override
        public String toString() {
            return String.format(
                    "WriteFinish{%d, %s, extraInfo=%s}",
                    version, channelID, bytesToString(extraInfo));
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

        public static WriteFinishCommit readFrom(ByteBuf byteBuf) {
            int version = byteBuf.readInt();
            ChannelID channelID = ChannelID.readFrom(byteBuf);
            int extraInfoLen = byteBuf.readInt();
            byte[] extraInfoBytes = new byte[extraInfoLen];
            byteBuf.readBytes(extraInfoBytes);
            String extraInfo = bytesToString(extraInfoBytes);
            return new WriteFinishCommit(version, channelID, extraInfo);
        }

        @Override
        public int getContentLength() {
            return 4 + channelID.getFootprint() + 4 + extraInfo.length;
        }

        @Override
        public void write(
                ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator) {

            ByteBuf byteBuf = allocateBuffer(allocator, ID, getContentLength());
            byteBuf.writeInt(version);
            channelID.writeTo(byteBuf);
            byteBuf.writeInt(extraInfo.length);
            byteBuf.writeBytes(extraInfo);
            out.write(byteBuf, promise);
        }

        public int getVersion() {
            return version;
        }

        public ChannelID getChannelID() {
            return channelID;
        }

        public String getExtraInfo() {
            return bytesToString(extraInfo);
        }

        @Override
        public String toString() {
            return String.format(
                    "WriteFinishCommit{%d, %s, extraInfo=%s}",
                    version, channelID, bytesToString(extraInfo));
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

        public static ReadHandshakeRequest readFrom(ByteBuf byteBuf) {
            int version = byteBuf.readInt();
            ChannelID channelID = ChannelID.readFrom(byteBuf);
            DataSetID dataSetID = DataSetID.readFrom(byteBuf);
            MapPartitionID mapID = MapPartitionID.readFrom(byteBuf);
            int startSubIdx = byteBuf.readInt();
            int endSubIdx = byteBuf.readInt();
            int initialCredit = byteBuf.readInt();
            int bufferSize = byteBuf.readInt();
            long offset = byteBuf.readLong();
            int extraInfoLen = byteBuf.readInt();
            byte[] extraInfoBytes = new byte[extraInfoLen];
            byteBuf.readBytes(extraInfoBytes);
            String extraInfo = bytesToString(extraInfoBytes);
            return new ReadHandshakeRequest(
                    version,
                    channelID,
                    dataSetID,
                    mapID,
                    startSubIdx,
                    endSubIdx,
                    initialCredit,
                    bufferSize,
                    offset,
                    extraInfo);
        }

        @Override
        public int getContentLength() {
            return 4
                    + channelID.getFootprint()
                    + dataSetID.getFootprint()
                    + mapID.getFootprint()
                    + 4
                    + 4
                    + 4
                    + 4
                    + 8
                    + 4
                    + extraInfo.length;
        }

        @Override
        public void write(
                ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator) {

            ByteBuf byteBuf = allocateBuffer(allocator, ID, getContentLength());
            byteBuf.writeInt(version);
            channelID.writeTo(byteBuf);
            dataSetID.writeTo(byteBuf);
            mapID.writeTo(byteBuf);
            byteBuf.writeInt(startSubIdx);
            byteBuf.writeInt(endSubIdx);
            byteBuf.writeInt(initialCredit);
            byteBuf.writeInt(bufferSize);
            byteBuf.writeLong(offset);
            byteBuf.writeInt(extraInfo.length);
            byteBuf.writeBytes(extraInfo);
            out.write(byteBuf, promise);
        }

        public int getVersion() {
            return version;
        }

        public ChannelID getChannelID() {
            return channelID;
        }

        public DataSetID getDataSetID() {
            return dataSetID;
        }

        public MapPartitionID getMapID() {
            return mapID;
        }

        public int getStartSubIdx() {
            return startSubIdx;
        }

        public int getEndSubIdx() {
            return endSubIdx;
        }

        public int getInitialCredit() {
            return initialCredit;
        }

        public int getBufferSize() {
            return bufferSize;
        }

        public long getOffset() {
            return offset;
        }

        public String getExtraInfo() {
            return bytesToString(extraInfo);
        }

        @Override
        public String toString() {
            return String.format(
                    "ReadHandshakeRequest{%d, %s, %s, %s, "
                            + "startSubIdx=%d, endSubIdx=%d, initialCredit=%d, bufferSize=%d, "
                            + "offset=%d, extraInfo=%s}",
                    version,
                    channelID,
                    dataSetID,
                    mapID,
                    startSubIdx,
                    endSubIdx,
                    initialCredit,
                    bufferSize,
                    offset,
                    bytesToString(extraInfo));
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

        public static ReadAddCredit readFrom(ByteBuf byteBuf) {
            int version = byteBuf.readInt();
            ChannelID channelID = ChannelID.readFrom(byteBuf);
            int credit = byteBuf.readInt();
            int extraInfoLen = byteBuf.readInt();
            byte[] extraInfoBytes = new byte[extraInfoLen];
            byteBuf.readBytes(extraInfoBytes);
            String extraInfo = bytesToString(extraInfoBytes);
            return new ReadAddCredit(version, channelID, credit, extraInfo);
        }

        @Override
        public int getContentLength() {
            return 4 + channelID.getFootprint() + 4 + 4 + extraInfo.length;
        }

        @Override
        public void write(
                ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator) {

            ByteBuf byteBuf = allocateBuffer(allocator, ID, getContentLength());
            byteBuf.writeInt(version);
            channelID.writeTo(byteBuf);
            byteBuf.writeInt(credit);
            byteBuf.writeInt(extraInfo.length);
            byteBuf.writeBytes(extraInfo);
            out.write(byteBuf, promise);
        }

        public int getVersion() {
            return version;
        }

        public ChannelID getChannelID() {
            return channelID;
        }

        public int getCredit() {
            return credit;
        }

        public String getExtraInfo() {
            return bytesToString(extraInfo);
        }

        @Override
        public String toString() {
            return String.format(
                    "ReadAddCredit{%d, %s, credit=%d, extraInfo=%s}",
                    version, channelID, credit, bytesToString(extraInfo));
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

        public static ReadData initByHeader(ByteBuf byteBuf) {
            int version = byteBuf.readInt();
            ChannelID channelID = ChannelID.readFrom(byteBuf);
            int backlog = byteBuf.readInt();
            int bufferSize = byteBuf.readInt();
            long offset = byteBuf.readLong();
            int extraInfoLen = byteBuf.readInt();
            byte[] extraInfoBytes = new byte[extraInfoLen];
            byteBuf.readBytes(extraInfoBytes);
            String extraInfo = bytesToString(extraInfoBytes);
            return new ReadData(version, channelID, backlog, bufferSize, offset, null, extraInfo);
        }

        @Override
        public int getContentLength() {
            throw new UnsupportedOperationException("Unsupported and should not be called.");
        }

        private int footprintExceptData() {
            return 4 + channelID.getFootprint() + 4 + 4 + 8 + 4 + extraInfo.length;
        }

        @Override
        public void write(
                ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator) {

            ByteBuf byteBuf = allocateBuffer(allocator, ID, footprintExceptData());
            byteBuf.writeInt(version);
            channelID.writeTo(byteBuf);
            byteBuf.writeInt(backlog);
            byteBuf.writeInt(bufferSize);
            byteBuf.writeLong(offset);
            byteBuf.writeInt(extraInfo.length);
            byteBuf.writeBytes(extraInfo);
            out.write(byteBuf);
            out.write(body, promise);
        }

        public int getVersion() {
            return version;
        }

        public ChannelID getChannelID() {
            return channelID;
        }

        public int getBacklog() {
            return backlog;
        }

        public ByteBuf getBuffer() {
            return body;
        }

        public void setBuffer(ByteBuf body) {
            this.body = body;
        }

        public int getBufferSize() {
            return bufferSize;
        }

        public long getOffset() {
            return offset;
        }

        public String getExtraInfo() {
            return bytesToString(extraInfo);
        }

        @Override
        public String toString() {
            return String.format(
                    "ReadData{%d, %s, size=%d, offset=%d, backlog=%d, extraInfo=%s}",
                    version, channelID, bufferSize, offset, backlog, bytesToString(extraInfo));
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

        public static CloseChannel readFrom(ByteBuf byteBuf) {
            int version = byteBuf.readInt();
            ChannelID channelID = ChannelID.readFrom(byteBuf);
            int extraInfoLen = byteBuf.readInt();
            byte[] extraInfoBytes = new byte[extraInfoLen];
            byteBuf.readBytes(extraInfoBytes);
            String extraInfo = bytesToString(extraInfoBytes);
            return new CloseChannel(version, channelID, extraInfo);
        }

        @Override
        public int getContentLength() {
            return 4 + channelID.getFootprint() + 4 + extraInfo.length;
        }

        @Override
        public void write(
                ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator) {

            ByteBuf byteBuf = allocateBuffer(allocator, ID, getContentLength());
            byteBuf.writeInt(version);
            channelID.writeTo(byteBuf);
            byteBuf.writeInt(extraInfo.length);
            byteBuf.writeBytes(extraInfo);
            out.write(byteBuf, promise);
        }

        public int getVersion() {
            return version;
        }

        public ChannelID getChannelID() {
            return channelID;
        }

        public String getExtraInfo() {
            return bytesToString(extraInfo);
        }

        @Override
        public String toString() {
            return String.format(
                    "CloseChannel{%d, %s, extraInfo=%s}",
                    version, channelID, bytesToString(extraInfo));
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

        public static CloseConnection readFrom(ByteBuf byteBuf) {
            byteBuf.readInt();
            int extraInfoLen = byteBuf.readInt();
            byte[] extraInfoBytes = new byte[extraInfoLen];
            byteBuf.readBytes(extraInfoBytes);
            return new CloseConnection();
        }

        @Override
        public int getContentLength() {
            return 4 + 4 + extraInfo.length;
        }

        @Override
        public void write(
                ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator) {

            ByteBuf byteBuf = allocateBuffer(allocator, ID, getContentLength());
            byteBuf.writeInt(version);
            byteBuf.writeInt(extraInfo.length);
            byteBuf.writeBytes(extraInfo);
            out.write(byteBuf, promise);
        }

        public int getVersion() {
            return version;
        }

        public String getExtraInfo() {
            return bytesToString(extraInfo);
        }

        @Override
        public String toString() {
            return String.format("CloseConnection");
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

        public static Heartbeat readFrom(ByteBuf byteBuf) {
            byteBuf.readInt();
            int extraInfoLen = byteBuf.readInt();
            byte[] extraInfoBytes = new byte[extraInfoLen];
            byteBuf.readBytes(extraInfoBytes);
            return new Heartbeat();
        }

        @Override
        public int getContentLength() {
            return 4 + 4 + extraInfo.length;
        }

        @Override
        public void write(
                ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator) {

            ByteBuf byteBuf = allocateBuffer(allocator, ID, getContentLength());
            byteBuf.writeInt(version);
            byteBuf.writeInt(extraInfo.length);
            byteBuf.writeBytes(extraInfo);
            out.write(byteBuf, promise);
        }

        public int getVersion() {
            return version;
        }

        public String getExtraInfo() {
            return bytesToString(extraInfo);
        }

        @Override
        public String toString() {
            return String.format("Heartbeat");
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
        public int getContentLength() {
            return 4 + channelID.getFootprint() + 4 + 4 + extraInfo.length;
        }

        @Override
        public void write(
                ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator) {

            ByteBuf byteBuf = allocateBuffer(allocator, ID, getContentLength());
            byteBuf.writeInt(version);
            channelID.writeTo(byteBuf);
            byteBuf.writeInt(backlog);
            byteBuf.writeInt(extraInfo.length);
            byteBuf.writeBytes(extraInfo);
            out.write(byteBuf, promise);
        }

        public static BacklogAnnouncement readFrom(ByteBuf byteBuf) {
            byteBuf.readInt();
            ChannelID channelID = ChannelID.readFrom(byteBuf);
            int backlog = byteBuf.readInt();
            int extraInfoLen = byteBuf.readInt();
            byte[] extraInfoBytes = new byte[extraInfoLen];
            byteBuf.readBytes(extraInfoBytes);
            return new BacklogAnnouncement(channelID, backlog);
        }

        public ChannelID getChannelID() {
            return channelID;
        }

        public int getBacklog() {
            return backlog;
        }

        public int getVersion() {
            return version;
        }

        public byte[] getExtraInfo() {
            return extraInfo;
        }

        @Override
        public String toString() {
            return String.format(
                    "BacklogAnnouncement{channelID=%s, backlog=%d, version=%d, extraInfo=%s}",
                    channelID, backlog, version, Arrays.toString(extraInfo));
        }
    }
}
