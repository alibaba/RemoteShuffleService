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
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.transfer.TransferMessage.CloseChannel;
import com.alibaba.flink.shuffle.transfer.TransferMessage.CloseConnection;
import com.alibaba.flink.shuffle.transfer.TransferMessage.ErrorResponse;
import com.alibaba.flink.shuffle.transfer.TransferMessage.Heartbeat;
import com.alibaba.flink.shuffle.transfer.TransferMessage.ReadAddCredit;
import com.alibaba.flink.shuffle.transfer.TransferMessage.ReadData;
import com.alibaba.flink.shuffle.transfer.TransferMessage.ReadHandshakeRequest;
import com.alibaba.flink.shuffle.transfer.TransferMessage.WriteAddCredit;
import com.alibaba.flink.shuffle.transfer.TransferMessage.WriteData;
import com.alibaba.flink.shuffle.transfer.TransferMessage.WriteFinish;
import com.alibaba.flink.shuffle.transfer.TransferMessage.WriteFinishCommit;
import com.alibaba.flink.shuffle.transfer.TransferMessage.WriteHandshakeRequest;
import com.alibaba.flink.shuffle.transfer.TransferMessage.WriteRegionFinish;
import com.alibaba.flink.shuffle.transfer.TransferMessage.WriteRegionStart;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.flink.shaded.netty4.io.netty.buffer.PooledByteBufAllocator;
import org.apache.flink.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;
import org.apache.flink.shaded.netty4.io.netty.util.ReferenceCounted;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.alibaba.flink.shuffle.common.utils.ProtocolUtils.currentProtocolVersion;
import static com.alibaba.flink.shuffle.common.utils.ProtocolUtils.emptyExtraMessage;
import static com.alibaba.flink.shuffle.common.utils.StringUtils.bytesToString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Test for {@link TransferMessage} encoding and decoding. */
public class EncodingDecodingTest {

    private static final ByteBufAllocator ALLOCATOR = new PooledByteBufAllocator();

    private static final Random random = new Random();

    private final CreditListener creditListener = new TestCreditListener();

    @Test
    public void testReadData() {
        testReadData(100, 128, 128);
        testReadData(100, 128, 100);
        testReadData(100, 128, 200);
        testReadData(1000, 128, 1024);
        testReadData(1000, 128, 1000);
        testReadData(1000, 1024, 128);
        testReadData(1000, 1024, 4096);
        testReadData(1000, 1024, 4000);
    }

    @Test
    public void testWriteData() {
        testWriteData(100, 128, 128);
        testWriteData(100, 128, 100);
        testWriteData(100, 128, 200);
        testWriteData(1000, 128, 1024);
        testWriteData(1000, 128, 1000);
        testWriteData(1000, 1024, 128);
        testWriteData(1000, 1024, 4096);
        testWriteData(1000, 1024, 4000);
    }

    @Test
    public void testErrorResponse() {
        int version = currentProtocolVersion();
        ChannelID channelID = new ChannelID();
        String errMsg = "My Exception.";
        String extraInfo = bytesToString(CommonUtils.randomBytes(32));
        Supplier<ErrorResponse> messageBuilder =
                () -> new ErrorResponse(version, channelID, errMsg.getBytes(), extraInfo);
        Consumer<TransferMessage> messageVerifier =
                msg -> {
                    ErrorResponse errRsp = (ErrorResponse) msg;
                    assertEquals(version, errRsp.getVersion());
                    assertEquals(channelID, errRsp.getChannelID());
                    assertEquals(errMsg, new String(errRsp.getErrorMessageBytes()));
                    assertEquals(extraInfo, errRsp.getExtraInfo());
                };
        testCommonMessage(messageBuilder, messageVerifier, 100, 10);
        testCommonMessage(messageBuilder, messageVerifier, 100, 16);
        testCommonMessage(messageBuilder, messageVerifier, 100, 20);
        testCommonMessage(messageBuilder, messageVerifier, 100, 40);
        testCommonMessage(messageBuilder, messageVerifier, 100, 100);
        testCommonMessage(messageBuilder, messageVerifier, 100, 1024);
    }

    @Test
    public void testWriteHandshakeRequest() {
        int version = currentProtocolVersion();
        ChannelID channelID = new ChannelID();
        JobID jobID = new JobID(CommonUtils.randomBytes(32));
        DataSetID dataSetID = new DataSetID(CommonUtils.randomBytes(32));
        MapPartitionID mapID = new MapPartitionID(CommonUtils.randomBytes(16));
        int numSubs = 1234;
        int bufferSize = random.nextInt();
        String dataPartitionTypeFactory = bytesToString(CommonUtils.randomBytes(32));
        String extraInfo = bytesToString(CommonUtils.randomBytes(32));
        Supplier<WriteHandshakeRequest> messageBuilder =
                () ->
                        new WriteHandshakeRequest(
                                version,
                                channelID,
                                jobID,
                                dataSetID,
                                mapID,
                                numSubs,
                                bufferSize,
                                dataPartitionTypeFactory,
                                extraInfo);
        Consumer<TransferMessage> messageVerifier =
                msg -> {
                    WriteHandshakeRequest tmp = (WriteHandshakeRequest) msg;
                    assertEquals(version, tmp.getVersion());
                    assertEquals(channelID, tmp.getChannelID());
                    assertEquals(jobID, tmp.getJobID());
                    assertEquals(dataSetID, tmp.getDataSetID());
                    assertEquals(mapID, tmp.getMapID());
                    assertEquals(numSubs, tmp.getNumSubs());
                    assertEquals(bufferSize, tmp.getBufferSize());
                    assertEquals(dataPartitionTypeFactory, tmp.getDataPartitionType());
                    assertEquals(extraInfo, tmp.getExtraInfo());
                };
        testCommonMessage(messageBuilder, messageVerifier, 100, 20);
        testCommonMessage(messageBuilder, messageVerifier, 100, 32);
        testCommonMessage(messageBuilder, messageVerifier, 100, 96);
        testCommonMessage(messageBuilder, messageVerifier, 100, 100);
        testCommonMessage(messageBuilder, messageVerifier, 100, 200);
        testCommonMessage(messageBuilder, messageVerifier, 100, 1024);
    }

    @Test
    public void testWriteAddCredit() {
        int version = currentProtocolVersion();
        ChannelID channelID = new ChannelID();
        int credit = 123;
        int regionIdx = 789;
        String extraInfo = bytesToString(CommonUtils.randomBytes(32));
        Supplier<WriteAddCredit> messageBuilder =
                () -> new WriteAddCredit(version, channelID, credit, regionIdx, extraInfo);
        Consumer<TransferMessage> messageVerifier =
                msg -> {
                    WriteAddCredit tmp = (WriteAddCredit) msg;
                    assertEquals(version, tmp.getVersion());
                    assertEquals(channelID, tmp.getChannelID());
                    assertEquals(credit, tmp.getCredit());
                    assertEquals(regionIdx, tmp.getRegionIdx());
                    assertEquals(extraInfo, tmp.getExtraInfo());
                };
        testCommonMessage(messageBuilder, messageVerifier, 100, 10);
        testCommonMessage(messageBuilder, messageVerifier, 100, 16);
        testCommonMessage(messageBuilder, messageVerifier, 100, 20);
        testCommonMessage(messageBuilder, messageVerifier, 100, 40);
        testCommonMessage(messageBuilder, messageVerifier, 100, 100);
        testCommonMessage(messageBuilder, messageVerifier, 100, 1024);
    }

    @Test
    public void testWriteRegionStart() {
        int version = currentProtocolVersion();
        ChannelID channelID = new ChannelID();
        int regionIdx = 123;
        boolean isBroadcast = false;
        String extraInfo = bytesToString(CommonUtils.randomBytes(32));
        Supplier<WriteRegionStart> messageBuilder =
                () -> new WriteRegionStart(version, channelID, regionIdx, isBroadcast, extraInfo);
        Consumer<TransferMessage> messageVerifier =
                msg -> {
                    WriteRegionStart tmp = (WriteRegionStart) msg;
                    assertEquals(version, tmp.getVersion());
                    assertEquals(channelID, tmp.getChannelID());
                    assertEquals(regionIdx, tmp.getRegionIdx());
                    assertEquals(isBroadcast, tmp.isBroadcast());
                    assertEquals(extraInfo, tmp.getExtraInfo());
                };
        testCommonMessage(messageBuilder, messageVerifier, 100, 10);
        testCommonMessage(messageBuilder, messageVerifier, 100, 16);
        testCommonMessage(messageBuilder, messageVerifier, 100, 20);
        testCommonMessage(messageBuilder, messageVerifier, 100, 40);
        testCommonMessage(messageBuilder, messageVerifier, 100, 100);
        testCommonMessage(messageBuilder, messageVerifier, 100, 1024);
    }

    @Test
    public void testWriteRegionFinish() {
        int version = currentProtocolVersion();
        ChannelID channelID = new ChannelID();
        String extraInfo = bytesToString(CommonUtils.randomBytes(32));
        Supplier<WriteRegionFinish> messageBuilder =
                () -> new WriteRegionFinish(version, channelID, extraInfo);
        Consumer<TransferMessage> messageVerifier =
                msg -> {
                    WriteRegionFinish tmp = (WriteRegionFinish) msg;
                    assertEquals(version, tmp.getVersion());
                    assertEquals(channelID, tmp.getChannelID());
                    assertEquals(extraInfo, tmp.getExtraInfo());
                };
        testCommonMessage(messageBuilder, messageVerifier, 100, 10);
        testCommonMessage(messageBuilder, messageVerifier, 100, 16);
        testCommonMessage(messageBuilder, messageVerifier, 100, 20);
        testCommonMessage(messageBuilder, messageVerifier, 100, 32);
        testCommonMessage(messageBuilder, messageVerifier, 100, 100);
        testCommonMessage(messageBuilder, messageVerifier, 100, 1024);
    }

    @Test
    public void testWriteFinish() {
        int version = currentProtocolVersion();
        ChannelID channelID = new ChannelID();
        String extraInfo = bytesToString(CommonUtils.randomBytes(32));
        Supplier<WriteFinish> messageBuilder = () -> new WriteFinish(version, channelID, extraInfo);
        Consumer<TransferMessage> messageVerifier =
                msg -> {
                    WriteFinish tmp = (WriteFinish) msg;
                    assertEquals(version, tmp.getVersion());
                    assertEquals(channelID, tmp.getChannelID());
                    assertEquals(extraInfo, tmp.getExtraInfo());
                };
        testCommonMessage(messageBuilder, messageVerifier, 100, 10);
        testCommonMessage(messageBuilder, messageVerifier, 100, 16);
        testCommonMessage(messageBuilder, messageVerifier, 100, 20);
        testCommonMessage(messageBuilder, messageVerifier, 100, 32);
        testCommonMessage(messageBuilder, messageVerifier, 100, 100);
        testCommonMessage(messageBuilder, messageVerifier, 100, 1024);
    }

    @Test
    public void testWriteFinishCommit() {
        int version = currentProtocolVersion();
        ChannelID channelID = new ChannelID();
        String extraInfo = bytesToString(CommonUtils.randomBytes(32));
        Supplier<WriteFinishCommit> messageBuilder =
                () -> new WriteFinishCommit(version, channelID, extraInfo);
        Consumer<TransferMessage> messageVerifier =
                msg -> {
                    WriteFinishCommit tmp = (WriteFinishCommit) msg;
                    assertEquals(version, tmp.getVersion());
                    assertEquals(channelID, tmp.getChannelID());
                    assertEquals(extraInfo, tmp.getExtraInfo());
                };
        testCommonMessage(messageBuilder, messageVerifier, 100, 10);
        testCommonMessage(messageBuilder, messageVerifier, 100, 16);
        testCommonMessage(messageBuilder, messageVerifier, 100, 20);
        testCommonMessage(messageBuilder, messageVerifier, 100, 32);
        testCommonMessage(messageBuilder, messageVerifier, 100, 100);
        testCommonMessage(messageBuilder, messageVerifier, 100, 1024);
    }

    @Test
    public void testReadHandshakeRequest() {
        int version = currentProtocolVersion();
        ChannelID channelID = new ChannelID();
        DataSetID dataSetID = new DataSetID(CommonUtils.randomBytes(32));
        MapPartitionID mapID = new MapPartitionID(CommonUtils.randomBytes(16));
        int startSubIdx = 123;
        int endSubIdx = 456;
        int initialCredit = 789;
        int bufferSize = random.nextInt();
        int offset = random.nextInt();
        String extraInfo = bytesToString(CommonUtils.randomBytes(32));
        Supplier<ReadHandshakeRequest> messageBuilder =
                () ->
                        new ReadHandshakeRequest(
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
        Consumer<TransferMessage> messageVerifier =
                msg -> {
                    ReadHandshakeRequest tmp = (ReadHandshakeRequest) msg;
                    assertEquals(version, tmp.getVersion());
                    assertEquals(channelID, tmp.getChannelID());
                    assertEquals(dataSetID, tmp.getDataSetID());
                    assertEquals(mapID, tmp.getMapID());
                    assertEquals(startSubIdx, tmp.getStartSubIdx());
                    assertEquals(endSubIdx, tmp.getEndSubIdx());
                    assertEquals(initialCredit, tmp.getInitialCredit());
                    assertEquals(bufferSize, tmp.getBufferSize());
                    assertEquals(offset, tmp.getOffset());
                    assertEquals(extraInfo, tmp.getExtraInfo());
                };
        testCommonMessage(messageBuilder, messageVerifier, 100, 20);
        testCommonMessage(messageBuilder, messageVerifier, 100, 72);
        testCommonMessage(messageBuilder, messageVerifier, 100, 144);
        testCommonMessage(messageBuilder, messageVerifier, 100, 200);
        testCommonMessage(messageBuilder, messageVerifier, 100, 1024);
    }

    @Test
    public void testReadAddCredit() {
        int version = currentProtocolVersion();
        ChannelID channelID = new ChannelID();
        int credit = 123;
        String extraInfo = bytesToString(CommonUtils.randomBytes(32));
        Supplier<ReadAddCredit> messageBuilder =
                () -> new ReadAddCredit(version, channelID, credit, extraInfo);
        Consumer<TransferMessage> messageVerifier =
                msg -> {
                    ReadAddCredit tmp = (ReadAddCredit) msg;
                    assertEquals(version, tmp.getVersion());
                    assertEquals(channelID, tmp.getChannelID());
                    assertEquals(credit, tmp.getCredit());
                    assertEquals(extraInfo, tmp.getExtraInfo());
                };
        testCommonMessage(messageBuilder, messageVerifier, 100, 10);
        testCommonMessage(messageBuilder, messageVerifier, 100, 16);
        testCommonMessage(messageBuilder, messageVerifier, 100, 20);
        testCommonMessage(messageBuilder, messageVerifier, 100, 40);
        testCommonMessage(messageBuilder, messageVerifier, 100, 100);
        testCommonMessage(messageBuilder, messageVerifier, 100, 1024);
    }

    @Test
    public void testCloseChannel() {
        int version = currentProtocolVersion();
        ChannelID channelID = new ChannelID();
        String extraInfo = bytesToString(CommonUtils.randomBytes(32));
        Supplier<CloseChannel> messageBuilder =
                () -> new CloseChannel(version, channelID, extraInfo);
        Consumer<TransferMessage> messageVerifier =
                msg -> {
                    CloseChannel tmp = (CloseChannel) msg;
                    assertEquals(version, tmp.getVersion());
                    assertEquals(channelID, tmp.getChannelID());
                    assertEquals(extraInfo, tmp.getExtraInfo());
                };
        testCommonMessage(messageBuilder, messageVerifier, 100, 10);
        testCommonMessage(messageBuilder, messageVerifier, 100, 16);
        testCommonMessage(messageBuilder, messageVerifier, 100, 20);
        testCommonMessage(messageBuilder, messageVerifier, 100, 32);
        testCommonMessage(messageBuilder, messageVerifier, 100, 100);
        testCommonMessage(messageBuilder, messageVerifier, 100, 1024);
    }

    @Test
    public void testCloseConnection() {
        int version = currentProtocolVersion();
        String extraInfo = emptyExtraMessage();
        Supplier<CloseConnection> messageBuilder = CloseConnection::new;
        Consumer<TransferMessage> messageVerifier =
                msg -> {
                    assertTrue(msg instanceof CloseConnection);
                    CloseConnection tmp = (CloseConnection) msg;
                    assertEquals(version, tmp.getVersion());
                    assertEquals(extraInfo, tmp.getExtraInfo());
                };

        testCommonMessage(messageBuilder, messageVerifier, 100, 1);
        testCommonMessage(messageBuilder, messageVerifier, 100, 2);
        testCommonMessage(messageBuilder, messageVerifier, 100, 3);
        testCommonMessage(messageBuilder, messageVerifier, 100, 4);
        testCommonMessage(messageBuilder, messageVerifier, 100, 8);
        testCommonMessage(messageBuilder, messageVerifier, 100, 16);
        testCommonMessage(messageBuilder, messageVerifier, 100, 100);
        testCommonMessage(messageBuilder, messageVerifier, 100, 1024);
    }

    @Test
    public void testHeartbeat() {
        int version = currentProtocolVersion();
        String extraInfo = emptyExtraMessage();
        Supplier<Heartbeat> messageBuilder = Heartbeat::new;
        Consumer<TransferMessage> messageVerifier =
                msg -> {
                    assertTrue(msg instanceof Heartbeat);
                    Heartbeat tmp = (Heartbeat) msg;
                    assertEquals(version, tmp.getVersion());
                    assertEquals(extraInfo, tmp.getExtraInfo());
                };

        testCommonMessage(messageBuilder, messageVerifier, 100, 1);
        testCommonMessage(messageBuilder, messageVerifier, 100, 2);
        testCommonMessage(messageBuilder, messageVerifier, 100, 3);
        testCommonMessage(messageBuilder, messageVerifier, 100, 4);
        testCommonMessage(messageBuilder, messageVerifier, 100, 8);
        testCommonMessage(messageBuilder, messageVerifier, 100, 16);
        testCommonMessage(messageBuilder, messageVerifier, 100, 100);
        testCommonMessage(messageBuilder, messageVerifier, 100, 1024);
    }

    private void testCommonMessage(
            Supplier<? extends TransferMessage> messageBuilder,
            Consumer<TransferMessage> messageVerifier,
            int numMessages,
            int targetChopSize) {
        List<TransferMessage> messages = new ArrayList<>();
        for (int i = 0; i < numMessages; i++) {
            messages.add(messageBuilder.get());
        }
        EmbeddedChannel channel =
                new EmbeddedChannel(new TransferMessageEncoder(), getDecoderDelegate(null));
        List<ByteBuf> encoded = encode(channel, messages);
        List<ByteBuf> chopped = chop(encoded, targetChopSize);
        chopped.forEach(channel::writeInbound);
        List<TransferMessage> receivedList = new ArrayList<>();
        TransferMessage received = null;
        while ((received = channel.readInbound()) != null) {
            receivedList.add(received);
        }
        assertEquals(numMessages, receivedList.size());
        receivedList.forEach(messageVerifier);
    }

    private void testReadData(int numReadDatas, int bufferSize, int targetChopSize) {
        int version = currentProtocolVersion();
        ChannelID channelID = new ChannelID();
        int offset = random.nextInt();
        String extraInfo = bytesToString(CommonUtils.randomBytes(32));
        int backlog = 789;

        TransferBufferPool transferBufferPool =
                new TestTransferBufferPool(numReadDatas * 2, bufferSize);
        EmbeddedChannel channel =
                new EmbeddedChannel(
                        new TransferMessageEncoder(), getDecoderDelegate(transferBufferPool));

        List<ByteBuf> buffers = constructBuffers(transferBufferPool, numReadDatas);
        int expectSize = getTotalSize(buffers);
        List<ReadData> readDatas =
                buffers.stream()
                        .map(
                                byteBuf ->
                                        new ReadData(
                                                version,
                                                channelID,
                                                backlog,
                                                byteBuf.readableBytes(),
                                                offset,
                                                byteBuf,
                                                extraInfo))
                        .collect(Collectors.toList());
        List<ByteBuf> encoded = encode(channel, readDatas);

        List<ByteBuf> chopped = chop(encoded, targetChopSize);
        chopped.forEach(channel::writeInbound);
        List<ReadData> receivedList = new ArrayList<>();
        ReadData received = null;
        while ((received = channel.readInbound()) != null) {
            receivedList.add(received);
        }
        assertEquals(numReadDatas, receivedList.size());
        receivedList.forEach(
                readData -> {
                    assertEquals(version, readData.getVersion());
                    assertEquals(channelID, readData.getChannelID());
                    assertEquals(offset, readData.getOffset());
                    assertEquals(backlog, readData.getBacklog());
                    assertEquals(extraInfo, readData.getExtraInfo());
                });
        List<ByteBuf> byteBufs =
                receivedList.stream().map(ReadData::getBuffer).collect(Collectors.toList());
        verifyBuffers(byteBufs, expectSize);
    }

    private void testWriteData(int numWriteDatas, int bufferSize, int targetChopSize) {
        int version = currentProtocolVersion();
        ChannelID channelID = new ChannelID();
        String extraInfo = bytesToString(CommonUtils.randomBytes(32));
        int subIdx = 789;

        TransferBufferPool transferBufferPool =
                new TestTransferBufferPool(numWriteDatas * 2, bufferSize);
        EmbeddedChannel channel =
                new EmbeddedChannel(
                        new TransferMessageEncoder(), getDecoderDelegate(transferBufferPool));

        List<ByteBuf> buffers = constructBuffers(transferBufferPool, numWriteDatas);
        int expectSize = getTotalSize(buffers);
        List<WriteData> readDatas =
                buffers.stream()
                        .map(
                                b ->
                                        new WriteData(
                                                version,
                                                channelID,
                                                b,
                                                subIdx,
                                                b.readableBytes(),
                                                false,
                                                extraInfo))
                        .collect(Collectors.toList());
        List<ByteBuf> encoded = encode(channel, readDatas);

        List<ByteBuf> chopped = chop(encoded, targetChopSize);
        chopped.forEach(channel::writeInbound);
        List<WriteData> receivedList = new ArrayList<>();
        WriteData received = null;
        while ((received = channel.readInbound()) != null) {
            receivedList.add(received);
        }
        assertEquals(numWriteDatas, receivedList.size());
        receivedList.forEach(
                writeData -> {
                    assertEquals(version, writeData.getVersion());
                    assertEquals(channelID, writeData.getChannelID());
                    assertEquals(subIdx, writeData.getSubIdx());
                    assertEquals(extraInfo, writeData.getExtraInfo());
                });
        List<ByteBuf> byteBufs =
                receivedList.stream().map(WriteData::getBuffer).collect(Collectors.toList());
        verifyBuffers(byteBufs, expectSize);
    }

    private List<ByteBuf> constructBuffers(TransferBufferPool bufferPool, int numBuffers) {
        List<ByteBuf> res = new ArrayList<>();
        long a = 0;
        for (int i = 0; i < numBuffers; i++) {
            ByteBuf buffer = bufferPool.requestBuffer();
            while (buffer.capacity() - buffer.writerIndex() > 8) {
                buffer.writeLong(a++);
            }
            res.add(buffer);
        }
        return res;
    }

    private int getTotalSize(List<ByteBuf> buffers) {
        return buffers.stream().map(ByteBuf::readableBytes).reduce(0, Integer::sum);
    }

    private List<ByteBuf> encode(
            EmbeddedChannel channel, List<? extends TransferMessage> messages) {
        List<ByteBuf> ret = new ArrayList<>();
        for (Object msg : messages) {
            channel.writeOutbound(msg);
            ByteBuf encoded;
            while ((encoded = channel.readOutbound()) != null) {
                ret.add(encoded);
            }
        }
        return ret;
    }

    private List<ByteBuf> chop(List<ByteBuf> buffers, int targetSize) {
        List<ByteBuf> ret = new ArrayList<>();
        ByteBuf tmp = ALLOCATOR.directBuffer(targetSize);
        for (ByteBuf buffer : buffers) {
            while (buffer.readableBytes() != 0) {
                int numBytes = Math.min(tmp.writableBytes(), buffer.readableBytes());
                tmp.writeBytes(buffer, numBytes);
                if (tmp.writableBytes() == 0) {
                    ret.add(tmp);
                    tmp = ALLOCATOR.directBuffer(targetSize);
                }
            }
            buffer.release();
        }
        if (tmp.readableBytes() == 0) {
            tmp.release();
        } else {
            ret.add(tmp);
        }
        return ret;
    }

    private void verifyBuffers(List<ByteBuf> buffers, int expectedSize) {
        assertEquals(expectedSize, getTotalSize(buffers));
        int a = 0;
        ByteBuf tmp = ALLOCATOR.directBuffer(1024);
        for (ByteBuf buffer : buffers) {
            while (buffer.readableBytes() != 0) {
                int numBytes = Math.min(tmp.writableBytes(), buffer.readableBytes());
                tmp.writeBytes(buffer, numBytes);
                if (tmp.writableBytes() == 0) {
                    while (tmp.readableBytes() > 0) {
                        assertEquals(a++, tmp.readLong());
                    }
                    tmp.clear();
                }
            }
        }
        tmp.release();
        buffers.forEach(ReferenceCounted::release);
    }

    private DecoderDelegate getDecoderDelegate(TransferBufferPool bufferPool) {
        Function<Byte, TransferMessageDecoder> messageDecoder =
                msgID -> {
                    switch (msgID) {
                        case ReadData.ID:
                            return new ShuffleReadDataDecoder(ignore -> bufferPool::requestBuffer);
                        case WriteData.ID:
                            return new ShuffleWriteDataDecoder(ignore -> bufferPool::requestBuffer);
                        default:
                            return new CommonTransferMessageDecoder();
                    }
                };
        return new DecoderDelegate(messageDecoder);
    }
}
