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
import com.alibaba.flink.shuffle.core.config.TransferOptions;
import com.alibaba.flink.shuffle.core.ids.ChannelID;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.core.memory.Buffer;
import com.alibaba.flink.shuffle.core.storage.DataPartitionReadingView;
import com.alibaba.flink.shuffle.core.storage.DataPartitionWritingView;
import com.alibaba.flink.shuffle.core.storage.ReadingViewContext;
import com.alibaba.flink.shuffle.core.storage.WritingViewContext;
import com.alibaba.flink.shuffle.transfer.TransferMessage.CloseChannel;
import com.alibaba.flink.shuffle.transfer.TransferMessage.ErrorResponse;
import com.alibaba.flink.shuffle.transfer.TransferMessage.ReadAddCredit;
import com.alibaba.flink.shuffle.transfer.TransferMessage.ReadData;
import com.alibaba.flink.shuffle.transfer.TransferMessage.ReadHandshakeRequest;
import com.alibaba.flink.shuffle.transfer.TransferMessage.WriteAddCredit;
import com.alibaba.flink.shuffle.transfer.TransferMessage.WriteData;
import com.alibaba.flink.shuffle.transfer.TransferMessage.WriteFinish;
import com.alibaba.flink.shuffle.transfer.TransferMessage.WriteHandshakeRequest;
import com.alibaba.flink.shuffle.transfer.TransferMessage.WriteRegionFinish;
import com.alibaba.flink.shuffle.transfer.TransferMessage.WriteRegionStart;
import com.alibaba.flink.shuffle.transfer.utils.NoOpPartitionedDataStore;

import org.apache.flink.shaded.netty4.io.netty.buffer.AbstractReferenceCountedByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.util.ReferenceCounted;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;
import static com.alibaba.flink.shuffle.common.utils.ProtocolUtils.currentProtocolVersion;
import static com.alibaba.flink.shuffle.common.utils.ProtocolUtils.emptyBufferSize;
import static com.alibaba.flink.shuffle.common.utils.ProtocolUtils.emptyDataPartitionType;
import static com.alibaba.flink.shuffle.common.utils.ProtocolUtils.emptyExtraMessage;
import static com.alibaba.flink.shuffle.common.utils.ProtocolUtils.emptyOffset;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** Test for shuffle server. */
public class ShuffleServerTest extends AbstractNettyTest {

    private ChannelID channelID;

    private MapPartitionID mapID;

    private MapPartitionID mapIDToFail;

    private int numSubs;

    private NettyServer nettyServer;

    private FakedPartitionDataStore dataStore;

    private NettyClient writeNettyClient;

    private NettyClient readNettyClient;

    private DummyChannelInboundHandlerAdaptor writeClientH;

    private DummyChannelInboundHandlerAdaptor readClientH;

    private Channel readChannel;

    private Channel writeChannel;

    private final CreditListener creditListener = new TestCreditListener();

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
        channelID = new ChannelID();
        mapID = new MapPartitionID(CommonUtils.randomBytes(32));
        mapIDToFail = new MapPartitionID(CommonUtils.randomBytes(32));
        numSubs = 2;
        int dataPort = initServer();
        address = new InetSocketAddress(InetAddress.getLocalHost(), dataPort);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        dataStore.shutDown(true);
        nettyServer.shutdown();
        if (writeNettyClient != null) {
            writeNettyClient.shutdown();
        }
        if (readNettyClient != null) {
            readNettyClient.shutdown();
        }
        super.tearDown();
    }

    /** Basic writing routine. */
    @Test
    public void testWritingRoutine() throws Exception {
        initWriteClient();

        // Client sends WriteHandshakeRequest, receives WriteAddCredit.
        writeChannel.writeAndFlush(
                new WriteHandshakeRequest(
                        currentProtocolVersion(),
                        channelID,
                        jobID,
                        dataSetID,
                        mapID,
                        numSubs,
                        emptyBufferSize(),
                        emptyDataPartitionType(),
                        emptyExtraMessage()));
        Queue<ByteBuf> buffersToSend = constructBuffers(2, 3);

        // Client sends WriteRegionStart.
        writeChannel.writeAndFlush(
                new WriteRegionStart(
                        currentProtocolVersion(), channelID, 0, false, emptyExtraMessage()));
        checkUntil(() -> assertEquals(1, dataStore.writingView.getRegionStartCount()));
        checkUntil(() -> assertEquals(1, writeClientH.numMessages()));
        assertTrue(writeClientH.getMsg(0) instanceof WriteAddCredit);
        WriteAddCredit writeAddCredit = (WriteAddCredit) writeClientH.getMsg(0);
        assertEquals(1, writeAddCredit.getCredit());

        // Client sends WriteData, receives WriteAddCredit.
        ByteBuf buffer = buffersToSend.poll();
        writeChannel.writeAndFlush(
                new WriteData(
                        currentProtocolVersion(),
                        channelID,
                        buffer,
                        0,
                        checkNotNull(buffer).readableBytes(),
                        false,
                        emptyExtraMessage()));
        checkUntil(() -> assertEquals(1, dataStore.receivedBuffers.size()));
        checkUntil(() -> assertEquals(2, writeClientH.numMessages()));
        assertTrue(writeClientH.getMsg(1) instanceof WriteAddCredit);
        writeAddCredit = (WriteAddCredit) writeClientH.getMsg(1);
        assertEquals(1, writeAddCredit.getCredit());

        // Client sends WriteData, receives WriteAddCredit.
        buffer = buffersToSend.poll();
        writeChannel.writeAndFlush(
                new WriteData(
                        currentProtocolVersion(),
                        channelID,
                        buffer,
                        0,
                        checkNotNull(buffer).readableBytes(),
                        false,
                        emptyExtraMessage()));
        checkUntil(() -> assertEquals(2, dataStore.receivedBuffers.size()));
        checkUntil(() -> assertEquals(3, writeClientH.numMessages()));
        assertTrue(writeClientH.getMsg(2) instanceof WriteAddCredit);
        writeAddCredit = (WriteAddCredit) writeClientH.getMsg(2);
        assertEquals(1, writeAddCredit.getCredit());

        // Client sends WriteRegionFinish and WriteFinish.
        writeChannel.writeAndFlush(
                new WriteRegionFinish(currentProtocolVersion(), channelID, emptyExtraMessage()));
        checkUntil(() -> assertEquals(1, dataStore.writingView.getRegionFinishCount()));
        writeChannel.writeAndFlush(
                new WriteFinish(currentProtocolVersion(), channelID, emptyExtraMessage()));
        checkUntil(() -> assertTrue(dataStore.writingView.isFinished()));
    }

    /**
     * Failure in {@link DataPartitionWritingView} should trigger and send ErrorResponse to client.
     */
    @Test
    public void testWritingButWritingViewTriggerFailure() throws Exception {
        initWriteClient();

        // Client sends WriteHandshakeRequest, receives WriteAddCredit.
        writeChannel.writeAndFlush(
                new WriteHandshakeRequest(
                        currentProtocolVersion(),
                        channelID,
                        jobID,
                        dataSetID,
                        mapID,
                        numSubs,
                        emptyBufferSize(),
                        emptyDataPartitionType(),
                        emptyExtraMessage()));
        checkUntil(() -> assertTrue(writeClientH.isConnected()));
        checkUntil(() -> assertNotNull(dataStore.writingView));

        // Failure in dataStore.
        dataStore.writingView.triggerFailure(new Exception("Expected exception"));
        checkUntil(() -> assertEquals(1, writeClientH.numMessages()));
        assertTrue(writeClientH.getLastMsg().toString().contains("Exception: Expected exception"));
    }

    /** {@link DataPartitionWritingView} should receive {@link Throwable} when broken connection. */
    @Test
    public void testWritingViewReceiveThrowableWhenBrokenConnection() throws Exception {
        initWriteClient();
        int prevBuffers = transferBufferPool.numBuffers();

        // Client sends WriteHandshakeRequest, receives WriteAddCredit.
        writeChannel.writeAndFlush(
                new WriteHandshakeRequest(
                        currentProtocolVersion(),
                        channelID,
                        jobID,
                        dataSetID,
                        mapID,
                        numSubs,
                        emptyBufferSize(),
                        emptyDataPartitionType(),
                        emptyExtraMessage()));
        checkUntil(() -> assertTrue(writeClientH.isConnected()));
        checkUntil(() -> assertNotNull(dataStore.writingView));
        checkUntil(() -> assertEquals(prevBuffers - 1, transferBufferPool.numBuffers()));

        // Client closes connection.
        writeClientH.close();
        Class<?> clazz = ShuffleException.class;
        checkUntil(() -> assertEquals(clazz, dataStore.writingView.getError().getClass()));
        checkUntil(() -> assertEquals(prevBuffers, transferBufferPool.numBuffers()));
    }

    // Test CloseChannel.
    @Test
    public void testWritingViewReceiveThrowableWhenChannelClosed() throws Exception {
        initWriteClient();

        checkUntil(() -> assertTrue(writeClientH.isConnected()));
        writeChannel.writeAndFlush(
                new WriteHandshakeRequest(
                        currentProtocolVersion(),
                        channelID,
                        jobID,
                        dataSetID,
                        mapID,
                        numSubs,
                        emptyBufferSize(),
                        emptyDataPartitionType(),
                        emptyExtraMessage()));
        writeClientH.send(
                new CloseChannel(currentProtocolVersion(), channelID, emptyExtraMessage()));
        checkUntil(() -> assertNotNull(dataStore.writingView.getError()));
        assertTrue(
                dataStore
                        .writingView
                        .getError()
                        .getMessage()
                        .contains("Channel closed abnormally"));
    }

    // Decoding error should result in ErrorResponse.
    @Test
    public void testDecodingErrorWillCauseErrorResponse() throws Exception {
        initWriteClient();

        writeChannel.writeAndFlush(
                new WriteHandshakeRequest(
                        currentProtocolVersion(),
                        channelID,
                        jobID,
                        dataSetID,
                        mapID,
                        numSubs,
                        emptyBufferSize(),
                        emptyDataPartitionType(),
                        emptyExtraMessage()));
        writeChannel.writeAndFlush(
                new WriteRegionStart(
                        currentProtocolVersion(), channelID, 0, false, emptyExtraMessage()));
        checkUntil(() -> assertTrue(writeClientH.isConnected()));
        checkUntil(() -> assertNotNull(dataStore.writingView));
        List<Buffer> candidateBuffers = dataStore.writingView.getCandidateBuffers();
        candidateBuffers.forEach(AbstractReferenceCountedByteBuf::release);
        candidateBuffers.clear();

        Queue<ByteBuf> buffersToSend = constructBuffers(1, 3);
        ByteBuf buffer = buffersToSend.poll();
        writeChannel.writeAndFlush(
                new WriteData(
                        currentProtocolVersion(),
                        channelID,
                        buffer,
                        0,
                        checkNotNull(buffer).readableBytes(),
                        false,
                        emptyExtraMessage()));
        checkUntil(() -> assertEquals(2, writeClientH.numMessages()));
        assertTrue(writeClientH.getLastMsg().getClass() == ErrorResponse.class);
        assertTrue(
                writeClientH
                        .getLastMsg()
                        .toString()
                        .contains("java.lang.IllegalStateException: No buffers available."));
    }

    /** Writing channels share the same connection do not disturb each other. */
    @Test
    public void testWritingChannelsCanShareSameConnection() throws Exception {
        initWriteClient();

        writeChannel.writeAndFlush(
                new WriteHandshakeRequest(
                        currentProtocolVersion(),
                        channelID,
                        jobID,
                        dataSetID,
                        mapID,
                        numSubs,
                        emptyBufferSize(),
                        emptyDataPartitionType(),
                        emptyExtraMessage()));
        writeChannel.writeAndFlush(
                new WriteRegionStart(
                        currentProtocolVersion(), channelID, 0, false, emptyExtraMessage()));
        checkUntil(() -> assertEquals(1, writeClientH.numMessages()));
        assertTrue(writeClientH.getMsg(0) instanceof WriteAddCredit);
        ChannelID channelID1 = new ChannelID();
        writeChannel.writeAndFlush(
                new WriteHandshakeRequest(
                        currentProtocolVersion(),
                        channelID1,
                        jobID,
                        dataSetID,
                        mapIDToFail,
                        numSubs,
                        emptyBufferSize(),
                        emptyDataPartitionType(),
                        emptyExtraMessage()));
        checkUntil(() -> assertEquals(2, writeClientH.numMessages()));
        assertTrue(writeClientH.getMsg(1) instanceof ErrorResponse);
        assertEquals(channelID1, ((ErrorResponse) writeClientH.getMsg(1)).getChannelID());

        writeChannel.writeAndFlush(
                new WriteRegionFinish(currentProtocolVersion(), channelID, emptyExtraMessage()));
        writeChannel.writeAndFlush(
                new WriteFinish(currentProtocolVersion(), channelID, emptyExtraMessage()));
        checkUntil(() -> assertTrue(dataStore.writingView.isFinished()));
    }

    /** Basic reading routine. */
    @Test
    public void testReadingRoutine() throws Exception {
        initReadClient();
        List<ByteBuf> buffersToReceive = new ArrayList<>(constructBuffers(3, 3));

        // Client sends ReadHandshakeRequest.
        readChannel.writeAndFlush(
                new ReadHandshakeRequest(
                        currentProtocolVersion(),
                        channelID,
                        dataSetID,
                        mapID,
                        0,
                        0,
                        1,
                        emptyBufferSize(),
                        emptyOffset(),
                        emptyExtraMessage()));
        checkUntil(() -> assertNotNull(dataStore.readingView));

        // Server sends ReadData.
        dataStore.readingView.notifyBuffers(buffersToReceive.subList(0, 2));
        checkUntil(() -> assertEquals(1, readClientH.numMessages()));
        assertTrue(readClientH.getMsg(0) instanceof ReadData);

        // Client sends ReadAddCredit.
        // Server sends ReadData.
        readChannel.writeAndFlush(
                new ReadAddCredit(currentProtocolVersion(), channelID, 2, emptyExtraMessage()));
        checkUntil(() -> assertEquals(2, readClientH.numMessages()));
        assertTrue(readClientH.getMsg(1) instanceof ReadData);

        // Server sends ReadData.
        dataStore.readingView.notifyBuffers(buffersToReceive.subList(2, 3));
        checkUntil(() -> assertEquals(3, readClientH.numMessages()));
        checkUntil(() -> assertTrue(readClientH.getMsg(2) instanceof ReadData));

        dataStore.readingView.setNoMoreData(true);
        assertTrue(dataStore.readingView.isFinished());

        List<ByteBuf> buffers =
                readClientH.getMessages().stream()
                        .filter(obj -> obj instanceof ReadData)
                        .map(obj -> ((ReadData) obj).getBuffer())
                        .collect(Collectors.toList());
        verifyBuffers(3, 3, buffers);
        buffers.forEach(ReferenceCounted::release);
    }

    /**
     * Failure in {@link DataPartitionReadingView} should trigger and send ErrorResponse to client.
     */
    @Test
    public void testReadingButReadingViewTriggerFailure() throws Exception {
        initReadClient();

        // Client sends ReadHandshakeRequest.
        readChannel.writeAndFlush(
                new ReadHandshakeRequest(
                        currentProtocolVersion(),
                        channelID,
                        dataSetID,
                        mapID,
                        0,
                        0,
                        1,
                        emptyBufferSize(),
                        emptyOffset(),
                        emptyExtraMessage()));
        checkUntil(() -> assertNotNull(dataStore.readingView));

        dataStore.readingView.triggerFailure(new Exception("Expected exception"));

        checkUntil(() -> assertEquals(1, readClientH.numMessages()));
        checkUntil(
                () ->
                        assertTrue(
                                readClientH
                                        .getLastMsg()
                                        .toString()
                                        .contains("Expected exception")));
    }

    /** {@link DataPartitionReadingView} should receive {@link Throwable} when broken connection. */
    @Test
    public void testReadingViewReceiveThrowableWhenBrokenConnection() throws Exception {
        initReadClient();

        // Client sends ReadHandshakeRequest.
        readChannel.writeAndFlush(
                new ReadHandshakeRequest(
                        currentProtocolVersion(),
                        channelID,
                        dataSetID,
                        mapID,
                        0,
                        0,
                        1,
                        emptyBufferSize(),
                        emptyOffset(),
                        emptyExtraMessage()));
        checkUntil(() -> assertNotNull(dataStore.readingView));

        // Client closes connection.
        readClientH.close();
        Class<?> clazz = ShuffleException.class;
        checkUntil(() -> assertEquals(clazz, dataStore.readingView.getError().getClass()));
    }

    // Test CloseChannel.
    @Test
    public void testReadingViewReceiveThrowableWhenChannelClosed() throws Exception {
        initReadClient();

        readChannel.writeAndFlush(
                new ReadHandshakeRequest(
                        currentProtocolVersion(),
                        channelID,
                        dataSetID,
                        mapID,
                        0,
                        0,
                        1,
                        emptyBufferSize(),
                        emptyOffset(),
                        emptyExtraMessage()));
        checkUntil(() -> assertNotNull(dataStore.readingView));

        readChannel.writeAndFlush(
                new CloseChannel(currentProtocolVersion(), channelID, emptyExtraMessage()));
        checkUntil(() -> assertNotNull(dataStore.readingView.getError()));
        assertTrue(
                dataStore
                        .readingView
                        .getError()
                        .getMessage()
                        .contains("Channel closed abnormally"));
    }

    /** DataSender error should result in ErrorResponse. */
    @Test
    public void testDataSenderErrorWillCauseErrorResponse() throws Exception {
        initReadClient();
        readChannel.writeAndFlush(
                new ReadHandshakeRequest(
                        currentProtocolVersion(),
                        channelID,
                        dataSetID,
                        mapID,
                        0,
                        0,
                        1,
                        emptyBufferSize(),
                        emptyOffset(),
                        emptyExtraMessage()));
        checkUntil(() -> assertNotNull(dataStore.readingView));
        dataStore.readingView.setBuffersToSend(
                new ArrayDeque<ByteBuf>() {
                    @Override
                    public ByteBuf poll() {
                        throw new ShuffleException("Poll failure");
                    }
                });
        dataStore.readingView.getDataAvailableListener().notifyDataAvailable();
        checkUntil(() -> assertEquals(1, readClientH.numMessages()));
        assertTrue(readClientH.getLastMsg().getClass() == ErrorResponse.class);
        assertTrue(readClientH.getLastMsg().toString().contains("Poll failure"));
    }

    /** Reading channels share the same connection do not disturb each other. */
    @Test
    public void testReadingChannelsCanShareSameConnection() throws Exception {
        initReadClient();
        List<ByteBuf> buffersToReceive = new ArrayList<>(constructBuffers(3, 3));

        // Client sends ReadHandshakeRequest.
        readChannel.writeAndFlush(
                new ReadHandshakeRequest(
                        currentProtocolVersion(),
                        channelID,
                        dataSetID,
                        mapID,
                        0,
                        0,
                        1,
                        emptyBufferSize(),
                        emptyOffset(),
                        emptyExtraMessage()));
        checkUntil(() -> assertNotNull(dataStore.readingView));

        // Server sends ReadData.
        dataStore.readingView.notifyBuffers(buffersToReceive.subList(0, 2));
        checkUntil(() -> assertEquals(1, readClientH.numMessages()));
        assertTrue(readClientH.getMsg(0) instanceof ReadData);

        // Client sends a handshake to fail the logic channel
        ChannelID channelID1 = new ChannelID();
        readChannel.writeAndFlush(
                new ReadHandshakeRequest(
                        currentProtocolVersion(),
                        channelID1,
                        dataSetID,
                        mapIDToFail,
                        0,
                        0,
                        1,
                        emptyBufferSize(),
                        emptyOffset(),
                        emptyExtraMessage()));
        checkUntil(() -> assertEquals(2, readClientH.numMessages()));
        assertTrue(readClientH.getMsg(1) instanceof ErrorResponse);
        assertEquals(channelID1, ((ErrorResponse) readClientH.getMsg(1)).getChannelID());

        // Client sends ReadAddCredit.
        // Server sends ReadData.
        readChannel.writeAndFlush(
                new ReadAddCredit(currentProtocolVersion(), channelID, 2, emptyExtraMessage()));
        checkUntil(() -> assertEquals(3, readClientH.numMessages()));
        assertTrue(readClientH.getMsg(2) instanceof ReadData);

        // Server sends ReadData.
        dataStore.readingView.notifyBuffers(buffersToReceive.subList(2, 3));
        checkUntil(() -> assertEquals(4, readClientH.numMessages()));
        checkUntil(() -> assertTrue(readClientH.getMsg(3) instanceof ReadData));

        dataStore.readingView.setNoMoreData(true);
        assertTrue(dataStore.readingView.isFinished());

        List<ByteBuf> buffers =
                readClientH.getMessages().stream()
                        .filter(obj -> obj instanceof ReadData)
                        .map(obj -> ((ReadData) obj).getBuffer())
                        .collect(Collectors.toList());
        verifyBuffers(3, 3, buffers);
        buffers.forEach(ReferenceCounted::release);
    }

    private void initWriteClient() throws Exception {
        writeClientH = new DummyChannelInboundHandlerAdaptor();
        writeNettyClient = new NettyClient(nettyConfig);
        writeNettyClient.init(
                () ->
                        new ChannelHandler[] {
                            new TransferMessageEncoder(),
                            DecoderDelegate.writeClientDecoderDelegate(),
                            writeClientH
                        });
        writeChannel = writeNettyClient.connect(address).await().channel();
    }

    private void initReadClient() throws Exception {
        readClientH = new DummyChannelInboundHandlerAdaptor();
        readNettyClient = new NettyClient(nettyConfig);
        readNettyClient.init(
                () ->
                        new ChannelHandler[] {
                            new TransferMessageEncoder(),
                            DecoderDelegate.readClientDecoderDelegate(
                                    ignore -> () -> transferBufferPool.requestBuffer()),
                            readClientH
                        });
        readChannel = readNettyClient.connect(address).await().channel();
    }

    private int initServer() throws Exception {
        dataStore = new FakedPartitionDataStore();
        int dataPort = getAvailablePort();
        nettyConfig.getConfig().setInteger(TransferOptions.SERVER_DATA_PORT, dataPort);
        nettyServer = new NettyServer(dataStore, nettyConfig);
        nettyServer.disableHeartbeat();
        nettyServer.start();
        return dataPort;
    }

    private class FakedPartitionDataStore extends NoOpPartitionedDataStore {

        private final ArrayDeque<ByteBuf> receivedBuffers;

        private FakedDataPartitionWritingView writingView;

        private FakedDataPartitionReadingView readingView;

        public FakedPartitionDataStore() {
            this.receivedBuffers = new ArrayDeque<>();
        }

        @Override
        public DataPartitionWritingView createDataPartitionWritingView(WritingViewContext context) {
            if (context.getMapPartitionID().equals(mapIDToFail)) {
                throw new ShuffleException("Fail a writing handshake.");
            }

            Consumer<Buffer> dataHandler =
                    buffer -> {
                        ByteBuf buffer0 = transferBufferPool.requestBuffer();
                        buffer0.writeBytes(buffer);
                        dataStore.receivedBuffers.add(buffer0);
                    };
            List<Buffer> buffers = new ArrayList<>();
            buffers.add((Buffer) transferBufferPool.requestBuffer());
            writingView =
                    new FakedDataPartitionWritingView(
                            context.getDataSetID(),
                            context.getMapPartitionID(),
                            dataHandler,
                            context.getDataRegionCreditListener(),
                            context.getFailureListener(),
                            buffers);
            return writingView;
        }

        @Override
        public DataPartitionReadingView createDataPartitionReadingView(ReadingViewContext context) {
            if (context.getPartitionID().equals(mapIDToFail)) {
                throw new ShuffleException("Fail a reading handshake.");
            }
            readingView =
                    new FakedDataPartitionReadingView(
                            context.getDataListener(),
                            context.getBacklogListener(),
                            context.getFailureListener());
            return readingView;
        }

        @Override
        public void shutDown(boolean releaseData) {
            receivedBuffers.forEach(ReferenceCounted::release);
        }
    }
}
