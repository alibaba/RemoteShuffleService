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
import com.alibaba.flink.shuffle.core.config.TransferOptions;
import com.alibaba.flink.shuffle.core.ids.ChannelID;
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.transfer.TransferMessage.CloseChannel;
import com.alibaba.flink.shuffle.transfer.TransferMessage.CloseConnection;
import com.alibaba.flink.shuffle.transfer.TransferMessage.ErrorResponse;
import com.alibaba.flink.shuffle.transfer.TransferMessage.ReadAddCredit;
import com.alibaba.flink.shuffle.transfer.TransferMessage.ReadData;
import com.alibaba.flink.shuffle.transfer.TransferMessage.ReadHandshakeRequest;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.util.ReferenceCounted;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.alibaba.flink.shuffle.common.utils.ProtocolUtils.currentProtocolVersion;
import static com.alibaba.flink.shuffle.common.utils.ProtocolUtils.emptyBufferSize;
import static com.alibaba.flink.shuffle.common.utils.ProtocolUtils.emptyExtraMessage;
import static com.alibaba.flink.shuffle.common.utils.ProtocolUtils.emptyOffset;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Test for {@link ShuffleReadClient}. */
public class ShuffleReadClientTest extends AbstractNettyTest {

    private static final int NUM_BUFFERS = 20;

    private static final int BUFFER_SIZE = 64;

    private NettyServer nettyServer;

    private ShuffleReadClient client;

    private ConnectionManager connManager;

    private TransferBufferPool clientBufferPool;

    private DummyChannelInboundHandlerAdaptor serverHandler;

    private List<ByteBuf> readDatas;

    private final AtomicReference<Throwable> cause = new AtomicReference<>();

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
        int dataPort = getAvailablePort();
        Pair<NettyServer, DummyChannelInboundHandlerAdaptor> pair = initShuffleServer(dataPort);
        nettyServer = pair.getLeft();
        serverHandler = pair.getRight();

        MapPartitionID mapID = new MapPartitionID(CommonUtils.randomBytes(16));
        readDatas = new ArrayList<>();
        clientBufferPool = new TestTransferBufferPool(NUM_BUFFERS, BUFFER_SIZE);
        connManager = ConnectionManager.createReadConnectionManager(nettyConfig, false);
        connManager.start();
        address = new InetSocketAddress(InetAddress.getLocalHost(), dataPort);
        client =
                new ShuffleReadClient(
                        address,
                        dataSetID,
                        mapID,
                        0,
                        0,
                        emptyBufferSize(),
                        clientBufferPool,
                        connManager,
                        readDatas::add,
                        cause::set);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        nettyServer.shutdown();
        connManager.shutdown();
        assertEquals(20, clientBufferPool.numBuffers());
        clientBufferPool.destroy();
    }

    /** Basic routine. */
    @Test
    public void testReadDataAndSendCredit() throws Exception {
        assertTrue(serverHandler.isEmpty());

        // Client sends ReadHandshakeRequest.
        client.connect();
        client.open();
        checkUntil(() -> assertEquals(1, serverHandler.numMessages()));
        assertTrue(serverHandler.getLastMsg() instanceof ReadHandshakeRequest);
        assertEquals(0, ((ReadHandshakeRequest) serverHandler.getLastMsg()).getInitialCredit());

        // Construct 25 buffers for sending.
        Queue<ByteBuf> serverBuffers = constructBuffers(25, 3);
        client.backlogReceived(25);
        checkUntil(() -> assertEquals(2, serverHandler.numMessages()));
        assertEquals(20, ((ReadAddCredit) serverHandler.getMsg(1)).getCredit());

        // Server sends ReadData.
        for (int i = 0; i < 20; i++) {
            ByteBuf buffer = serverBuffers.poll();
            serverHandler.send(
                    new ReadData(
                            currentProtocolVersion(),
                            client.getChannelID(),
                            1,
                            buffer.readableBytes(),
                            emptyOffset(),
                            buffer,
                            emptyExtraMessage()));
        }
        checkUntil(() -> assertEquals(20, readDatas.size()));
        readDatas.forEach(ReferenceCounted::release);
        readDatas.clear();

        checkUntil(() -> assertEquals(5, serverHandler.numMessages()));
        assertEquals(2, ((ReadAddCredit) serverHandler.getMsg(2)).getCredit());
        assertEquals(2, ((ReadAddCredit) serverHandler.getMsg(3)).getCredit());
        assertEquals(1, ((ReadAddCredit) serverHandler.getMsg(4)).getCredit());

        for (int i = 0; i < 5; i++) {
            ByteBuf buffer = serverBuffers.poll();
            serverHandler.send(
                    new ReadData(
                            currentProtocolVersion(),
                            client.getChannelID(),
                            1,
                            buffer.readableBytes(),
                            emptyOffset(),
                            buffer,
                            emptyExtraMessage()));
        }
        checkUntil(() -> assertEquals(5, readDatas.size()));
        while (!readDatas.isEmpty()) {
            readDatas.remove(0).release();
        }
        delayCheck(() -> assertEquals(5, serverHandler.numMessages()));

        client.close();
        checkUntil(() -> assertEquals(7, serverHandler.numMessages()));
        assertTrue(serverHandler.getMsg(5) instanceof CloseChannel);
        assertTrue(serverHandler.getMsg(6) instanceof CloseConnection);
    }

    /** Release all buffers when close. */
    @Test
    public void testReleaseAllBuffersWhenClose() throws Exception {
        int prevAvailableBuffers = transferBufferPool.numBuffers();

        // Client sends ReadHandshakeRequest.
        client.connect();
        client.open();
        checkUntil(() -> serverHandler.isConnected());

        // Close client.
        client.close();
        assertEquals(prevAvailableBuffers, transferBufferPool.numBuffers());
    }

    /** Close connection from server. */
    @Test
    public void testCloseConnectionFromServer() throws Exception {
        // Client sends ReadHandshakeRequest.
        client.connect();
        client.open();
        checkUntil(() -> assertTrue(serverHandler.isConnected()));

        serverHandler.close();
        checkUntil(() -> assertTrue(cause.get() instanceof IOException));
        client.close();
    }

    /** ErrorResponse from server. */
    @Test
    public void testErrorResponseFromServer() throws Exception {
        // Client sends ReadHandshakeRequest.
        client.connect();
        client.open();
        checkUntil(() -> assertTrue(serverHandler.isConnected()));

        // Server sends ErrorResponse.
        String errMsg = "Expected exception.";
        serverHandler.send(
                new ErrorResponse(
                        currentProtocolVersion(),
                        client.getChannelID(),
                        errMsg.getBytes(),
                        emptyExtraMessage()));
        checkUntil(
                () -> {
                    assertTrue(cause.get().getCause() instanceof IOException);
                    assertEquals(errMsg, ((IOException) cause.get().getCause()).getMessage());
                });
        client.close();
    }

    // Feed data after close.
    @Test
    public void testFeedDataAfterClose() throws Exception {
        client.connect();
        client.open();
        checkUntil(() -> assertTrue(serverHandler.isConnected()));

        client.close();

        ByteBuf byteBuf = constructBuffers(1, 3).poll();
        client.dataReceived(
                new ReadData(
                        currentProtocolVersion(),
                        client.getChannelID(),
                        0,
                        byteBuf.readableBytes(),
                        emptyOffset(),
                        byteBuf,
                        emptyExtraMessage()));
        assertEquals(0, byteBuf.refCnt());

        // Test close multiple times.
        client.close();
    }

    // Avoid deadlock caused by the competition between ReadClient upper layer lock and the limited
    // Netty thread resource.
    @Test
    public void testDeadlockAvoidance() throws Exception {
        DataSetID dataSetID = new DataSetID(CommonUtils.randomBytes(32));
        Object lock = new Object();
        nettyConfig.getConfig().setInteger(TransferOptions.NUM_THREADS_CLIENT, 1);
        ConnectionManager connManager =
                ConnectionManager.createReadConnectionManager(nettyConfig, false);
        connManager.start();

        // Prepare client0
        MapPartitionID mapID0 = new MapPartitionID(CommonUtils.randomBytes(16));
        int dataPort0 = getAvailablePort();
        Pair<NettyServer, DummyChannelInboundHandlerAdaptor> pair0 = initShuffleServer(dataPort0);
        NettyServer nettyServer0 = pair0.getLeft();
        DummyChannelInboundHandlerAdaptor serverH0 = pair0.getRight();
        InetSocketAddress address0 = new InetSocketAddress(InetAddress.getLocalHost(), dataPort0);
        List<ByteBuf> readData0 = new ArrayList<>();
        Consumer<ByteBuf> dataListener =
                byteBuf -> {
                    synchronized (lock) {
                        readData0.add(byteBuf);
                    }
                };
        TransferBufferPool clientBufferPool0 = new TestTransferBufferPool(NUM_BUFFERS, BUFFER_SIZE);
        ShuffleReadClient shuffleReadClient0 =
                new ShuffleReadClient(
                        address0,
                        dataSetID,
                        mapID0,
                        0,
                        0,
                        emptyBufferSize(),
                        clientBufferPool0,
                        connManager,
                        dataListener,
                        ignore -> {});
        shuffleReadClient0.connect();
        shuffleReadClient0.open();
        checkUntil(() -> assertTrue(serverH0.isConnected()));

        // Prepare client1
        MapPartitionID mapID1 = new MapPartitionID(CommonUtils.randomBytes(16));
        int dataPort1 = getAvailablePort();
        Pair<NettyServer, DummyChannelInboundHandlerAdaptor> pair1 = initShuffleServer(dataPort1);
        NettyServer nettyServer1 = pair1.getLeft();
        InetSocketAddress address1 = new InetSocketAddress(InetAddress.getLocalHost(), dataPort1);
        TransferBufferPool clientBufferPool1 = new TestTransferBufferPool(NUM_BUFFERS, BUFFER_SIZE);
        ShuffleReadClient shuffleReadClient1 =
                new ShuffleReadClient(
                        address1,
                        dataSetID,
                        mapID1,
                        0,
                        0,
                        emptyBufferSize(),
                        clientBufferPool1,
                        connManager,
                        ignore -> {},
                        ignore -> {});
        shuffleReadClient1.connect();

        // Testing procedure.
        synchronized (lock) {
            Thread t =
                    new Thread(
                            () -> {
                                ChannelID channelID0 = shuffleReadClient0.getChannelID();
                                Queue<ByteBuf> serverBuffers = constructBuffers(1, 3);
                                ByteBuf byteBuf = serverBuffers.poll();
                                serverH0.send(
                                        new ReadData(
                                                currentProtocolVersion(),
                                                channelID0,
                                                0,
                                                byteBuf.readableBytes(),
                                                emptyOffset(),
                                                byteBuf,
                                                emptyExtraMessage()));
                            });
            t.start();
            while (getAllThreads(NettyConfig.CLIENT_THREAD_GROUP_NAME, Thread.State.BLOCKED)
                    .isEmpty()) {
                Thread.sleep(1000);
            }
            shuffleReadClient1.open();
        }

        readData0.forEach(ReferenceCounted::release);

        shuffleReadClient0.close();
        shuffleReadClient1.close();
        nettyServer0.shutdown();
        nettyServer1.shutdown();
        clientBufferPool0.destroy();
        clientBufferPool1.destroy();
    }

    private Pair<NettyServer, DummyChannelInboundHandlerAdaptor> initShuffleServer(int dataPort)
            throws Exception {
        DummyChannelInboundHandlerAdaptor serverH = new DummyChannelInboundHandlerAdaptor();
        nettyConfig.getConfig().setInteger(TransferOptions.SERVER_DATA_PORT, dataPort);
        NettyServer nettyServer =
                new NettyServer(null, nettyConfig) {
                    @Override
                    public ChannelHandler[] getServerHandlers() {
                        return new ChannelHandler[] {
                            new TransferMessageEncoder(),
                            DecoderDelegate.serverDecoderDelegate(null),
                            serverH
                        };
                    }
                };
        nettyServer.start();
        return Pair.of(nettyServer, serverH);
    }

    private List<Thread> getAllThreads(String namePrefix, Thread.State state) {
        ThreadGroup group = Thread.currentThread().getThreadGroup();
        ThreadGroup topGroup = group;
        while (group != null) {
            topGroup = group;
            group = group.getParent();
        }
        int estimatedSize = topGroup.activeCount() * 2;
        Thread[] slackList = new Thread[estimatedSize];
        int actualSize = topGroup.enumerate(slackList);
        Thread[] list = new Thread[actualSize];
        System.arraycopy(slackList, 0, list, 0, actualSize);

        List<Thread> ret = new ArrayList<>();
        for (Thread t : list) {
            if (t.getName().startsWith(namePrefix) && t.getState() == state) {
                ret.add(t);
            }
        }
        return ret;
    }
}
