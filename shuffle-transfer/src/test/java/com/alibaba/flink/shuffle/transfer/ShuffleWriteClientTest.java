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
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.transfer.TransferMessage.CloseChannel;
import com.alibaba.flink.shuffle.transfer.TransferMessage.CloseConnection;
import com.alibaba.flink.shuffle.transfer.TransferMessage.ErrorResponse;
import com.alibaba.flink.shuffle.transfer.TransferMessage.WriteAddCredit;
import com.alibaba.flink.shuffle.transfer.TransferMessage.WriteData;
import com.alibaba.flink.shuffle.transfer.TransferMessage.WriteFinish;
import com.alibaba.flink.shuffle.transfer.TransferMessage.WriteFinishCommit;
import com.alibaba.flink.shuffle.transfer.TransferMessage.WriteHandshakeRequest;
import com.alibaba.flink.shuffle.transfer.TransferMessage.WriteRegionFinish;
import com.alibaba.flink.shuffle.transfer.TransferMessage.WriteRegionStart;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import static com.alibaba.flink.shuffle.common.utils.ProtocolUtils.currentProtocolVersion;
import static com.alibaba.flink.shuffle.common.utils.ProtocolUtils.emptyBufferSize;
import static com.alibaba.flink.shuffle.common.utils.ProtocolUtils.emptyDataPartitionType;
import static com.alibaba.flink.shuffle.common.utils.ProtocolUtils.emptyExtraMessage;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** Test for {@link ShuffleWriteClient}. */
public class ShuffleWriteClientTest extends AbstractNettyTest {

    private NettyServer nettyServer;

    private ConnectionManager connManager;

    private ShuffleWriteClient client0;

    private ShuffleWriteClient client1;

    private volatile DummyChannelInboundHandlerAdaptor serverH;

    private final CreditListener creditListener = new TestCreditListener();

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
        int dataPort = initShuffleServer();
        MapPartitionID mapID0 = new MapPartitionID(CommonUtils.randomBytes(16));
        MapPartitionID mapID1 = new MapPartitionID(CommonUtils.randomBytes(16));
        int subsNum = 2;
        connManager = ConnectionManager.createWriteConnectionManager(nettyConfig, false);
        connManager.start();
        address = new InetSocketAddress(InetAddress.getLocalHost(), dataPort);
        client0 =
                new ShuffleWriteClient(
                        address,
                        jobID,
                        dataSetID,
                        mapID0,
                        subsNum,
                        emptyBufferSize(),
                        emptyDataPartitionType(),
                        connManager);
        client1 =
                new ShuffleWriteClient(
                        address,
                        jobID,
                        dataSetID,
                        mapID1,
                        subsNum,
                        emptyBufferSize(),
                        emptyDataPartitionType(),
                        connManager);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        connManager.shutdown();
        serverH.close();
        nettyServer.shutdown();
        super.tearDown();
    }

    /** Basic routine. */
    @Test
    public void testWriteDataAndSendCredit() throws Exception {
        Queue<ByteBuf> buffersToSend = constructBuffers(4, 3);
        int subIdx = 0;

        checkUntil(() -> assertTrue(serverH.isEmpty()));

        // Client send WriteHandshakeRequest.
        runAsync(() -> client0.open());
        checkUntil(() -> assertEquals(1, serverH.numMessages()));
        assertTrue(serverH.isConnected());
        assertTrue(serverH.getLastMsg() instanceof WriteHandshakeRequest);

        // Client sends WriteRegionStart;
        runAsync(() -> client0.regionStart(false));
        checkUntil(() -> assertEquals(2, serverH.numMessages()));
        assertTrue(serverH.getLastMsg() instanceof WriteRegionStart);

        // Client sends WriteData.
        runAsync(() -> client0.write(buffersToSend.poll(), subIdx));
        checkUntil(() -> assertTrue(client0.isWaitingForCredit()));

        // Server sends 1 credit.
        serverH.send(
                new WriteAddCredit(
                        currentProtocolVersion(),
                        client0.getChannelID(),
                        1,
                        0,
                        emptyExtraMessage()));
        checkUntil(() -> assertEquals(3, serverH.numMessages()));
        checkUntil(() -> assertTrue(serverH.getLastMsg() instanceof WriteData));

        // Client sends a WriteData.
        runAsync(() -> client0.write(buffersToSend.poll(), subIdx));
        checkUntil(() -> assertTrue(client0.isWaitingForCredit()));

        // Server sends 1 credit.
        // Client sends WriteData and WriteRegionFinish.
        serverH.send(
                new WriteAddCredit(
                        currentProtocolVersion(),
                        client0.getChannelID(),
                        1,
                        0,
                        emptyExtraMessage()));
        runAsync(() -> client0.regionFinish());
        checkUntil(() -> assertEquals(5, serverH.numMessages()));
        assertTrue(serverH.getMsg(3) instanceof WriteData);
        assertTrue(serverH.getMsg(4) instanceof WriteRegionFinish);

        // Server sends outdated credits
        serverH.send(
                new WriteAddCredit(
                        currentProtocolVersion(),
                        client0.getChannelID(),
                        1,
                        0,
                        emptyExtraMessage()));
        serverH.send(
                new WriteAddCredit(
                        currentProtocolVersion(),
                        client0.getChannelID(),
                        1,
                        0,
                        emptyExtraMessage()));
        runAsync(
                () -> {
                    client0.write(buffersToSend.poll(), subIdx);
                    client0.write(buffersToSend.poll(), subIdx);
                    client0.regionFinish();
                    client0.finish();
                });
        checkUntil(() -> assertTrue(client0.isWaitingForCredit()));

        // Server sends 2 credits.
        // Client sends WriteData and WriteRegionFinish and WriteFinish.
        serverH.send(
                new WriteAddCredit(
                        currentProtocolVersion(),
                        client0.getChannelID(),
                        2,
                        1,
                        emptyExtraMessage()));

        checkUntil(() -> assertEquals(9, serverH.numMessages()));
        assertTrue(serverH.getMsg(5) instanceof WriteData);
        assertTrue(serverH.getMsg(6) instanceof WriteData);
        assertTrue(serverH.getMsg(7) instanceof WriteRegionFinish);
        assertTrue(serverH.getMsg(8) instanceof WriteFinish);
        assertTrue(client0.isWaitingForFinishCommit());

        // Server sends WriteFinishCommit.
        serverH.send(
                new WriteFinishCommit(
                        currentProtocolVersion(), client0.getChannelID(), emptyExtraMessage()));
        checkUntil(() -> assertFalse(client0.isWaitingForFinishCommit()));

        client0.close();
        checkUntil(() -> assertEquals(11, serverH.numMessages()));
        assertTrue(serverH.getMsg(9) instanceof CloseChannel);
        assertTrue(serverH.getMsg(10) instanceof CloseConnection);

        // verify buffers
        List<ByteBuf> receivedBuffers = new ArrayList<>();
        serverH.getMessages().stream()
                .filter(o -> o instanceof WriteData)
                .forEach(obj -> receivedBuffers.add(((WriteData) obj).getBuffer()));
        verifyBuffers(4, 3, receivedBuffers);
    }

    /** Client receives a {@link Throwable} when broken connection. */
    @Test
    public void testClientReceiveThrowableWhenBrokenConnection() throws Exception {
        // Client sends WriteHandshakeRequest.
        runAsync(() -> client0.open());
        checkUntil(() -> assertTrue(serverH.isConnected()));

        // Server closes connection.
        serverH.close();
        checkUntil(() -> assertTrue(client0.getCause() instanceof IOException));

        client0.close();
    }

    /** Client gets notified lock when broken connection. */
    @Test
    public void testClientGetsNotifiedLockWhenBrokenConnection() throws Exception {
        Queue<ByteBuf> buffersToSend = constructBuffers(1, 3);
        int subIdx = 0;

        // Client send WriteHandshakeRequest.
        runAsync(() -> client0.open());
        checkUntil(() -> assertTrue(serverH.isConnected()));

        // Client sends WriteRegionStart.
        runAsync(() -> client0.regionStart(false));

        // Client sends WriteData.
        ByteBuf byteBuf = buffersToSend.poll();
        runAsync(() -> client0.write(byteBuf, subIdx));
        checkUntil(() -> assertTrue(client0.isWaitingForCredit()));

        // Server closes connection.
        serverH.close();
        checkUntil(() -> assertTrue(!client0.isWaitingForCredit()));

        client0.close();
    }

    /** ErrorResponse from server. */
    @Test
    public void testReceiveErrorResponseFromServer() throws Exception {
        // Client sends WriteHandshakeRequest.
        runAsync(() -> client0.open());
        checkUntil(() -> assertTrue(serverH.isConnected()));

        // ErrorResponse from server.
        String errMsg = "Expected exception.";
        serverH.send(
                new ErrorResponse(
                        currentProtocolVersion(),
                        client0.getChannelID(),
                        errMsg.getBytes(),
                        emptyExtraMessage()));
        checkUntil(
                () -> {
                    assertTrue(client0.getCause() instanceof IOException);
                    assertTrue(client0.getCause().getCause().getMessage().contains(errMsg));
                });

        Queue<ByteBuf> buffersToSend = constructBuffers(1, 3);
        assertThrows(Exception.class, () -> client0.write(buffersToSend.poll(), 0));

        client0.close();
    }

    /** Multiple channels shared the same physical connection. */
    @Test
    public void testMultipleChannelsSharedSamePhysicalConnection() throws Exception {
        runAsync(() -> client0.open());
        runAsync(() -> client1.open());
        checkUntil(() -> assertEquals(2, serverH.numMessages()));

        Channel channel0 = connManager.getChannel(client0.getChannelID(), address);
        Channel channel1 = connManager.getChannel(client1.getChannelID(), address);
        WriteClientHandler clientHandler = channel0.pipeline().get(WriteClientHandler.class);
        assertEquals(channel0, channel1);

        serverH.send(
                new WriteAddCredit(
                        currentProtocolVersion(),
                        client0.getChannelID(),
                        2,
                        0,
                        emptyExtraMessage()));
        serverH.send(
                new WriteAddCredit(
                        currentProtocolVersion(),
                        client1.getChannelID(),
                        2,
                        0,
                        emptyExtraMessage()));
        Queue<ByteBuf> buffersToSend = constructBuffers(4, 3);
        runAsync(() -> client0.write(buffersToSend.poll(), 0));
        runAsync(() -> client1.write(buffersToSend.poll(), 0));
        checkUntil(() -> assertEquals(4, serverH.numMessages()));
        assertTrue(serverH.getMsg(0) instanceof WriteHandshakeRequest);
        assertTrue(serverH.getMsg(1) instanceof WriteHandshakeRequest);
        assertTrue(serverH.getMsg(2) instanceof WriteData);
        assertTrue(serverH.getMsg(3) instanceof WriteData);

        runAsync(() -> client0.write(buffersToSend.poll(), 1));
        checkUntil(() -> assertEquals(5, serverH.numMessages()));
        assertTrue(serverH.getMsg(4) instanceof WriteData);

        client0.close();
        assertFalse(clientHandler.isRegistered(client0.getChannelID()));
        assertTrue(channel1.isActive());
        assertTrue(clientHandler.isRegistered(client1.getChannelID()));
        checkUntil(() -> assertEquals(6, serverH.numMessages()));
        assertTrue(serverH.getMsg(5) instanceof CloseChannel);

        runAsync(() -> client1.write(buffersToSend.poll(), 1));
        checkUntil(() -> assertEquals(7, serverH.numMessages()));
        assertTrue(serverH.getMsg(6) instanceof WriteData);

        client1.close();
        checkUntil(() -> assertEquals(9, serverH.numMessages()));
        assertTrue(serverH.getMsg(7) instanceof CloseChannel);
        assertTrue(serverH.getMsg(8) instanceof CloseConnection);
        checkUntil(() -> assertFalse(channel1.isActive()));
        assertFalse(clientHandler.isRegistered(client1.getChannelID()));
    }

    private int initShuffleServer() throws Exception {
        serverH = new DummyChannelInboundHandlerAdaptor();
        int dataPort = getAvailablePort();
        nettyConfig.getConfig().setInteger(TransferOptions.SERVER_DATA_PORT, dataPort);
        nettyServer =
                new NettyServer(null, nettyConfig) {
                    @Override
                    public ChannelHandler[] getServerHandlers() {
                        return new ChannelHandler[] {
                            new TransferMessageEncoder(),
                            DecoderDelegate.serverDecoderDelegate(ignore -> () -> requestBuffer()),
                            serverH
                        };
                    }
                };
        nettyServer.start();
        return dataPort;
    }
}
