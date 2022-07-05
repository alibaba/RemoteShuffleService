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
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.transfer.TransferMessage.ReadHandshakeRequest;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import static com.alibaba.flink.shuffle.common.utils.ProtocolUtils.currentProtocolVersion;
import static com.alibaba.flink.shuffle.common.utils.ProtocolUtils.emptyBufferSize;
import static com.alibaba.flink.shuffle.common.utils.ProtocolUtils.emptyExtraMessage;
import static com.alibaba.flink.shuffle.common.utils.ProtocolUtils.emptyOffset;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** Test for {@link DecoderDelegate}. */
public class DecoderDelegateTest extends AbstractNettyTest {

    private NettyServer nettyServer;

    private DummyChannelInboundHandlerAdaptor serverH;

    private MapPartitionID mapID;

    private DecoderDelegate decoderDelegate;

    private FakeDecoder decoder;

    private NettyClient nettyClient;

    private Channel channel;

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();

        mapID = new MapPartitionID(CommonUtils.randomBytes(32));

        int dataPort = initShuffleServer();
        address = new InetSocketAddress(InetAddress.getLocalHost(), dataPort);

        nettyClient = new NettyClient(nettyConfig);
        nettyClient.init(() -> new ChannelHandler[] {new TransferMessageEncoder()});
        channel = nettyClient.connect(address).await().channel();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        nettyServer.shutdown();
        channel.close();
        nettyClient.shutdown();
    }

    @Test
    public void testDecoderException() throws Exception {
        // Send ReadHandshakeRequest -- FakeDecoder throws when processing message.
        channel.writeAndFlush(
                new ReadHandshakeRequest(
                        currentProtocolVersion(),
                        new ChannelID(),
                        dataSetID,
                        mapID,
                        0,
                        0,
                        1,
                        emptyBufferSize(),
                        emptyOffset(),
                        emptyExtraMessage()));
        checkUntil(() -> assertTrue(decoder.isClosed));
    }

    @Test
    public void testCloseConnection() throws Exception {
        checkUntil(() -> assertNotNull(decoderDelegate));
        decoder = new FakeDecoder();
        decoderDelegate.setCurrentDecoder(decoder);
        channel.close();
        checkUntil(() -> assertTrue(decoder.isClosed));
    }

    private int initShuffleServer() throws Exception {
        serverH = new DummyChannelInboundHandlerAdaptor();
        int dataPort = getAvailablePort();
        nettyConfig.getConfig().setInteger(TransferOptions.SERVER_DATA_PORT, dataPort);
        nettyServer =
                new NettyServer(null, nettyConfig) {
                    @Override
                    public ChannelHandler[] getServerHandlers() {
                        decoder = new FakeDecoder();
                        decoderDelegate = new DecoderDelegate(ignore -> decoder);
                        return new ChannelHandler[] {decoderDelegate, serverH};
                    }
                };
        nettyServer.start();
        return dataPort;
    }

    private static class FakeDecoder extends TransferMessageDecoder {

        volatile boolean isClosed = false;

        @Override
        public DecodingResult onChannelRead(ByteBuf data) throws Exception {
            throw new Exception("Expected exception.");
        }

        @Override
        public void close() {
            isClosed = true;
        }
    }
}
