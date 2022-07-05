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

import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;

import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Test for {@link ConnectionManager}. */
public class ConnectionManagerTest {

    private final int maxRetries = 10;

    private final Duration connectionRetryWait = Duration.ofMillis(100);

    @Test
    public void testConnectionRetry() throws Exception {
        for (int numFailsFirstConnects : new int[] {0, 1, maxRetries, maxRetries + 1}) {
            ConnectionManager connMgr = createConnectionManager(numFailsFirstConnects);
            connMgr.start();
            Channel ch = null;
            try {
                InetSocketAddress address = new InetSocketAddress(InetAddress.getLocalHost(), 0);
                ch = connMgr.getChannel(new ChannelID(), address);
            } catch (Exception ignored) {
            }
            if (numFailsFirstConnects < maxRetries) {
                assertNotNull(ch);
            } else {
                assertNull(ch);
            }
            connMgr.shutdown();
        }
    }

    @Test
    public void testReuse() throws Exception {
        InetSocketAddress address0 = new InetSocketAddress(InetAddress.getLocalHost(), 0);
        InetSocketAddress address1 = new InetSocketAddress(InetAddress.getLocalHost(), 1);

        ConnectionManager connMgr = createConnectionManager(0);
        connMgr.start();

        ChannelID channelID00 = new ChannelID();
        Channel ch00 = connMgr.getChannel(channelID00, address0);
        assertNotNull(ch00);
        assertEquals(1, connMgr.numPhysicalConnections());

        ChannelID channelID01 = new ChannelID();
        Channel ch01 = connMgr.getChannel(channelID01, address0);
        assertNotNull(ch01);
        assertEquals(1, connMgr.numPhysicalConnections());
        assertEquals(ch00, ch01);

        ChannelID channelID10 = new ChannelID();
        Channel ch10 = connMgr.getChannel(channelID10, address1);
        assertNotNull(ch10);
        assertEquals(2, connMgr.numPhysicalConnections());
        assertNotEquals(ch00, ch10);

        connMgr.releaseChannel(address0, channelID01);
        assertEquals(2, connMgr.numPhysicalConnections());

        connMgr.releaseChannel(address0, channelID00);
        assertEquals(1, connMgr.numPhysicalConnections());

        connMgr.releaseChannel(address1, channelID10);
        assertEquals(0, connMgr.numPhysicalConnections());

        connMgr.shutdown();
    }

    @Test
    public void testReconnectDelay() throws Exception {
        long startTime = System.nanoTime();
        int numFailsFirstConnects = 2;
        ConnectionManager connMgr = createConnectionManager(numFailsFirstConnects);
        connMgr.start();
        InetSocketAddress address = new InetSocketAddress(InetAddress.getLocalHost(), 0);
        connMgr.getChannel(new ChannelID(), address);
        connMgr.shutdown();
        long duration = System.nanoTime() - startTime;
        long delay = connectionRetryWait.toNanos() * numFailsFirstConnects;
        String msg = String.format("Retry duration (%d) < delay (%d)", duration, delay);
        assertTrue(msg, duration >= delay);
    }

    @Test
    public void testMultipleConcurrentConnect() throws Exception {
        ConnectionManager connMgr = createConnectionManager(0);
        connMgr.start();
        List<InetSocketAddress> addrs = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            addrs.add(new InetSocketAddress(InetAddress.getLocalHost(), i));
        }
        List<Thread> threads = new ArrayList<>();
        List<ChannelID> channelIDs = new ArrayList<>();
        AtomicReference<Throwable> cause = new AtomicReference<>(null);
        for (int i = 0; i < 24; i++) {
            final int idx = i % 4;
            ChannelID channelID = new ChannelID();
            channelIDs.add(channelID);
            Runnable r =
                    () -> {
                        try {
                            assertNotNull(connMgr.getChannel(channelID, addrs.get(idx)));
                        } catch (Throwable t) {
                            cause.set(t);
                        }
                    };
            threads.add(new Thread(r));
        }
        for (Thread t : threads) {
            t.start();
        }
        for (Thread t : threads) {
            t.join();
        }
        assertNull(cause.get());
        assertEquals(4, connMgr.numPhysicalConnections());
        for (int i = 0; i < 24; i++) {
            connMgr.releaseChannel(addrs.get(i % 4), channelIDs.get(i));
        }
        assertEquals(0, connMgr.numPhysicalConnections());
    }

    private ConnectionManager createConnectionManager(int numFailsFirstConnects) {
        NettyClient mockedNettyClient = mock(NettyClient.class);
        AtomicInteger failedTimes = new AtomicInteger(0);
        when(mockedNettyClient.connect(any(InetSocketAddress.class)))
                .thenAnswer(
                        invoke -> {
                            Channel ch = mock(Channel.class);
                            when(ch.writeAndFlush(any(Object.class)))
                                    .thenReturn(mock(ChannelFuture.class));
                            return mockChannelFuture(ch, failedTimes, numFailsFirstConnects);
                        });
        return new ConnectionManager(null, null, maxRetries, connectionRetryWait) {
            @Override
            public synchronized void start() throws IOException {
                this.nettyClient = mockedNettyClient;
            }
        };
    }

    private ChannelFuture mockChannelFuture(
            Channel channel, AtomicInteger failedTimes, int numFailsFirstConnects)
            throws Exception {
        ChannelFuture channelFuture = mock(ChannelFuture.class);
        when(channelFuture.sync())
                .thenAnswer(
                        invoke -> {
                            if (failedTimes.get() < numFailsFirstConnects) {
                                failedTimes.incrementAndGet();
                                throw new Exception("Connection failure.");
                            } else {
                                return channelFuture;
                            }
                        });
        when(channelFuture.channel()).thenReturn(channel);
        return channelFuture;
    }
}
