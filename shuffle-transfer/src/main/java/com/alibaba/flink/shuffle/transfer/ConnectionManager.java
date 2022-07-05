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
import com.alibaba.flink.shuffle.transfer.TransferMessage.CloseChannel;
import com.alibaba.flink.shuffle.transfer.TransferMessage.CloseConnection;

import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.handler.timeout.IdleStateHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static com.alibaba.flink.shuffle.common.utils.ProtocolUtils.currentProtocolVersion;
import static com.alibaba.flink.shuffle.common.utils.ProtocolUtils.emptyExtraMessage;
import static org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener.CLOSE_ON_FAILURE;

/** Connection manager manages physical connections for remote input channels at runtime. */
public class ConnectionManager {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectionManager.class);

    /** The number of times to retry when connection failure. */
    private final int connectionRetries;

    /** Time to wait between two consecutive connection retries. */
    private final Duration connectionRetryWait;

    /** Netty configuration. */
    private final NettyConfig nettyConfig;

    /** Netty client to create connections to remote. */
    protected volatile NettyClient nettyClient;

    /** {@link Channel}s by remote shuffle address. */
    private final Map<InetSocketAddress, CompletableFuture<PhysicalChannel>> channelsByAddress =
            new ConcurrentHashMap<>();

    /** {@link ChannelHandler}s when create connections. */
    private final Supplier<ChannelHandler[]> channelHandlersSupplier;

    /** If the manager is shutdown. */
    private volatile boolean isShutdown;

    public ConnectionManager(
            NettyConfig nettyConfig,
            Supplier<ChannelHandler[]> channelHandlersSupplier,
            int connectionRetries,
            Duration connectionRetryWait) {
        this.nettyConfig = nettyConfig;
        this.channelHandlersSupplier = channelHandlersSupplier;
        this.connectionRetries = connectionRetries;
        this.connectionRetryWait = connectionRetryWait;
    }

    /** Start internal related components for network connection. */
    public void start() throws IOException {
        if (nettyClient != null) {
            return;
        }
        nettyClient = new NettyClient(nettyConfig);
        nettyClient.init(channelHandlersSupplier);
    }

    /** Shutdown internal related components for network connection. */
    public void shutdown() {
        if (isShutdown) {
            return;
        }
        nettyClient.shutdown();
        isShutdown = true;
    }

    /**
     * Ask for a physical connection. It will return from exists or create a new connection.
     *
     * @param channelID {@link ChannelID} related with the connection.
     * @param address {@link InetSocketAddress} of the connection.
     */
    public Channel getChannel(ChannelID channelID, InetSocketAddress address)
            throws IOException, InterruptedException {
        Channel ret = null;
        while ((ret = getOrCreateChannel(channelID, address)) == null) {
            continue;
        }
        return ret;
    }

    private Channel getOrCreateChannel(ChannelID channelID, InetSocketAddress addr)
            throws IOException, InterruptedException {
        CompletableFuture<PhysicalChannel> newFuture = new CompletableFuture<>();
        CompletableFuture<PhysicalChannel> oldFuture =
                channelsByAddress.putIfAbsent(addr, newFuture);

        if (oldFuture == null) {
            try {
                Channel channel = createChannel(addr);
                PhysicalChannel physicalChannel = new PhysicalChannel(channel);
                physicalChannel.register(channelID);
                newFuture.complete(physicalChannel);
                return channel;
            } catch (Throwable t) {
                newFuture.completeExceptionally(new IOException("Cannot create connection.", t));
                channelsByAddress.remove(addr);
                throw t;
            }
        } else {
            try {
                PhysicalChannel physicalChannel = oldFuture.get();
                if (!physicalChannel.register(channelID)) {
                    return null;
                } else {
                    return physicalChannel.nettyChannel;
                }
            } catch (ExecutionException t) {
                throw new IOException("Cannot get a channel.", t);
            }
        }
    }

    /**
     * Release a reference of a physical connection. A connection is closed when no reference on it.
     *
     * @param address {@link InetSocketAddress} of the connection.
     * @param channelID {@link ChannelID} related with the connection.
     */
    public void releaseChannel(InetSocketAddress address, ChannelID channelID) throws IOException {
        try {
            CompletableFuture<PhysicalChannel> future = channelsByAddress.get(address);
            if (future == null) {
                return;
            }

            PhysicalChannel pChannel = future.get();
            if (!pChannel.isRegistered(channelID)) {
                return;
            }

            CloseChannel closeChannel =
                    new CloseChannel(currentProtocolVersion(), channelID, emptyExtraMessage());
            LOG.debug("(remote: {}) Send {}.", pChannel.nettyChannel.remoteAddress(), closeChannel);
            pChannel.nettyChannel.writeAndFlush(closeChannel).addListener(CLOSE_ON_FAILURE);
            pChannel.unRegister(address, channelID);
        } catch (Throwable t) {
            throw new IOException("Failed to release channel.", t);
        }
    }

    private void closeConnection(InetSocketAddress address, Channel channel) {
        LOG.debug("Close connection to {}.", address);
        channel.writeAndFlush(new CloseConnection()).addListener(ChannelFutureListener.CLOSE);
        channelsByAddress.remove(address);
    }

    /** Number of physical connections. */
    public int numPhysicalConnections() {
        return channelsByAddress.size();
    }

    private Channel createChannel(InetSocketAddress address) throws InterruptedException {
        LOG.debug("Create connection to {}.", address);

        long retryWait = Math.max(1, connectionRetryWait.toMillis());
        for (int i = 0; i < connectionRetries; i++) {
            try {
                return nettyClient.connect(address).sync().channel();
            } catch (InterruptedException e) {
                throw e;
            } catch (Throwable throwable) {
                LOG.warn(
                        "(remote: {}) Fire connection failed {} time(s).",
                        address,
                        i + 1,
                        throwable);
                if (i + 1 >= connectionRetries) {
                    throw throwable;
                }
                // Sleep for a period of connect timeout, thus remote can have some time to recover.
                Thread.sleep(retryWait);
            }
        }
        throw new IllegalStateException("Cannot arrive here.");
    }

    private class PhysicalChannel {
        final Channel nettyChannel;
        final Set<ChannelID> channelIDs = new HashSet<>();
        boolean isReleased;

        PhysicalChannel(Channel nettyChannel) {
            this.nettyChannel = nettyChannel;
        }

        synchronized boolean register(ChannelID channelID) {
            if (isReleased) {
                return false;
            }
            channelIDs.add(channelID);
            return true;
        }

        synchronized boolean isRegistered(ChannelID channelID) {
            return channelIDs.contains(channelID);
        }

        synchronized void unRegister(InetSocketAddress address, ChannelID channelID) {
            channelIDs.remove(channelID);
            if (channelIDs.isEmpty()) {
                closeConnection(address, nettyChannel);
                isReleased = true;
            }
        }
    }

    /** Create {@link ConnectionManager} for write-client. */
    public static ConnectionManager createWriteConnectionManager(
            NettyConfig nettyConfig, boolean enableHeartbeat) {
        return new ConnectionManager(
                nettyConfig,
                () ->
                        new ChannelHandler[] {
                            new TransferMessageEncoder(),
                            DecoderDelegate.writeClientDecoderDelegate(),
                            new IdleStateHandler(
                                    nettyConfig.getHeartbeatTimeoutSeconds(),
                                    0,
                                    0,
                                    TimeUnit.SECONDS),
                            new WriteClientHandler(
                                    enableHeartbeat
                                            ? nettyConfig.getHeartbeatIntervalSeconds()
                                            : -1)
                        },
                nettyConfig.getConnectionRetries(),
                nettyConfig.getConnectionRetryWait());
    }

    /** Create {@link ConnectionManager} for read-client. */
    public static ConnectionManager createReadConnectionManager(
            NettyConfig nettyConfig, boolean enableHeartbeat) {
        return new ConnectionManager(
                nettyConfig,
                () -> {
                    ReadClientHandler handler =
                            new ReadClientHandler(
                                    enableHeartbeat
                                            ? nettyConfig.getHeartbeatIntervalSeconds()
                                            : -1);
                    return new ChannelHandler[] {
                        new TransferMessageEncoder(),
                        DecoderDelegate.readClientDecoderDelegate(handler.bufferSuppliers()),
                        new IdleStateHandler(
                                nettyConfig.getHeartbeatTimeoutSeconds(), 0, 0, TimeUnit.SECONDS),
                        handler
                    };
                },
                nettyConfig.getConnectionRetries(),
                nettyConfig.getConnectionRetryWait());
    }
}
