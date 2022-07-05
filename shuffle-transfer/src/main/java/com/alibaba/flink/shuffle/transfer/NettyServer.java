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

import com.alibaba.flink.shuffle.core.executor.ExecutorThreadFactory;
import com.alibaba.flink.shuffle.core.storage.PartitionedDataStore;
import com.alibaba.flink.shuffle.core.utils.FatalExitExceptionHandler;

import org.apache.flink.shaded.netty4.io.netty.bootstrap.ServerBootstrap;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelOption;
import org.apache.flink.shaded.netty4.io.netty.channel.epoll.Epoll;
import org.apache.flink.shaded.netty4.io.netty.channel.epoll.EpollEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.epoll.EpollServerSocketChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.flink.shaded.netty4.io.netty.handler.timeout.IdleStateHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkState;

/** Facility to provide shuffle service based on Netty framework. */
public class NettyServer {

    /** Heartbeat timeout. */
    private final int heartbeatTimeout;

    /** Heartbeat interval. */
    private final int heartbeatInterval;

    private boolean heartbeatEnabled = true;

    private static final ExecutorThreadFactory.Builder THREAD_FACTORY_BUILDER =
            new ExecutorThreadFactory.Builder()
                    .setExceptionHandler(FatalExitExceptionHandler.INSTANCE);

    private static final Logger LOG = LoggerFactory.getLogger(NettyServer.class);

    private final NettyConfig nettyConfig;

    private ServerBootstrap bootstrap;

    private ChannelFuture bindFuture;

    private final PartitionedDataStore dataStore;

    public NettyServer(PartitionedDataStore dataStore, NettyConfig nettyConfig) {
        this.dataStore = dataStore;
        this.nettyConfig = nettyConfig;
        this.heartbeatTimeout = nettyConfig.getHeartbeatTimeoutSeconds();
        this.heartbeatInterval = nettyConfig.getHeartbeatIntervalSeconds();
    }

    public static ThreadFactory getNamedThreadFactory(String name) {
        return THREAD_FACTORY_BUILDER.setPoolName(name).build();
    }

    public void start() throws IOException {
        LOG.info("Starting NettyServer on port " + nettyConfig.getServerPort());
        init(() -> new ServerChannelInitializer(this::getServerHandlers));
    }

    public ChannelHandler[] getServerHandlers() {
        WriteServerHandler writeServerHandler =
                new WriteServerHandler(dataStore, heartbeatEnabled ? heartbeatInterval : -1);
        ReadServerHandler readServerHandler =
                new ReadServerHandler(dataStore, heartbeatEnabled ? heartbeatInterval : -1);

        WritingService writingService = writeServerHandler.getWritingService();
        ReadingService readingService = readServerHandler.getReadingService();

        return new ChannelHandler[] {
            new TransferMessageEncoder(),
            DecoderDelegate.serverDecoderDelegate(writingService::getBufferSupplier),
            new IdleStateHandler(heartbeatTimeout, 0, 0, TimeUnit.SECONDS),
            writeServerHandler,
            readServerHandler,
            new DataSender(readingService),
        };
    }

    public void disableHeartbeat() {
        heartbeatEnabled = false;
    }

    private void init(Supplier<ServerChannelInitializer> channelInitializer) throws IOException {

        checkState(bootstrap == null, "Netty server has already been initialized.");

        final long start = System.nanoTime();

        bootstrap = new ServerBootstrap();

        // --------------------------------------------------------------------
        // Transport-specific configuration
        // --------------------------------------------------------------------

        switch (nettyConfig.getTransportType()) {
            case NIO:
                initNioBootstrap();
                break;

            case EPOLL:
                initEpollBootstrap();
                break;

            case AUTO:
                if (Epoll.isAvailable()) {
                    initEpollBootstrap();
                    LOG.info("Transport type 'auto': using EPOLL.");
                } else {
                    initNioBootstrap();
                    LOG.info("Transport type 'auto': using NIO.");
                }
        }

        // --------------------------------------------------------------------
        // Configuration
        // --------------------------------------------------------------------

        // Server bind address
        bootstrap.localAddress(nettyConfig.getServerAddress(), nettyConfig.getServerPort());

        int serverBacklog = nettyConfig.getServerConnectBacklog();
        if (serverBacklog > 0) {
            bootstrap.option(ChannelOption.SO_BACKLOG, serverBacklog);
        }

        // Receive and send buffer size
        int receiveAndSendBufferSize = nettyConfig.getSendAndReceiveBufferSize();
        if (receiveAndSendBufferSize > 0) {
            bootstrap.childOption(ChannelOption.SO_SNDBUF, receiveAndSendBufferSize);
            bootstrap.childOption(ChannelOption.SO_RCVBUF, receiveAndSendBufferSize);
        }

        // --------------------------------------------------------------------
        // Child channel pipeline for accepted connections
        // --------------------------------------------------------------------

        bootstrap.childHandler(channelInitializer.get());

        // --------------------------------------------------------------------
        // Start Server
        // --------------------------------------------------------------------

        bindFuture = bootstrap.bind().syncUninterruptibly();
        InetSocketAddress localAddress = (InetSocketAddress) bindFuture.channel().localAddress();

        final long duration = (System.nanoTime() - start) / 1_000_000;
        LOG.info(
                "Successful initialization (took {} ms). Listening on SocketAddress {}.",
                duration,
                localAddress);
    }

    public void shutdown() {
        final long start = System.nanoTime();
        if (bindFuture != null) {
            bindFuture.channel().close().awaitUninterruptibly();
            bindFuture = null;
        }

        if (bootstrap != null) {
            if (bootstrap.group() != null) {
                bootstrap.group().shutdownGracefully();
            }
            bootstrap = null;
        }
        final long duration = (System.nanoTime() - start) / 1_000_000;
        LOG.info(
                "Successful shutdown on port {} (took {} ms).",
                nettyConfig.getServerPort(),
                duration);
    }

    private void initNioBootstrap() {
        // Add the server port number to the name in order to distinguish
        // multiple servers running on the same host.
        String name =
                NettyConfig.SERVER_THREAD_GROUP_NAME + " (" + nettyConfig.getServerPort() + ")";

        NioEventLoopGroup bossGroup = new NioEventLoopGroup(1);
        NioEventLoopGroup workerGroup =
                new NioEventLoopGroup(
                        nettyConfig.getServerNumThreads(), getNamedThreadFactory(name));
        bootstrap.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class);
    }

    private void initEpollBootstrap() {
        // Add the server port number to the name in order to distinguish
        // multiple servers running on the same host.
        String name =
                NettyConfig.SERVER_THREAD_GROUP_NAME + " (" + nettyConfig.getServerPort() + ")";

        EpollEventLoopGroup bossGroup = new EpollEventLoopGroup(1);
        EpollEventLoopGroup workerGroup =
                new EpollEventLoopGroup(
                        nettyConfig.getServerNumThreads(), getNamedThreadFactory(name));
        bootstrap.group(bossGroup, workerGroup).channel(EpollServerSocketChannel.class);
    }

    private static class ServerChannelInitializer extends ChannelInitializer<SocketChannel> {

        private final Supplier<ChannelHandler[]> serverHandlersProvider;

        public ServerChannelInitializer(Supplier<ChannelHandler[]> serverHandlersProvider) {
            this.serverHandlersProvider = serverHandlersProvider;
        }

        @Override
        public void initChannel(SocketChannel channel) {
            channel.pipeline().addLast(serverHandlersProvider.get());
        }
    }
}
