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
import com.alibaba.flink.shuffle.core.utils.FatalExitExceptionHandler;

import org.apache.flink.shaded.netty4.io.netty.bootstrap.Bootstrap;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelException;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelOption;
import org.apache.flink.shaded.netty4.io.netty.channel.epoll.Epoll;
import org.apache.flink.shaded.netty4.io.netty.channel.epoll.EpollEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.epoll.EpollSocketChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioSocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.function.Supplier;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkState;

/** Netty client to create connections to remote. */
public class NettyClient {

    private static final Logger LOG = LoggerFactory.getLogger(NettyClient.class);

    private static final ExecutorThreadFactory.Builder THREAD_FACTORY_BUILDER =
            new ExecutorThreadFactory.Builder()
                    .setExceptionHandler(FatalExitExceptionHandler.INSTANCE);

    private final NettyConfig config;

    private Supplier<ChannelHandler[]> channelHandlersSupplier;

    private Bootstrap bootstrap;

    public NettyClient(NettyConfig config) {
        this.config = config;
    }

    public void init(Supplier<ChannelHandler[]> channelHandlersSupplier) throws IOException {

        checkState(bootstrap == null, "Netty client has already been initialized.");

        this.channelHandlersSupplier = channelHandlersSupplier;

        final long start = System.nanoTime();

        bootstrap = new Bootstrap();

        // --------------------------------------------------------------------
        // Transport-specific configuration
        // --------------------------------------------------------------------

        switch (config.getTransportType()) {
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

        bootstrap.option(ChannelOption.TCP_NODELAY, true);
        bootstrap.option(ChannelOption.SO_KEEPALIVE, true);

        // Timeout for new connections
        bootstrap.option(
                ChannelOption.CONNECT_TIMEOUT_MILLIS,
                config.getClientConnectTimeoutSeconds() * 1000);

        // Receive and send buffer size
        int receiveAndSendBufferSize = config.getSendAndReceiveBufferSize();
        if (receiveAndSendBufferSize > 0) {
            bootstrap.option(ChannelOption.SO_SNDBUF, receiveAndSendBufferSize);
            bootstrap.option(ChannelOption.SO_RCVBUF, receiveAndSendBufferSize);
        }

        final long duration = (System.nanoTime() - start) / 1_000_000;
        LOG.info("Successful initialization (took {} ms).", duration);
    }

    public void shutdown() {
        final long start = System.nanoTime();

        if (bootstrap != null) {
            if (bootstrap.group() != null) {
                bootstrap.group().shutdownGracefully();
            }
            bootstrap = null;
        }

        final long duration = (System.nanoTime() - start) / 1_000_000;
        LOG.info("Successful shutdown (took {} ms).", duration);
    }

    private void initNioBootstrap() {
        // Add the server port number to the name in order to distinguish
        // multiple clients running on the same host.
        String name = NettyConfig.CLIENT_THREAD_GROUP_NAME + " (" + config.getServerPort() + ")";

        NioEventLoopGroup nioGroup =
                new NioEventLoopGroup(
                        config.getClientNumThreads(),
                        THREAD_FACTORY_BUILDER.setPoolName(name).build());
        bootstrap.group(nioGroup).channel(NioSocketChannel.class);
    }

    private void initEpollBootstrap() {
        // Add the server port number to the name in order to distinguish
        // multiple clients running on the same host.
        String name = NettyConfig.CLIENT_THREAD_GROUP_NAME + " (" + config.getServerPort() + ")";

        EpollEventLoopGroup epollGroup =
                new EpollEventLoopGroup(
                        config.getClientNumThreads(),
                        THREAD_FACTORY_BUILDER.setPoolName(name).build());
        bootstrap.group(epollGroup).channel(EpollSocketChannel.class);
    }

    // ------------------------------------------------------------------------
    // Client connections
    // ------------------------------------------------------------------------

    public ChannelFuture connect(final InetSocketAddress serverSocketAddress) {
        checkState(bootstrap != null, "Client has not been initialized yet.");

        // --------------------------------------------------------------------
        // Child channel pipeline for accepted connections
        // --------------------------------------------------------------------

        bootstrap.handler(
                new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel channel) {
                        channel.pipeline().addLast(channelHandlersSupplier.get());
                    }
                });

        try {
            return bootstrap.connect(serverSocketAddress);
        } catch (ChannelException e) {
            if ((e.getCause() instanceof java.net.SocketException
                            && e.getCause().getMessage().equals("Too many open files"))
                    || (e.getCause() instanceof ChannelException
                            && e.getCause().getCause() instanceof java.net.SocketException
                            && e.getCause()
                                    .getCause()
                                    .getMessage()
                                    .equals("Too many open files"))) {
                throw new ChannelException(
                        ""
                                + "The operating system does not offer enough file handles"
                                + " to open the network connection. Please increase the"
                                + " number of available file handles.",
                        e.getCause());
            } else {
                throw e;
            }
        }
    }
}
