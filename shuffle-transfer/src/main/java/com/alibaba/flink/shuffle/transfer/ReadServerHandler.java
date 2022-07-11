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

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;

/** A {@link ChannelInboundHandler} serves shuffle read process on server side. */
public abstract class ReadServerHandler extends SimpleChannelInboundHandler<TransferMessage> {

    /** Service logic underground. */
    private final ReadingService readingService = null;

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, TransferMessage msg) {
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
    }

    private void onMessage(ChannelHandlerContext ctx, TransferMessage msg) throws Throwable {
        // process msg with ReadingService
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {

    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {

    }

    public ReadingService getReadingService() {
        return readingService;
    }

    // This method is invoked when:
    // 1. Received CloseConnection from client;
    // 2. Network error --
    //    a. sending failure message;
    //    b. connection inactive;
    //    c. connection exception caught;
    //
    // This method does below things:
    // 1. Triggering errors to corresponding logical channels;
    // 2. Stopping the heartbeat to client;
    // 3. Closing physical connection;
    public abstract void close(ChannelHandlerContext ctx, Throwable throwable);
}
