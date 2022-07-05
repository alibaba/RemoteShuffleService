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

import com.alibaba.flink.shuffle.transfer.TransferMessage.CloseConnection;
import com.alibaba.flink.shuffle.transfer.TransferMessage.ReadData;
import com.alibaba.flink.shuffle.transfer.TransferMessage.WriteData;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** A dummy {@link ChannelInboundHandlerAdapter} for testing. */
public class DummyChannelInboundHandlerAdaptor extends ChannelInboundHandlerAdapter {

    private volatile ChannelHandlerContext currentCtx;

    private final List<Object> messages = Collections.synchronizedList(new ArrayList<>());

    private volatile Throwable cause;

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        this.cause = cause;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        currentCtx = ctx;
    }

    @Override
    public synchronized void channelRead(ChannelHandlerContext ctx, Object msg) {
        messages.add(msg);
        if (msg instanceof CloseConnection) {
            ctx.channel().close();
        }
    }

    public synchronized Object getMsg(int index) {
        if (index >= messages.size()) {
            return null;
        }
        return messages.get(index);
    }

    public synchronized Object getLastMsg() {
        if (messages.isEmpty()) {
            return null;
        }
        return messages.get(messages.size() - 1);
    }

    public synchronized List<Object> getMessages() {
        return messages;
    }

    public synchronized int numMessages() {
        return messages.size();
    }

    public synchronized boolean isEmpty() {
        return messages.isEmpty();
    }

    public void send(Object obj) {
        currentCtx.writeAndFlush(obj);
    }

    public boolean isConnected() {
        return currentCtx != null;
    }

    public void close() {
        messages.forEach(
                msg -> {
                    if (msg instanceof WriteData) {
                        ((WriteData) msg).getBuffer().release();
                    }
                    if (msg instanceof ReadData) {
                        ((ReadData) msg).getBuffer().release();
                    }
                });
        currentCtx.close().awaitUninterruptibly();
    }
}
