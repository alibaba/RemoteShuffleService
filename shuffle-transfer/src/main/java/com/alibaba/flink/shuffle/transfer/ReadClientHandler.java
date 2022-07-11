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

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A {@link ChannelInboundHandlerAdapter} shared by multiple {@link ShuffleReadClient} for shuffle
 * read.
 */
public abstract class ReadClientHandler extends ChannelInboundHandlerAdapter {

    /** The sharing {@link ShuffleReadClient}s by {@link ChannelID}s. */
    private final Map<ChannelID, ShuffleReadClient> readClientsByChannelID =
            new ConcurrentHashMap<>();

    /** Register a {@link ShuffleReadClient}. */
    public abstract void register(ShuffleReadClient shuffleReadClient);

    /** Unregister a {@link ShuffleReadClient}. */
    public abstract void unregister(ShuffleReadClient shuffleReadClient);

    public void notifyReadCredit(TransferMessage.ReadAddCredit addCredit) {
    }

    /** Get buffer suppliers to decode shuffle read data. */
    public Function<ChannelID, Supplier<ByteBuf>> bufferSuppliers() {
        // process use readClientsByChannelID
        return null;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {

    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
// process use readClientsByChannelID
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {

    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        // process use readClientsByChannelID
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        // process use readClientsByChannelID
    }
}
