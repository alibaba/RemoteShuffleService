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

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.Map;

/**
 * A {@link ChannelInboundHandlerAdapter} shared by multiple {@link ShuffleWriteClient} for shuffle
 * write.
 */
public abstract class WriteClientHandler extends ChannelInboundHandlerAdapter {

    /** The sharing {@link ShuffleWriteClient}s by {@link ChannelID}s. */
    private final Map<ChannelID, ShuffleWriteClient> writeClientsByChannelID = null;

    /** Register a {@link ShuffleWriteClient}. */
    public abstract void register(ShuffleWriteClient shuffleWriteClient);

    /** Unregister a {@link ShuffleWriteClient}. */
    public abstract void unregister(ShuffleWriteClient shuffleWriteClient);

    /** Whether a {@link ChannelID} is registered ever. */
    public boolean isRegistered(ChannelID channelID) {
        return writeClientsByChannelID.containsKey(channelID);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        // use client to process msg
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
    }
}
