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
import com.alibaba.flink.shuffle.core.storage.PartitionedDataStore;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandler;

import java.util.function.Supplier;

/**
 * Harness used to write data to storage. It performs shuffle write by {@link PartitionedDataStore}.
 * The lifecycle is the same with a Netty {@link ChannelInboundHandler} instance.
 */
public interface WritingService {

    void handshake();

    void write(ChannelID channelID, int subIdx, ByteBuf byteBuf);

    Supplier<ByteBuf> getBufferSupplier(ChannelID channelID);

    void regionStart(ChannelID channelID, int regionIdx, boolean isBroadcast);

    void regionFinish(ChannelID channelID);

    void writeFinish(ChannelID channelID, Runnable committedListener);

    void closeAbnormallyIfUnderServing(ChannelID channelID);

    void releaseOnError(Throwable cause, ChannelID channelID);
}
