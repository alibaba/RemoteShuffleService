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

package com.alibaba.flink.shuffle.core.memory;

import com.alibaba.flink.shuffle.common.utils.CommonUtils;

import org.apache.flink.shaded.netty4.io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.flink.shaded.netty4.io.netty.buffer.UnpooledDirectByteBuf;

import java.nio.ByteBuffer;

/**
 * We use {@link UnpooledDirectByteBuf} directly to reduce one copy from {@link Buffer} to Netty's
 * {@link org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf} when transmitting data over the
 * network.
 */
public class Buffer extends UnpooledDirectByteBuf {

    private final ByteBuffer buffer;

    private final BufferRecycler recycler;

    public Buffer(ByteBuffer buffer, BufferRecycler recycler, int readableBytes) {
        super(UnpooledByteBufAllocator.DEFAULT, buffer, buffer.capacity());

        CommonUtils.checkArgument(recycler != null, "Must be not null.");
        CommonUtils.checkArgument(buffer.position() == 0, "Position must be 0.");

        this.buffer = buffer;
        this.recycler = recycler;
        writerIndex(readableBytes);
    }

    @Override
    protected void deallocate() {
        buffer.clear();
        recycler.recycle(buffer);
    }
}
