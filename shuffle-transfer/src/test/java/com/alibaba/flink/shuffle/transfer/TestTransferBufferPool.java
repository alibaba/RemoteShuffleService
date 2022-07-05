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

import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.core.memory.Buffer;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** A {@link TransferBufferPool} as a testing utility. */
public class TestTransferBufferPool extends TransferBufferPool {

    public TestTransferBufferPool(int numBuffers, int bufferSize) {
        super(Collections.emptyList());

        List<ByteBuf> buffers = new ArrayList<>(allocateBuffers(numBuffers, bufferSize));
        addBuffers(buffers);
    }

    public ByteBuf requestBufferBlocking() {
        while (true) {
            ByteBuf byteBuf = requestBuffer();
            if (byteBuf != null) {
                return byteBuf;
            }
            CommonUtils.runQuietly(() -> Thread.sleep(10));
        }
    }

    private List<ByteBuf> allocateBuffers(int numBuffers, int bufferSize) {
        List<ByteBuf> buffers = new ArrayList<>(numBuffers);
        for (int i = 0; i < numBuffers; i++) {
            ByteBuf byteBuf = new Buffer(ByteBuffer.allocateDirect(bufferSize), this, 0);
            buffers.add(byteBuf);
        }
        return buffers;
    }
}
