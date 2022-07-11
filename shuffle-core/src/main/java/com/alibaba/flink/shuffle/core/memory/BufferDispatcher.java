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

import com.alibaba.flink.shuffle.core.ids.DataPartitionID;
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.listener.BufferListener;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayDeque;
import java.util.Queue;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkArgument;

/** Wrapper to allocate buffers from {@link ByteBufferPool}. */
public abstract class BufferDispatcher {
    private final ByteBufferPool bufferPool;

    @GuardedBy("lock")
    private final Queue<BufferRequirement> bufferRequirements;

    /**
     * @param name Name of the underlying buffer pool.
     * @param numBuffers Total number of available buffers when start.
     * @param bufferSize Size of a single buffer.
     */
    public BufferDispatcher(String name, int numBuffers, int bufferSize) {
        this.bufferPool = new ByteBufferPool(name, numBuffers, bufferSize);
        this.bufferRequirements = new ArrayDeque<>();
    }

    /**
     * Request buffer(s) asynchronously. Note that buffers will be allocated by priorities and
     * notified by {@link BufferListener}.
     */
    public abstract void requestBuffer(
            JobID jobID,
            DataSetID dataSetID,
            DataPartitionID dataPartitionID,
            int min,
            int max,
            BufferListener listener);

    /** Destroy this {@link BufferDispatcher} and underlying {@link ByteBufferPool}. */
    public abstract void destroy();

    private static final class BufferRequirement {

        private final int min;
        private final int max;
        private final BufferListener bufferListener;

        BufferRequirement(int min, int max, BufferListener bufferListener) {
            checkArgument(
                    min > 0 && max > 0 && max >= min,
                    String.format("Invalid min=%d, max=%d.", min, max));
            this.min = min;
            this.max = max;
            this.bufferListener = bufferListener;
        }
    }
}
