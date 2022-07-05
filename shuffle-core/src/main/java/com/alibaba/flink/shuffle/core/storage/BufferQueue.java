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

package com.alibaba.flink.shuffle.core.storage;

import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.core.memory.BufferDispatcher;
import com.alibaba.flink.shuffle.core.utils.BufferUtils;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Queue;

/** A buffer queue implementation enhanced with a <b>release</b> state. */
@NotThreadSafe
public class BufferQueue {

    /** All available buffers in this buffer queue. */
    private final Queue<ByteBuffer> buffers = new ArrayDeque<>();

    /** Target {@link DataPartition} this buffer queue belongs to. */
    private final DataPartition owner;

    /** Target {@link BufferDispatcher} where buffers are allocated from and recycled to. */
    private final BufferDispatcher dispatcher;

    /** Number of buffers occupied by this buffer queue (added but still not recycled). */
    private int numBuffersOccupied;

    /** Whether this buffer queue is released or not. */
    private boolean isReleased;

    public BufferQueue(DataPartition owner, BufferDispatcher dispatcher) {
        CommonUtils.checkArgument(owner != null, "Must be not null.");
        CommonUtils.checkArgument(dispatcher != null, "Must be not null.");

        this.owner = owner;
        this.dispatcher = dispatcher;
    }

    /** Returns the number of available buffers in this buffer queue. */
    public int size() {
        return buffers.size();
    }

    /**
     * Returns an available buffer from this buffer queue or returns null if no buffer is available
     * currently.
     */
    @Nullable
    public ByteBuffer poll() {
        return buffers.poll();
    }

    /**
     * Adds an available buffer to this buffer queue and will throw exception if this buffer queue
     * has been released.
     */
    public void add(ByteBuffer availableBuffer) {
        CommonUtils.checkArgument(availableBuffer != null, "Must be not null.");
        CommonUtils.checkState(!isReleased, "Buffer queue has been released.");

        buffers.add(availableBuffer);
        ++numBuffersOccupied;
    }

    /**
     * Adds a collection of available buffers to this buffer queue and will throw exception if this
     * buffer queue has been released.
     */
    public void add(Collection<ByteBuffer> availableBuffers) {
        CommonUtils.checkArgument(availableBuffers != null, "Must be not null.");
        CommonUtils.checkState(!isReleased, "Buffer queue has been released.");

        buffers.addAll(availableBuffers);
        numBuffersOccupied += availableBuffers.size();
    }

    public int numBuffersOccupied() {
        return numBuffersOccupied;
    }

    public void recycleAll() {
        numBuffersOccupied -= buffers.size();

        DataPartitionMeta dpm = owner.getPartitionMeta();
        BufferUtils.recycleBuffers(
                buffers, dispatcher, dpm.getJobID(), dpm.getDataSetID(), dpm.getDataPartitionID());
        buffers.clear();
    }

    public void recycle(ByteBuffer buffer) {
        if (buffer == null) {
            return;
        }
        --numBuffersOccupied;

        DataPartitionMeta dpm = owner.getPartitionMeta();
        BufferUtils.recycleBuffer(
                buffer, dispatcher, dpm.getJobID(), dpm.getDataSetID(), dpm.getDataPartitionID());
    }

    /**
     * Releases this buffer queue and recycles all available buffers. After released, no buffer can
     * be added to or polled from this buffer queue.
     */
    public void release() {
        isReleased = true;
        recycleAll();
    }

    /** Returns true is this buffer queue has been released. */
    public boolean isReleased() {
        return isReleased;
    }
}
