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

import com.alibaba.flink.shuffle.core.memory.BufferRecycler;

/**
 * Data reader for {@link DataPartition}. Each {@link DataPartitionReader} can read data from one or
 * multiple consecutive {@link ReducePartition}s.
 */
public interface DataPartitionReader extends Comparable<DataPartitionReader> {

    /**
     * Opens this partition reader for data reading. A partition reader must be opened first before
     * reading any data.
     */
    void open() throws Exception;

    /**
     * Reads data from the corresponding {@link DataPartition}. It can allocate buffers from the
     * given {@link BufferQueue} if any memory is needed and the allocated buffers must be recycled
     * by the given {@link BufferRecycler}.
     */
    boolean readData(BufferQueue buffers, BufferRecycler recycler) throws Exception;

    /**
     * Returns a {@link BufferWithBacklog} instance containing one data buffer read and the number
     * of remaining data buffers already read in this partition reader.
     */
    BufferWithBacklog nextBuffer() throws Exception;

    /** Releases this partition reader which releases all resources if any exception occurs. */
    void release(Throwable throwable) throws Exception;

    /**
     * Returns true if this partition reader has finished reading the target {@link DataPartition}
     * and all the data read has been consumed by the corresponding data consumer.
     */
    boolean isFinished();

    /**
     * Returns the data reading priority of this partition reader. Lower value means higher priority
     * and multiple partition readers of the same {@link DataPartition} will read data in the order
     * of this priority.
     */
    long getPriority();

    /**
     * Notifies the failure to this partition reader when any exception occurs at the corresponding
     * data consumer side.
     */
    void onError(Throwable throwable);

    /**
     * Whether this partition reader has been opened or not. A partition reader must be opened first
     * before reading any data.
     */
    boolean isOpened();

    @Override
    default int compareTo(DataPartitionReader that) {
        return Long.compare(getPriority(), that.getPriority());
    }
}
