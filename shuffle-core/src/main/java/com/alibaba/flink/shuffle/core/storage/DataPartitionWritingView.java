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

import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.core.ids.ReducePartitionID;
import com.alibaba.flink.shuffle.core.listener.DataCommitListener;
import com.alibaba.flink.shuffle.core.memory.Buffer;
import com.alibaba.flink.shuffle.core.memory.BufferSupplier;

import javax.annotation.Nullable;

/**
 * When trying to write data to a {@link DataPartition}, the data producer needs to call {@link
 * PartitionedDataStore#createDataPartitionWritingView} which will return an instance of this
 * interface, then the producer can write data to the target {@link DataPartition} data through
 * these {@link #regionStarted}, {@link #onBuffer}, {@link #regionFinished} and {@link #finish}
 * method.
 *
 * <p>Data {@link Buffer} is the smallest piece of data can be written and data region is the basic
 * data unit can be written which can contain multiple data {@link Buffer}s. Data must be divided
 * into one or multiple data regions before writing. Each data region is a piece data that can be
 * consumed independently, which means a data region can not contain any partial records and data
 * compression should never span multiple data regions. As a result, we can rearrange the data
 * regions consumed by the same data consumer freely.
 *
 * <p>Before writing a data region, the {@link #regionStarted} method must be called, after which,
 * data {@link Buffer}s can be written through the {@link #onBuffer} method. Then the data region
 * can be marked as finished by the {@link #regionFinished} method. After all target data has been
 * written, the {@link #finish} method can be called to finish data writing.
 */
public interface DataPartitionWritingView {

    /**
     * Writes a {@link Buffer} of the given {@link MapPartitionID} and {@link ReducePartitionID} to
     * the corresponding {@link DataPartition} through this partition writing view.
     */
    void onBuffer(Buffer buffer, ReducePartitionID reducePartitionID);

    /**
     * Marks the starting of a new data region and announces the number of credits required by the
     * new data region.
     *
     * @param dataRegionIndex Index of the new data region to be written.
     * @param isBroadcastRegion Whether to broadcast data to all reduce partitions in this region.
     */
    void regionStarted(int dataRegionIndex, boolean isBroadcastRegion);

    /**
     * Marks the current data region as finished, after which no data of the same region will be
     * written any more and the current data region is completed and ready to be processed.
     */
    void regionFinished();

    /**
     * Finishes the data input, which means no data will be written through this partition writing
     * view any more.
     *
     * @param commitListener Listener to be notified after all data of the input is committed.
     */
    void finish(DataCommitListener commitListener);

    /**
     * Notifies an error to the {@link DataPartitionWriter} if the data producer encounters any
     * unrecoverable failure.
     */
    void onError(@Nullable Throwable throwable);

    /**
     * Returns a {@link BufferSupplier} instance from which free {@link Buffer}s can be allocated
     * for data receiving of the network stack.
     */
    BufferSupplier getBufferSupplier();
}
