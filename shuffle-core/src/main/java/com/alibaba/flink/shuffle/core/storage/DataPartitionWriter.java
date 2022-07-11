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
import com.alibaba.flink.shuffle.core.memory.BufferRecycler;
import com.alibaba.flink.shuffle.core.memory.BufferSupplier;

/**
 * Data writer for {@link DataPartition}. Each {@link DataPartitionWriter} can write data of one
 * same {@link MapPartitionID} to the target {@link DataPartition}, that is, those data must come
 * from the same data producer.
 */
public interface DataPartitionWriter extends BufferSupplier {

    /**
     * Returns the corresponding {@link MapPartitionID} of the data to be written by this partition
     * writer.
     */
    MapPartitionID getMapPartitionID();

    /**
     * Writes the input data to the target {@link DataPartition} and returns true if all data has
     * been written successfully.
     */
    boolean writeData() throws Exception;

    /**
     * Adds a data {@link Buffer} of the given {@link MapPartitionID} and {@link ReducePartitionID}
     * to this partition writer.
     */
    void addBuffer(ReducePartitionID reducePartitionID, Buffer buffer);

    /**
     * Starts a new data region and announces the number of credits required by the data region.
     *
     * @param dataRegionIndex Index of the new data region to be written.
     * @param isBroadcastRegion Whether to broadcast data to all reduce partitions in this region.
     */
    void startRegion(int dataRegionIndex, boolean isBroadcastRegion);

    /**
     * Finishes the current data region, after which the current data region is completed and ready
     * to be processed.
     */
    void finishRegion();

    /**
     * Finishes the data input, which means no data can be added to this partition writer any more.
     *
     * @param commitListener Listener to be notified after all data of the input is committed.
     */
    void finishDataInput(DataCommitListener commitListener);

    /**
     * Assigns credits to this partition writer to be used to receive data from the corresponding
     * data producer. Returns true if this partition writer still needs more credits (buffers) for
     * data receiving.
     */
    boolean assignCredits(BufferQueue credits, BufferRecycler recycler);

    /**
     * Notifies the failure to this partition writer when any exception occurs at the corresponding
     * data producer side.
     */
    void onError(Throwable throwable);

    /** Releases this partition writer which releases all resources if any exception occurs. */
    void release(Throwable throwable) throws Exception;
}
