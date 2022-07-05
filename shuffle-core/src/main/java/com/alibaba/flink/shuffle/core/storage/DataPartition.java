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
import com.alibaba.flink.shuffle.core.listener.BacklogListener;
import com.alibaba.flink.shuffle.core.listener.DataListener;
import com.alibaba.flink.shuffle.core.listener.DataRegionCreditListener;
import com.alibaba.flink.shuffle.core.listener.FailureListener;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

/**
 * {@link DataPartition} is a collection of partitioned data and is the basic data unit managed by
 * remote shuffle workers. One or multiple data producers can write the produced data to a {@link
 * DataPartition} and one or multiple data consumers can read data from it.
 *
 * <p>In distributed computation, large dataset will be divided into smaller data partitions to be
 * processed by parallel tasks. Between adjacent computation vertices in a computation graph, the
 * producer task will produce a collection of data, which will be divided into multi partitions and
 * consumed by the corresponding consumer task. The well known map-reduce naming style is adopted,
 * the data collection produced by the producer task is called {@link MapPartition} and the data
 * collection consumed by the consumer task is called {@link ReducePartition}. Logically, each piece
 * of data belongs to both a {@link MapPartition} and a {@link ReducePartition}, physically, it can
 * be stored in either way.
 */
public interface DataPartition {

    /** Returns the {@link DataPartitionMeta} of this data partition. */
    DataPartitionMeta getPartitionMeta();

    /** Returns the {@link DataPartitionType} of this data partition. */
    DataPartitionType getPartitionType();

    /**
     * Creates and returns a {@link DataPartitionWriter} with the target {@link MapPartitionID}.
     * This method must release all allocated resources itself if any exception occurs.
     *
     * @param mapPartitionID ID of the {@link MapPartition} that the data belongs to.
     * @param dataRegionCreditListener Listener to be notified when available.
     * @param failureListener Listener to be notified when any failure occurs.
     * @return The target {@link DataPartitionWriter} used to write data to this data partition.
     */
    DataPartitionWriter createPartitionWriter(
            MapPartitionID mapPartitionID,
            DataRegionCreditListener dataRegionCreditListener,
            FailureListener failureListener)
            throws Exception;

    /**
     * Creates and returns a {@link DataPartitionReader} for the target range of reduce partitions.
     * This method must release all allocated resources itself if any exception occurs.
     *
     * @param startPartitionIndex Index of the first logic {@link ReducePartition} to read.
     * @param endPartitionIndex Index of the last logic {@link ReducePartition} to read (inclusive).
     * @param dataListener Listener to be notified when any data is available.
     * @param failureListener Listener to be notified when any failure occurs.
     * @return The target {@link DataPartitionReader} used to read data from this data partition.
     */
    DataPartitionReader createPartitionReader(
            int startPartitionIndex,
            int endPartitionIndex,
            DataListener dataListener,
            BacklogListener backlogListener,
            FailureListener failureListener)
            throws Exception;

    /**
     * Releases this data partition which release all resources including the data. This method can
     * be called multiple times so must be reentrant.
     *
     * @param releaseCause Cause which leads to the release, null means released after consumption.
     */
    CompletableFuture<?> releasePartition(@Nullable Throwable releaseCause);

    /** Returns a boolean flag indicating whether this data partition is consumable or not. */
    boolean isConsumable();

    /** Returns the statistics information of this data partition. */
    DataPartitionStatistics getDataPartitionStatistics();

    /**
     * Type definition of {@link DataPartition}. All {@link DataPartition}s must be either of type
     * {@link #MAP_PARTITION} or type {@link #REDUCE_PARTITION}.
     */
    enum DataPartitionType {

        /**
         * <b>MAP_PARTITION</b> is a type of {@link DataPartition} in which all data must be of the
         * same {@link MapPartitionID} but can have different {@link ReducePartitionID}s. For
         * example, data produced by a map task in map-reduce computation model can be stored as a
         * map partition.
         */
        MAP_PARTITION,

        /**
         * <b>REDUCE_PARTITION</b> is a type of {@link DataPartition} in which all data must be of
         * the same {@link ReducePartitionID} but can have different {@link MapPartitionID}s. For
         * example, data consumed by a reduce task in map-reduce computation model can be stored as
         * a reduce partition.
         */
        REDUCE_PARTITION
    }
}
