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

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.core.executor.SingleThreadExecutor;
import com.alibaba.flink.shuffle.core.executor.SingleThreadExecutorPool;
import com.alibaba.flink.shuffle.core.ids.DataPartitionID;
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.memory.BufferDispatcher;

import javax.annotation.Nullable;

import java.util.Map;

/**
 * {@link PartitionedDataStore} is the storage of {@link DataPartition}s. Different types of {@link
 * DataPartition}s can be added to and removed from this data store.
 */
public interface PartitionedDataStore {

    /**
     * Creates a {@link DataPartitionWritingView} instance as the channel to write data to. This
     * method must be called before writing any data to this data store and for each logic {@link
     * MapPartition}, a new exclusive writing view will be created and returned.
     *
     * @return A {@link DataPartitionWritingView} instance as the channel to write data to.
     */
    DataPartitionWritingView createDataPartitionWritingView(WritingViewContext context)
            throws Exception;

    /**
     * Creates a {@link DataPartitionReadingView} instance as the channel to read data from. This
     * method must be called before reading any data from this data store and for each logic {@link
     * ReducePartition} being read, a new exclusive reading view will be created and returned.
     *
     * @return A {@link DataPartitionReadingView} instance as the channel to read data from.
     */
    DataPartitionReadingView createDataPartitionReadingView(ReadingViewContext context)
            throws Exception;

    /** Returns a boolean flag indicating whether the target {@link DataPartition} is consumable. */
    boolean isDataPartitionConsumable(DataPartitionMeta partitionMeta);

    /**
     * Adds a new {@link DataPartition} to this data store. This happens when adding an external
     * {@link DataPartition} or restarting from failure. Exception will be thrown if the target
     * partition (identified by and {@link DataPartitionID}, {@link DataSetID} and {@link JobID})
     * already exists in this data store.
     */
    void addDataPartition(DataPartitionMeta partitionMeta) throws Exception;

    /**
     * Removes the {@link DataPartition} identified by the given {@link DataPartitionMeta} from this
     * data store. Different from {@link #releaseDataPartition}, this method does not releases the
     * corresponding {@link DataPartition}.
     */
    void removeDataPartition(DataPartitionMeta partitionMeta);

    /**
     * Releases the and removes {@link DataPartition} identified by the given {@link DataSetID} and
     * {@link DataPartitionID} from this data store.
     *
     * <p>Note: This method works asynchronously so does not release target partition immediately.
     */
    void releaseDataPartition(
            DataSetID dataSetID, DataPartitionID partitionID, @Nullable Throwable throwable);

    /**
     * Releases all the {@link DataPartition}s belonging to the target {@link DataSet} identified by
     * the given {@link DataSetID} from this data store.
     *
     * <p>Note: This method works asynchronously so does not release target partitions immediately.
     */
    void releaseDataSet(DataSetID dataSetID, @Nullable Throwable throwable);

    /**
     * Releases all the {@link DataPartition}s produced by the corresponding job identified by the
     * given {@link JobID} from this data store.
     *
     * <p>Note: This method works asynchronously so does not release target partitions immediately.
     */
    void releaseDataByJobID(JobID jobID, @Nullable Throwable throwable);

    /** Updates the used storage space information for the target shuffle worker. */
    void updateUsedStorageSpace();

    /** Returns the statistics information of this data store. */
    DataStoreStatistics getDataStoreStatistics();

    /**
     * Shuts down this data store and releases the resources.
     *
     * @param releaseData Whether to also release all data or not.
     */
    void shutDown(boolean releaseData);

    /** Returns true if this data store has been shut down. */
    boolean isShutDown();

    /** Returns the cluster {@link Configuration} to read the configured values. */
    Configuration getConfiguration();

    /**
     * Returns the {@link BufferDispatcher} to allocate {@link java.nio.ByteBuffer}s for data
     * writing.
     */
    BufferDispatcher getWritingBufferDispatcher();

    /**
     * Returns the {@link BufferDispatcher} to allocate {@link java.nio.ByteBuffer}s for data
     * reading.
     */
    BufferDispatcher getReadingBufferDispatcher();

    /**
     * Returns the {@link SingleThreadExecutorPool} to allocate {@link SingleThreadExecutor}s for
     * {@link DataPartition} processing.
     */
    SingleThreadExecutorPool getExecutorPool(StorageMeta storageMeta);

    /** Updates the available storage space information for the target shuffle worker. */
    void updateFreeStorageSpace();

    /**
     * Checks the health status of the underlying storage. It will remove the unhealthy storage and
     * add the healthy storage back.
     */
    void updateStorageHealthStatus();

    /**
     * Gets the storage space information indexed by partition factory name for the target shuffle
     * worker.
     */
    Map<String, StorageSpaceInfo> getStorageSpaceInfos();
}
