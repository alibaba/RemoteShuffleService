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
import com.alibaba.flink.shuffle.core.ids.DataPartitionID;
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.JobID;

import java.io.DataInput;
import java.util.Map;

/**
 * Factory to create new {@link DataPartition}s. Different type of {@link DataPartition}s need
 * different {@link DataPartitionFactory}s.
 */
public interface DataPartitionFactory {

    /** Initializes this partition factory with the given {@link Configuration}. */
    void initialize(Configuration configuration) throws Exception;

    /**
     * Creates and returns a {@link DataPartition} of the corresponding type to write data to and
     * read data from. This method should never allocate any resource to avoid resource leak in case
     * of worker crash.
     *
     * @param dataStore {@link PartitionedDataStore} which stores the created {@link DataPartition}.
     * @param jobID ID of the job which is trying to write data to the {@link PartitionedDataStore}.
     * @param dataSetID ID of the dataset to witch the {@link DataPartition} belongs to.
     * @param dataPartitionID ID of the {@link DataPartition} to be created and written.
     * @param numReducePartitions Number of logic {@link ReducePartition}s of the {@link DataSet}.
     * @return A new {@link DataPartition} to write data to and read data from.
     */
    DataPartition createDataPartition(
            PartitionedDataStore dataStore,
            JobID jobID,
            DataSetID dataSetID,
            DataPartitionID dataPartitionID,
            int numReducePartitions)
            throws Exception;

    /**
     * Creates and returns a {@link DataPartition} from the given {@link DataPartitionMeta}. This
     * method can be used to recover lost {@link DataPartition}s after failure.
     *
     * @param dataStore {@link PartitionedDataStore} which stores the created {@link DataPartition}.
     * @param partitionMeta {@link DataPartitionMeta} used to construct the {@link DataPartition}.
     * @return A {@link DataPartition} constructed from the given {@link DataPartitionMeta}.
     */
    DataPartition createDataPartition(
            PartitionedDataStore dataStore, DataPartitionMeta partitionMeta) throws Exception;

    /**
     * Recovers and returns a {@link DataPartitionMeta} instance from the given {@link DataInput}.
     * The created {@link DataPartitionMeta} can be used to construct lost {@link DataPartition}s.
     */
    DataPartitionMeta recoverDataPartitionMeta(DataInput dataInput) throws Exception;

    /** Returns the data partition type this partition factory is going to create. */
    DataPartition.DataPartitionType getDataPartitionType();

    /** Updates the available storage space information. */
    void updateFreeStorageSpace();

    /** Gets the storage space information, including used space and available space. */
    StorageSpaceInfo getStorageSpaceInfo();

    /**
     * Whether the given storage space is valid (no overuse and remaining space enough) for this
     * data partition factory.
     */
    boolean isStorageSpaceValid(
            StorageSpaceInfo storageSpaceInfo,
            long minReservedSpaceBytes,
            long maxUsableSpaceBytes);

    /**
     * Checks the health status of the underlying storage. It will remove the unhealthy storage and
     * add the healthy storage back.
     */
    void updateStorageHealthStatus();

    /** Whether this partition factory only uses SSD as storage device. */
    boolean useSsdOnly();

    /** Whether this partition factory only uses HDD as storage device. */
    boolean useHddOnly();

    /** Obtains the underlying storage media name from the storage path. */
    String getStorageNameFromPath(String storagePath);

    /** Updates the storage spaces in bytes already used. */
    void updateUsedStorageSpace(Map<String, Long> storageUsedBytes);
}
