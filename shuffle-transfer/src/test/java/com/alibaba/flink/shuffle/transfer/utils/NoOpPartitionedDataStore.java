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

package com.alibaba.flink.shuffle.transfer.utils;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.core.executor.SingleThreadExecutorPool;
import com.alibaba.flink.shuffle.core.ids.DataPartitionID;
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.memory.BufferDispatcher;
import com.alibaba.flink.shuffle.core.storage.DataPartitionMeta;
import com.alibaba.flink.shuffle.core.storage.DataPartitionReadingView;
import com.alibaba.flink.shuffle.core.storage.DataPartitionWritingView;
import com.alibaba.flink.shuffle.core.storage.DataStoreStatistics;
import com.alibaba.flink.shuffle.core.storage.PartitionedDataStore;
import com.alibaba.flink.shuffle.core.storage.ReadingViewContext;
import com.alibaba.flink.shuffle.core.storage.StorageMeta;
import com.alibaba.flink.shuffle.core.storage.StorageSpaceInfo;
import com.alibaba.flink.shuffle.core.storage.WritingViewContext;

import javax.annotation.Nullable;

import java.util.Map;

/** An empty partitioned data store used for tests. */
public class NoOpPartitionedDataStore implements PartitionedDataStore {

    @Override
    public DataPartitionWritingView createDataPartitionWritingView(WritingViewContext context)
            throws Exception {
        return null;
    }

    @Override
    public DataPartitionReadingView createDataPartitionReadingView(ReadingViewContext context)
            throws Exception {
        return null;
    }

    @Override
    public boolean isDataPartitionConsumable(DataPartitionMeta partitionMeta) {
        return false;
    }

    @Override
    public void addDataPartition(DataPartitionMeta partitionMeta) throws Exception {}

    @Override
    public void removeDataPartition(DataPartitionMeta partitionMeta) {}

    @Override
    public void releaseDataPartition(
            DataSetID dataSetID, DataPartitionID partitionID, @Nullable Throwable throwable) {}

    @Override
    public void releaseDataSet(DataSetID dataSetID, @Nullable Throwable throwable) {}

    @Override
    public void releaseDataByJobID(JobID jobID, @Nullable Throwable throwable) {}

    @Override
    public void updateUsedStorageSpace() {}

    @Override
    public DataStoreStatistics getDataStoreStatistics() {
        return DataStoreStatistics.EMPTY_DATA_STORE_STATISTICS;
    }

    @Override
    public void shutDown(boolean releaseData) {}

    @Override
    public boolean isShutDown() {
        return false;
    }

    @Override
    public Configuration getConfiguration() {
        return null;
    }

    @Override
    public BufferDispatcher getWritingBufferDispatcher() {
        return null;
    }

    @Override
    public BufferDispatcher getReadingBufferDispatcher() {
        return null;
    }

    @Override
    public SingleThreadExecutorPool getExecutorPool(StorageMeta storageMeta) {
        return null;
    }

    @Override
    public void updateFreeStorageSpace() {}

    @Override
    public void updateStorageHealthStatus() {}

    @Override
    public Map<String, StorageSpaceInfo> getStorageSpaceInfos() {
        return null;
    }
}
