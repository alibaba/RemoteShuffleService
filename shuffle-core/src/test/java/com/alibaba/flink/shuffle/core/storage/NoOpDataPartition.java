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
import com.alibaba.flink.shuffle.core.ids.DataPartitionID;
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.core.listener.BacklogListener;
import com.alibaba.flink.shuffle.core.listener.DataListener;
import com.alibaba.flink.shuffle.core.listener.DataRegionCreditListener;
import com.alibaba.flink.shuffle.core.listener.FailureListener;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

/** No-op {@link DataPartition} implementation for tests. */
public class NoOpDataPartition implements DataPartition {

    private final DataPartitionMeta partitionMeta;

    public NoOpDataPartition(JobID jobID, DataSetID dataSetID, DataPartitionID partitionID) {
        CommonUtils.checkArgument(jobID != null);
        CommonUtils.checkArgument(dataSetID != null);
        CommonUtils.checkArgument(partitionID != null);

        this.partitionMeta =
                new TestDataPartitionMeta(
                        jobID,
                        dataSetID,
                        partitionID,
                        new StorageMeta("/tmp", StorageType.SSD, "/tmp"));
    }

    @Override
    public DataPartitionMeta getPartitionMeta() {
        return partitionMeta;
    }

    @Override
    public DataPartitionType getPartitionType() {
        return null;
    }

    @Override
    public DataPartitionWriter createPartitionWriter(
            MapPartitionID mapPartitionID,
            DataRegionCreditListener dataRegionCreditListener,
            FailureListener failureListener) {
        return null;
    }

    @Override
    public DataPartitionReader createPartitionReader(
            int startPartitionIndex,
            int endPartitionIndex,
            DataListener dataListener,
            BacklogListener backlogListener,
            FailureListener failureListener) {
        return null;
    }

    @Override
    public CompletableFuture<?> releasePartition(@Nullable Throwable releaseCause) {
        return null;
    }

    @Override
    public boolean isConsumable() {
        return false;
    }

    @Override
    public DataPartitionStatistics getDataPartitionStatistics() {
        return null;
    }
}
