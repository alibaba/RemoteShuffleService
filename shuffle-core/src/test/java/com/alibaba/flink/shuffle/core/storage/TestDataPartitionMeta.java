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

import java.io.DataOutput;
import java.util.List;

/** A {@link DataPartitionMeta} implementation for tests. */
public class TestDataPartitionMeta extends DataPartitionMeta {

    private final DataPartitionID partitionID;

    public TestDataPartitionMeta(
            JobID jobID,
            DataSetID dataSetID,
            DataPartitionID partitionID,
            StorageMeta storageMeta) {
        super(jobID, dataSetID, storageMeta);

        CommonUtils.checkArgument(partitionID != null);
        this.partitionID = partitionID;
    }

    @Override
    public DataPartitionID getDataPartitionID() {
        return partitionID;
    }

    @Override
    public String getPartitionFactoryClassName() {
        return null;
    }

    @Override
    public List<MapPartitionID> getMapPartitionIDs() {
        return null;
    }

    @Override
    public void writeTo(DataOutput dataOutput) {}
}
