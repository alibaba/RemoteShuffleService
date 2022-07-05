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
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.core.ids.ReducePartitionID;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** {@link DataPartitionMeta} of {@link ReducePartition}. */
public abstract class ReducePartitionMeta extends DataPartitionMeta {

    private static final long serialVersionUID = -7617298646112796354L;

    protected final ReducePartitionID partitionID;

    protected final Set<MapPartitionID> mapPartitionIDS = new HashSet<>();

    public ReducePartitionMeta(
            JobID jobID,
            DataSetID dataSetID,
            ReducePartitionID partitionID,
            StorageMeta storageMeta) {
        super(jobID, dataSetID, storageMeta);

        CommonUtils.checkArgument(partitionID != null, "Must be not null.");
        this.partitionID = partitionID;
    }

    @Override
    public ReducePartitionID getDataPartitionID() {
        return partitionID;
    }

    @Override
    public List<MapPartitionID> getMapPartitionIDs() {
        return new ArrayList<>(mapPartitionIDS);
    }
}
