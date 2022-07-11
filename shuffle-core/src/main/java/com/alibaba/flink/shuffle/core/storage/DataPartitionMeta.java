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

import java.io.Serializable;

/**
 * Meta information of the {@link DataPartition}. It is supposed to be able to reconstruct the lost
 * {@link DataPartition} from the corresponding {@link DataPartitionMeta}.
 */
public abstract class DataPartitionMeta implements Serializable {

    private static final long serialVersionUID = -5045608362993147450L;

    protected final StorageMeta storageMeta;

    protected final JobID jobID;

    protected final DataSetID dataSetID;

    public DataPartitionMeta(JobID jobID, DataSetID dataSetID, StorageMeta storageMeta) {
        CommonUtils.checkArgument(jobID != null, "Must be not null.");
        CommonUtils.checkArgument(dataSetID != null, "Must be not null.");
        CommonUtils.checkArgument(storageMeta != null, "Must be not null.");

        this.jobID = jobID;
        this.dataSetID = dataSetID;
        this.storageMeta = storageMeta;
    }
}
