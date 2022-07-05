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
import com.alibaba.flink.shuffle.core.listener.DataRegionCreditListener;
import com.alibaba.flink.shuffle.core.listener.FailureListener;

/** Context used to create {@link DataPartitionWritingView}. */
public class WritingViewContext {

    /** ID of the job which is trying to write data. */
    private final JobID jobID;

    /** ID of the {@link DataSet} that the written data belongs to. */
    private final DataSetID dataSetID;

    /** ID of the {@link DataPartition} that the written data belongs to. */
    private final DataPartitionID dataPartitionID;

    /** ID of the logic {@link MapPartition} that the written data belongs to. */
    private final MapPartitionID mapPartitionID;

    /** Number of the {@link ReducePartition}s of the whole {@link DataSet}. */
    private final int numReducePartitions;

    /** Factory class name used to crate the target {@link DataPartition}. */
    private final String partitionFactoryClassName;

    /** Listener to be notified when there are any new credits available. */
    private final DataRegionCreditListener dataRegionCreditListener;

    /** Listener to be notified when any internal exception occurs. */
    private final FailureListener failureListener;

    public WritingViewContext(
            JobID jobID,
            DataSetID dataSetID,
            DataPartitionID dataPartitionID,
            MapPartitionID mapPartitionID,
            int numReducePartitions,
            String partitionFactoryClassName,
            DataRegionCreditListener dataRegionCreditListener,
            FailureListener failureListener) {
        CommonUtils.checkArgument(jobID != null, "Must be not null.");
        CommonUtils.checkArgument(dataSetID != null, "Must be not null.");
        CommonUtils.checkArgument(dataPartitionID != null, "Must be not null.");
        CommonUtils.checkArgument(mapPartitionID != null, "Must be not null.");
        CommonUtils.checkArgument(numReducePartitions > 0, "Must be positive.");
        CommonUtils.checkArgument(partitionFactoryClassName != null, "Must be not null.");
        CommonUtils.checkArgument(dataRegionCreditListener != null, "Must be not null.");
        CommonUtils.checkArgument(failureListener != null, "Must be not null.");

        this.jobID = jobID;
        this.dataSetID = dataSetID;
        this.dataPartitionID = dataPartitionID;
        this.mapPartitionID = mapPartitionID;
        this.numReducePartitions = numReducePartitions;
        this.partitionFactoryClassName = partitionFactoryClassName;
        this.dataRegionCreditListener = dataRegionCreditListener;
        this.failureListener = failureListener;
    }

    public JobID getJobID() {
        return jobID;
    }

    public DataSetID getDataSetID() {
        return dataSetID;
    }

    public DataPartitionID getDataPartitionID() {
        return dataPartitionID;
    }

    public MapPartitionID getMapPartitionID() {
        return mapPartitionID;
    }

    public int getNumReducePartitions() {
        return numReducePartitions;
    }

    public String getPartitionFactoryClassName() {
        return partitionFactoryClassName;
    }

    public DataRegionCreditListener getDataRegionCreditListener() {
        return dataRegionCreditListener;
    }

    public FailureListener getFailureListener() {
        return failureListener;
    }
}
