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
import com.alibaba.flink.shuffle.core.ids.ReducePartitionID;
import com.alibaba.flink.shuffle.core.listener.BacklogListener;
import com.alibaba.flink.shuffle.core.listener.DataListener;
import com.alibaba.flink.shuffle.core.listener.FailureListener;

/** Context used to create {@link DataPartitionReadingView}. */
public class ReadingViewContext {

    /** ID of the {@link DataPartition} to read data from. */
    private final DataPartitionID partitionID;

    /** ID of the {@link DataSet} to which the {@link DataPartition} belongs. */
    private final DataSetID dataSetID;

    /** Index of the first logic {@link ReducePartition} to be read (inclusive). */
    private final int startPartitionIndex;

    /** Index of the last logic {@link ReducePartition} to be read (inclusive). */
    private final int endPartitionIndex;

    /** Listener to be notified when there is any data available for reading. */
    private final DataListener dataListener;

    /** Listener to be notified when there is any backlog available in the reading view. */
    private final BacklogListener backlogListener;

    /** Listener to be notified when any internal exception occurs. */
    private final FailureListener failureListener;

    public ReadingViewContext(
            DataSetID dataSetID,
            DataPartitionID partitionID,
            int startPartitionIndex,
            int endPartitionIndex,
            DataListener dataListener,
            BacklogListener backlogListener,
            FailureListener failureListener) {
        CommonUtils.checkArgument(dataSetID != null, "Must be not null.");
        CommonUtils.checkArgument(partitionID != null, "Must be not null.");
        CommonUtils.checkArgument(startPartitionIndex >= 0, "Must be non-negative.");
        CommonUtils.checkArgument(endPartitionIndex >= startPartitionIndex, "Illegal index range.");
        CommonUtils.checkArgument(dataListener != null, "Must be not null.");
        CommonUtils.checkArgument(backlogListener != null, "Must be not null.");
        CommonUtils.checkArgument(failureListener != null, "Must be not null.");

        if (partitionID.getPartitionType() == DataPartition.DataPartitionType.REDUCE_PARTITION) {
            ReducePartitionID reducePartitionID = (ReducePartitionID) partitionID;
            CommonUtils.checkArgument(
                    reducePartitionID.getPartitionIndex() == endPartitionIndex
                            && reducePartitionID.getPartitionIndex() == startPartitionIndex,
                    "Illegal reduce partition index range.");
        }

        this.partitionID = partitionID;
        this.dataSetID = dataSetID;
        this.startPartitionIndex = startPartitionIndex;
        this.endPartitionIndex = endPartitionIndex;
        this.dataListener = dataListener;
        this.backlogListener = backlogListener;
        this.failureListener = failureListener;
    }

    public DataPartitionID getPartitionID() {
        return partitionID;
    }

    public DataSetID getDataSetID() {
        return dataSetID;
    }

    public int getStartPartitionIndex() {
        return startPartitionIndex;
    }

    public int getEndPartitionIndex() {
        return endPartitionIndex;
    }

    public DataListener getDataListener() {
        return dataListener;
    }

    public BacklogListener getBacklogListener() {
        return backlogListener;
    }

    public FailureListener getFailureListener() {
        return failureListener;
    }
}
