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
import com.alibaba.flink.shuffle.core.exception.DuplicatedPartitionException;
import com.alibaba.flink.shuffle.core.ids.DataPartitionID;
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.JobID;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * {@link DataSet} is a collection of {@link DataPartition}s which have the same {@link DataSetID}.
 * For example, {@link DataPartition}s produced by different parallel tasks of the same computation
 * vertex can have the same {@link DataSetID} and belong to the same {@link DataSet}.
 */
@NotThreadSafe
public class DataSet {

    /** All {@link DataPartition}s in this dataset. */
    private final HashMap<DataPartitionID, DataPartition> dataPartitions;

    /** ID of the job which produced this dataset. */
    private final JobID jobID;

    /** ID of this dataset. */
    private final DataSetID dataSetID;

    public DataSet(JobID jobID, DataSetID dataSetID) {
        CommonUtils.checkArgument(jobID != null, "Must be not null.");
        CommonUtils.checkArgument(dataSetID != null, "Must be not null.");

        this.jobID = jobID;
        this.dataSetID = dataSetID;
        this.dataPartitions = new HashMap<>();
    }

    /** Returns the {@link DataPartition} with the given {@link DataPartitionID}. */
    public DataPartition getDataPartition(DataPartitionID partitionID) {
        CommonUtils.checkArgument(partitionID != null, "Must be not null.");

        return dataPartitions.get(partitionID);
    }

    /**
     * Adds the given {@link DataPartition} to this dataset. Exception will be thrown if the {@link
     * DataPartition} with the same {@link DataPartitionID} already exists in this dataset.
     */
    public void addDataPartition(DataPartition dataPartition) {
        CommonUtils.checkArgument(dataPartition != null, "Must be not null.");

        DataPartitionID partitionID = dataPartition.getPartitionMeta().getDataPartitionID();
        if (dataPartitions.containsKey(partitionID)) {
            throw new DuplicatedPartitionException(dataPartition.getPartitionMeta().toString());
        }
        dataPartitions.put(partitionID, dataPartition);
    }

    public JobID getJobID() {
        return jobID;
    }

    public DataSetID getDataSetID() {
        return dataSetID;
    }

    /** Returns the number of {@link DataPartition}s in this dataset. */
    public int getNumDataPartitions() {
        return dataPartitions.size();
    }

    /**
     * Returns true if this dataset contains the target {@link DataPartition} of the given {@link
     * DataPartitionID}.
     */
    public boolean containsDataPartition(DataPartitionID dataPartitionID) {
        return dataPartitions.containsKey(dataPartitionID);
    }

    /**
     * Removes the {@link DataPartition} with the given {@link DataPartitionID} from this dataset.
     */
    public DataPartition removeDataPartition(DataPartitionID partitionID) {
        CommonUtils.checkArgument(partitionID != null, "Must be not null.");

        return dataPartitions.remove(partitionID);
    }

    /**
     * Removes all {@link DataPartition}s in this dataset and returns a list of the cleared {@link
     * DataPartition}s.
     */
    public List<DataPartition> clearDataPartitions() {
        List<DataPartition> partitions = new ArrayList<>(dataPartitions.values());
        dataPartitions.clear();
        return partitions;
    }

    /** Returns all {@link DataPartition}s belonging to this dataset. */
    public ArrayList<DataPartition> getDataPartitions() {
        return new ArrayList<>(dataPartitions.values());
    }

    @Override
    public String toString() {
        return "DataSet{"
                + "JobID="
                + jobID
                + ", DataSetID="
                + dataSetID
                + ", NumDataPartitions="
                + dataPartitions.size()
                + '}';
    }

    // ---------------------------------------------------------------------------------------------
    // For test
    // ---------------------------------------------------------------------------------------------

    public Set<DataPartitionID> getDataPartitionIDs() {
        return new HashSet<>(dataPartitions.keySet());
    }
}
