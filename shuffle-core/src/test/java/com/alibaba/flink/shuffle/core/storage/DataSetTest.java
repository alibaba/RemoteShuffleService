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

import com.alibaba.flink.shuffle.common.exception.ShuffleException;
import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.core.ids.DataPartitionID;
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;

import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Tests for {@link DataSet}. */
public class DataSetTest {

    private static final JobID JOB_ID = new JobID(CommonUtils.randomBytes(16));

    private static final DataSetID DATA_SET_ID = new DataSetID(CommonUtils.randomBytes(16));

    @Test
    public void testAddDataPartition() {
        int numDataPartitions = 10;
        DataSet dataSet = new DataSet(JOB_ID, DATA_SET_ID);
        Set<DataPartitionID> partitionIDS = addDataPartitions(dataSet, numDataPartitions);

        assertEquals(numDataPartitions, dataSet.getNumDataPartitions());
        assertEquals(partitionIDS, dataSet.getDataPartitionIDs());
    }

    @Test(expected = ShuffleException.class)
    public void testAddExistingDataPartition() {
        DataSet dataSet = new DataSet(JOB_ID, DATA_SET_ID);
        DataPartitionID partitionID = new MapPartitionID(CommonUtils.randomBytes(16));
        DataPartition dataPartition = new NoOpDataPartition(JOB_ID, DATA_SET_ID, partitionID);

        dataSet.addDataPartition(dataPartition);
        dataSet.addDataPartition(dataPartition);
    }

    @Test
    public void testGetDataPartition() {
        int numDataPartitions = 10;
        DataSet dataSet = new DataSet(JOB_ID, DATA_SET_ID);
        Set<DataPartitionID> partitionIDS = addDataPartitions(dataSet, numDataPartitions);

        for (DataPartitionID partitionID : partitionIDS) {
            assertNotNull(dataSet.getDataPartition(partitionID));
        }
        assertNull(dataSet.getDataPartition(new MapPartitionID(CommonUtils.randomBytes(16))));
    }

    @Test
    public void testRemoveDataPartition() {
        int numDataPartitions = 10;
        DataSet dataSet = new DataSet(JOB_ID, DATA_SET_ID);
        Set<DataPartitionID> partitionIDS = addDataPartitions(dataSet, numDataPartitions);

        dataSet.removeDataPartition(new MapPartitionID(CommonUtils.randomBytes(16)));
        assertEquals(numDataPartitions, dataSet.getNumDataPartitions());

        int count = 0;
        for (DataPartitionID partitionID : partitionIDS) {
            ++count;
            dataSet.removeDataPartition(partitionID);
            assertEquals(numDataPartitions - count, dataSet.getNumDataPartitions());
        }
    }

    @Test
    public void testClearDataPartition() {
        int numDataPartitions = 10;
        DataSet dataSet = new DataSet(JOB_ID, DATA_SET_ID);
        Set<DataPartitionID> partitionIDS = addDataPartitions(dataSet, numDataPartitions);

        List<DataPartition> dataPartitions = dataSet.clearDataPartitions();
        for (DataPartition dataPartition : dataPartitions) {
            assertTrue(
                    partitionIDS.contains(dataPartition.getPartitionMeta().getDataPartitionID()));
        }
        assertEquals(0, dataSet.getNumDataPartitions());
    }

    private Set<DataPartitionID> addDataPartitions(DataSet dataSet, int numDataPartitions) {
        Set<DataPartitionID> partitionIDS = new HashSet<>();
        for (int i = 0; i < numDataPartitions; ++i) {
            DataPartitionID partitionID = new MapPartitionID(CommonUtils.randomBytes(16));
            partitionIDS.add(partitionID);
            dataSet.addDataPartition(new NoOpDataPartition(JOB_ID, DATA_SET_ID, partitionID));
        }
        return partitionIDS;
    }
}
