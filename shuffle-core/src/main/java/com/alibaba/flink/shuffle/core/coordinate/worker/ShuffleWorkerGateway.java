/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.flink.shuffle.core.coordinate.worker;

import com.alibaba.flink.shuffle.core.coordinate.Acknowledge;
import com.alibaba.flink.shuffle.core.ids.DataPartitionID;
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.InstanceID;
import com.alibaba.flink.shuffle.core.ids.JobID;

import java.util.concurrent.CompletableFuture;

/** The rpc gateway for shuffle workers. */
public interface ShuffleWorkerGateway  {

    /**
     * Receives heartbeat from the shuffle manager.
     *
     * @param managerID The InstanceID of the shuffle manager.
     */
    void heartbeatFromManager(InstanceID managerID);

    /**
     * Disconnects the worker from the shuffle manager.
     *
     * @param cause The reason for disconnecting.
     */
    void disconnectManager(Exception cause);

    /**
     * Releases the shuffle resource for one data partition.
     *
     * @param jobID The id of the job produces the data partition.
     * @param dataSetID The id of the dataset that contains the data partition..
     * @param dataPartitionID The id of the data partition.
     * @return The future indicating whether the request is submitted.
     */
    CompletableFuture<Acknowledge> releaseDataPartition(
            JobID jobID, DataSetID dataSetID, DataPartitionID dataPartitionID);

    /**
     * Removes the meta for the already released data partition after the manager has also marked it
     * as releasing.
     *
     * @param jobID The id of the job produces the data partition.
     * @param dataSetID The id of the dataset that contains the data partition..
     * @param dataPartitionID The id of the data partition.
     * @return The future indicating whether the request is submitted.
     */
    CompletableFuture<Acknowledge> removeReleasedDataPartitionMeta(
            JobID jobID, DataSetID dataSetID, DataPartitionID dataPartitionID);
}
