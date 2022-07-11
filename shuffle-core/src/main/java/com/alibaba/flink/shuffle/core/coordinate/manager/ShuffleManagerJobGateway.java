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

package com.alibaba.flink.shuffle.core.coordinate.manager;

import com.alibaba.flink.shuffle.core.coordinate.Acknowledge;
import com.alibaba.flink.shuffle.core.coordinate.ManagerToJobHeartbeatPayload;
import com.alibaba.flink.shuffle.core.coordinate.RegistrationResponse;
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.InstanceID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/** The rpc gateway used by a FLINK job to allocate or release a {@link ShuffleResource}. */
public interface ShuffleManagerJobGateway {

    /**
     * Registers a client instance to the ShuffleManager, which represents a job.
     *
     * @param jobID The FLINK job id.
     * @return The response for the registration.
     */
    CompletableFuture<RegistrationResponse> registerClient(JobID jobID, InstanceID clientID);

    /**
     * Unregisters a client instance from the ShuffleManager, which represents a job.
     *
     * @param jobID The job id.
     * @return The response for the registration.
     */
    CompletableFuture<Acknowledge> unregisterClient(JobID jobID, InstanceID clientID);

    /**
     * Receives the heartbeat from the client, which represents a job.
     *
     * @param jobID The job id.
     * @return The InstanceID of the client.
     */
    CompletableFuture<ManagerToJobHeartbeatPayload> heartbeatFromClient(
            JobID jobID, InstanceID clientID, Set<InstanceID> cachedWorkerList);

    /**
     * Requests a shuffle resource for storing all the data partitions produced by one map task in
     * one dataset.
     *
     * @param jobID The job id.
     * @param dataSetID The id of the dataset that contains this partition.
     * @param mapPartitionID The id represents the map task.
     * @param numberOfConsumers The number of consumers of the partition.
     * @param consumerGroupID The id of consumer vertex group of the partition.
     * @param dataPartitionFactoryName The factory name of the data partition.
     * @return The allocated shuffle resources.
     */
    CompletableFuture<ShuffleResource> requestShuffleResource(
            JobID jobID,
            InstanceID clientID,
            DataSetID dataSetID,
            MapPartitionID mapPartitionID,
            int numberOfConsumers,
            long consumerGroupID,
            String dataPartitionFactoryName);

    /**
     * Requests a shuffle resource for storing all the data partitions produced by one map task in
     * one dataset. This method can achieve better data locality based on the map task location.
     * Note that we keep both {@link #requestShuffleResource} methods for compatibility.
     *
     * @param jobID The job id.
     * @param dataSetID The id of the dataset that contains this partition.
     * @param mapPartitionID The id represents the map task.
     * @param numberOfConsumers The number of consumers of the partition.
     * @param consumerGroupID The id of consumer vertex group of the partition.
     * @param dataPartitionFactoryName The factory name of the data partition.
     * @param taskLocation The location (host name) of the target task requesting resources.
     * @return The allocated shuffle resources.
     */
    CompletableFuture<ShuffleResource> requestShuffleResource(
            JobID jobID,
            InstanceID clientID,
            DataSetID dataSetID,
            MapPartitionID mapPartitionID,
            int numberOfConsumers,
            long consumerGroupID,
            String dataPartitionFactoryName,
            @Nullable String taskLocation);

    /**
     * Releases resources for all the data partitions produced by one map task in one dataset.
     *
     * @param jobID The job id.
     * @param dataSetID The id of the dataset that contains this partition.
     * @param mapPartitionID The id represents the map task.
     * @return The result for releasing shuffle resource.
     */
    CompletableFuture<Acknowledge> releaseShuffleResource(
            JobID jobID, InstanceID clientID, DataSetID dataSetID, MapPartitionID mapPartitionID);

    /**
     * Gets num of registered shuffle workers.
     *
     * @return the num of registered shuffle workers.
     */
    CompletableFuture<Integer> getNumberOfRegisteredWorkers();

    CompletableFuture<List<JobID>> listJobs();
}

