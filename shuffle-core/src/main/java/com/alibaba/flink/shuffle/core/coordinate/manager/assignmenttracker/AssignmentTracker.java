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

package com.alibaba.flink.shuffle.core.coordinate.manager.assignmenttracker;

import com.alibaba.flink.shuffle.core.coordinate.ChangedWorkerStatus;
import com.alibaba.flink.shuffle.core.coordinate.DataPartitionCoordinate;
import com.alibaba.flink.shuffle.core.coordinate.DataPartitionStatus;
import com.alibaba.flink.shuffle.core.coordinate.StorageSpaceInfo;
import com.alibaba.flink.shuffle.core.coordinate.manager.ShuffleResource;
import com.alibaba.flink.shuffle.core.coordinate.worker.ShuffleWorkerGateway;
import com.alibaba.flink.shuffle.core.ids.DataPartitionID;
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.InstanceID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.core.ids.RegistrationID;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/** Component tracks the assignment of data partitions on shuffle workers. */
public interface AssignmentTracker {

    /**
     * Whether a shuffle worker is registered.
     *
     * @param registrationID The registration ID of the shuffle worker.
     * @return Whether the shuffle worker is registered.
     */
    boolean isWorkerRegistered(RegistrationID registrationID);

    /**
     * Registers a shuffle worker on worker connected to the manager.
     *
     * @param workerID The ID of the shuffle worker.
     * @param registrationID The ID of the shuffle worker registration.
     * @param gateway The gateway of the shuffle worker.
     * @param externalAddress The address for the tasks to connect to.
     * @param dataPort The port of the shuffle worker to provide data partition read/write service.
     */
    void registerWorker(
            InstanceID workerID,
            RegistrationID registrationID,
            ShuffleWorkerGateway gateway,
            String externalAddress,
            int dataPort);

    /**
     * Worker initiates releasing one data partition, like when the data has failure during writing.
     * On calling of this method, the data should has been removed on the worker side and one piece
     * of meta is kept for synchronizing the status with manager.
     *
     * @param workerRegistrationID The registration ID of the shuffle worker.
     * @param jobID The id of the job produces the data partition.
     * @param dataSetID The id of the dataset that contains the data partition..
     * @param dataPartitionID The id of the data partition.
     */
    void workerReportDataPartitionReleased(
            RegistrationID workerRegistrationID,
            JobID jobID,
            DataSetID dataSetID,
            DataPartitionID dataPartitionID);

    /**
     * Synchronizes the list of data partition on a shuffle worker. The caller of this method should
     * ensures all the reported jobs has been registered.
     *
     * @param workerRegistrationID The registration ID of the shuffle worker.
     * @param dataPartitionStatuses The statuses of data partitions residing on the shuffle worker.
     */
    void synchronizeWorkerDataPartitions(
            RegistrationID workerRegistrationID, List<DataPartitionStatus> dataPartitionStatuses);

    /**
     * Unregisters a shuffle worker when disconnected with a worker.
     *
     * @param workerRegistrationID The registration ID of the shuffle worker.
     */
    void unregisterWorker(RegistrationID workerRegistrationID);

    /**
     * Whether a job client is registered.
     *
     * @param jobID The job id.
     * @return Whether the job client is registered.
     */
    boolean isJobRegistered(JobID jobID);

    /**
     * Registers a job client.
     *
     * @param jobID The job id.
     */
    void registerJob(JobID jobID);

    /**
     * Unregisters a job client.
     *
     * @param jobID The job id.
     */
    void unregisterJob(JobID jobID);

    /**
     * Requests resources for all the data partitions produced by one map task in one dataset.
     *
     * @param jobID The job id.
     * @param dataSetID The id of the dataset that contains this partition.
     * @param mapPartitionID The id represents the map task.
     * @param numberOfConsumers The number of consumers of the partition.
     * @param consumerGroupID The id of consumer vertex gourp of the partition.
     * @param dataPartitionFactoryName The factory name of the data partition.
     * @param taskLocation The location (host name) of the target task requesting resources.
     * @return The allocated shuffle resources.
     */
    ShuffleResource requestShuffleResource(
            JobID jobID,
            DataSetID dataSetID,
            MapPartitionID mapPartitionID,
            int numberOfConsumers,
            long consumerGroupID,
            String dataPartitionFactoryName,
            String taskLocation);

    /**
     * Client releases resources for all the data partitions produced by one map task in one
     * dataset.
     *
     * @param jobID The job id.
     * @param dataSetID The id of the dataset that contains this partition.
     * @param mapPartitionID The id represents the map task.
     */
    void releaseShuffleResource(JobID jobID, DataSetID dataSetID, MapPartitionID mapPartitionID);

    /**
     * Computes the list of workers that get changed for the specified job.
     *
     * @param cachedWorkerList The current cached worker list for this job.
     * @param considerUnrelatedWorkers Whether to also consider the unrelated workers.
     * @return The status of changed workers.
     */
    ChangedWorkerStatus computeChangedWorkers(
            JobID jobID, Collection<InstanceID> cachedWorkerList, boolean considerUnrelatedWorkers);

    /**
     * Lists the registered jobs.
     *
     * @return The list of current jobs.
     */
    List<JobID> listJobs();

    /**
     * Gets the number of workers.
     *
     * @return The number of current workers.
     */
    int getNumberOfWorkers();

    /**
     * Gets the data partition distribution for a specific job.
     *
     * @param jobID the id of the job to acquire.
     * @return The data partition distribution.
     */
    Map<DataPartitionCoordinate, InstanceID> getDataPartitionDistribution(JobID jobID);

    /** Reports the storage space information of the corresponding worker. */
    void reportWorkerStorageSpaces(
            InstanceID instanceID,
            RegistrationID shuffleWorkerRegisterId,
            Map<String, StorageSpaceInfo> storageSpaceInfos);
}
