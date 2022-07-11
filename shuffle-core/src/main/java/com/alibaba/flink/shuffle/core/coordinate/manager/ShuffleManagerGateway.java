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
import com.alibaba.flink.shuffle.core.coordinate.DataPartitionStatus;
import com.alibaba.flink.shuffle.core.coordinate.RegistrationResponse;
import com.alibaba.flink.shuffle.core.coordinate.ShuffleWorkerRegistration;
import com.alibaba.flink.shuffle.core.coordinate.WorkerToManagerHeartbeatPayload;
import com.alibaba.flink.shuffle.core.ids.DataPartitionID;
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.InstanceID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.ids.RegistrationID;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/** The shuffle manager rpc gateway. */
public interface ShuffleManagerGateway extends ShuffleManagerJobGateway {

    /**
     * Registers a shuffle worker to the manager.
     *
     * @param workerRegistration The information of the shuffle worker.
     * @return The response of the registration.
     */
    CompletableFuture<RegistrationResponse> registerWorker(
            ShuffleWorkerRegistration workerRegistration);

    /**
     * Worker initiates releasing one data partition, like when the data has failure during writing.
     * On calling of this method, the data should have been removed on the worker side and one piece
     * of meta is kept for synchronizing the status with manager.
     *
     * @param jobID The id of the job produces the data partition.
     * @param dataSetID The id of the dataset that contains the data partition..
     * @param dataPartitionID The id of the data partition.
     */
    CompletableFuture<Acknowledge> workerReportDataPartitionReleased(
            InstanceID workerID,
            RegistrationID registrationID,
            JobID jobID,
            DataSetID dataSetID,
            DataPartitionID dataPartitionID);

    /**
     * Reports the list of data partitions in the current worker.
     *
     * @param workerID The InstanceID of the shuffle worker.
     * @param registrationID The InstanceId of the shuffle worker.
     * @param dataPartitionStatuses The list of data partitions in the shuffle worker.
     * @return Whether the report is success.
     */
    CompletableFuture<Acknowledge> reportDataPartitionStatus(
            InstanceID workerID,
            RegistrationID registrationID,
            List<DataPartitionStatus> dataPartitionStatuses);

    /**
     * Receives heartbeat from the shuffle worker.
     *
     * @param workerID The InstanceID of the worker.
     * @param payload The status of the current worker.
     */
    void heartbeatFromWorker(InstanceID workerID, WorkerToManagerHeartbeatPayload payload);

    /**
     * Disconnects the shuffle worker from the manager.
     *
     * @param workerID The InstanceID of the shuffle worker.
     * @param cause The reason of disconnect.
     * @return Whether the disconnect is success.
     */
    CompletableFuture<Acknowledge> disconnectWorker(InstanceID workerID, Exception cause);
}
