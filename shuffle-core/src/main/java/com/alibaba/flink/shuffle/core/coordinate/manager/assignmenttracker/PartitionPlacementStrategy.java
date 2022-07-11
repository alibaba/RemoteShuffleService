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


import com.alibaba.flink.shuffle.core.coordinate.DataPartitionCoordinate;
import com.alibaba.flink.shuffle.core.coordinate.DataPartitionStatus;
import com.alibaba.flink.shuffle.core.coordinate.StorageSpaceInfo;
import com.alibaba.flink.shuffle.core.coordinate.worker.ShuffleWorkerGateway;
import com.alibaba.flink.shuffle.core.ids.InstanceID;
import com.alibaba.flink.shuffle.core.ids.RegistrationID;
import com.alibaba.flink.shuffle.core.storage.DataPartitionFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Worker selection strategy for storing the next data partition. */
public interface PartitionPlacementStrategy {

    WorkerStatus[] selectNextWorker(PartitionPlacementContext partitionPlacementContext);


    void addWorker(WorkerStatus worker);

    void removeWorker(WorkerStatus worker);

    /** The status of a shuffle worker. */
    class WorkerStatus {

        private  InstanceID workerID;

        private  RegistrationID registrationID;

        private  ShuffleWorkerGateway gateway;

        private  String dataAddress;

        private  String hostName;

        private  int dataPort;

        private final Map<String, StorageSpaceInfo> storageSpaceInfos = new ConcurrentHashMap<>();

        private final Map<DataPartitionCoordinate, DataPartitionStatus> dataPartitions =
                new HashMap<>();
    }

    class PartitionPlacementContext {
        private Map<RegistrationID, WorkerStatus> currentWorkers;

        private DataPartitionFactory partitionFactory;

        private String taskLocation;

        private int numberOfConsumers;
    }
}
