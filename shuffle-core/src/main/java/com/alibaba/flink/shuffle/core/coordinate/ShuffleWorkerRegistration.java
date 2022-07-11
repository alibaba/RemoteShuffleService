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

package com.alibaba.flink.shuffle.core.coordinate;

import com.alibaba.flink.shuffle.core.ids.InstanceID;

import java.io.Serializable;
import java.util.Map;

/** Information provided by the ShuffleWorker when it registers to the ShuffleManager. */
public class ShuffleWorkerRegistration implements Serializable {

    /** The rpc address of the ShuffleWorker that registers. */
    private String rpcAddress;

    /** The hostname of the ShuffleWorker that registers. */
    private  String hostname;

    /** The resource id of the ShuffleWorker that registers. */
    private  InstanceID workerID;

    /** The port used for data transfer. */
    private int dataPort;

    /** The process id of the shuffle worker. Currently, it is only used in e2e tests. */
    private  int processID;

    /** The storage space information indexed by partition factory name. */
    private Map<String, StorageSpaceInfo> storageSpaceInfos;
}
