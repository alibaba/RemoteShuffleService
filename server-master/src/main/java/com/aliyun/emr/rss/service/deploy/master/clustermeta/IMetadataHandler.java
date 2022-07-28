/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.emr.rss.service.deploy.master.clustermeta;

import java.util.List;
import java.util.Map;

import com.aliyun.emr.rss.common.meta.DiskInfo;
import com.aliyun.emr.rss.common.meta.WorkerInfo;

public interface IMetadataHandler {
  void handleRequestSlots(
    String shuffleKey,
    String hostName,
    Map<String, Map<String, Integer>> workerToAllocatedSlots,
    String requestId);

  void handleReleaseSlots(
    String shuffleKey,
    List<String> workerIds,
    List<Map<String, Integer>> slotStrings,
    String requestId);

  void handleUnRegisterShuffle(String shuffleKey, String requestId);

  void handleAppHeartbeat(
    String appId,
    long totalWritten,
    long fileCount,
    long time,
    String requestId);

  void handleAppLost(String appId, String requestId);

  void handleWorkerLost(String host, int rpcPort, int pushPort, int fetchPort, int replicatePort,
    String requestId);

  void handleWorkerHeartBeat(String host, int rpcPort, int pushPort, int fetchPort,
    int replicatePort, Map<String, DiskInfo> disks, long time, String requestId);

  void handleRegisterWorker(String host, int rpcPort, int pushPort, int fetchPort,
    int replicatePort, Map<String,DiskInfo> disks, String requestId);

  void handleReportWorkerFailure(List<WorkerInfo> failedNodes, String requestId);

  void handleUpdatePartitionSize();
}
