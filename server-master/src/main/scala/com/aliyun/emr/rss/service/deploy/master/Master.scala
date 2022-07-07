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

package com.aliyun.emr.rss.service.deploy.master

import java.util

import com.aliyun.emr.rss.common.RssConf
import com.aliyun.emr.rss.common.meta.WorkerInfo
import com.aliyun.emr.rss.common.protocol.message.ControlMessages._
import com.aliyun.emr.rss.common.rpc._

private[deploy] class Master(
    override val rpcEnv: RpcEnv,
    address: RpcAddress,
    val conf: RssConf)
  extends RpcEndpoint {

  // start threads to check timeout for workers and applications
  override def onStart(): Unit = {
  }

  override def onStop(): Unit = {
  }

  override def onDisconnected(address: RpcAddress): Unit = {
  }

  override def receive: PartialFunction[Any, Unit] = {
    case CheckForWorkerTimeOut =>
    case CheckForApplicationTimeOut =>
    case WorkerLost(host, rpcPort, pushPort, fetchPort, replicatePort, requestId) =>
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case HeartBeatFromApplication(appId, requestId) =>

    case RegisterWorker(host, rpcPort, pushPort, fetchPort, replicatePort, numSlots, requestId) =>

    case requestSlots @ RequestSlots(_, _, _, _, _, _) =>

    case ReleaseSlots(applicationId, shuffleId, workerIds, slots, requestId) =>

    case UnregisterShuffle(applicationId, shuffleId, requestId) =>

    case msg: GetBlacklist =>

    case ApplicationLost(appId, requestId) =>

    case HeartbeatFromWorker(host, rpcPort, pushPort, fetchPort, replicatePort, numSlots,
    shuffleKeys, requestId) =>

    case GetWorkerInfos =>

    case ReportWorkerFailure(failedWorkers: util.List[WorkerInfo], requestId: String) =>

    case GetClusterLoadStatus(numPartitions: Int) =>
  }
}

private[deploy] object Master {
  def main(args: Array[String]): Unit = {

  }
}
