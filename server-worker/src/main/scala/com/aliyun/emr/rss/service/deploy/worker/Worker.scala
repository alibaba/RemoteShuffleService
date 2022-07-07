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

package com.aliyun.emr.rss.service.deploy.worker

import com.aliyun.emr.rss.common.RssConf
import com.aliyun.emr.rss.common.network.client.RpcResponseCallback
import com.aliyun.emr.rss.common.network.protocol.{PushData, PushMergedData}
import com.aliyun.emr.rss.common.network.server.FileInfo
import com.aliyun.emr.rss.common.protocol.message.ControlMessages._
import com.aliyun.emr.rss.common.rpc._

private[deploy] class Worker(
    override val rpcEnv: RpcEnv,
    val conf: RssConf)
  extends RpcEndpoint with PushDataHandler with OpenStreamHandler {

  def updateNumSlots(numSlots: Int): Unit = {
    heartBeatToMaster()
  }

  def heartBeatToMaster(): Unit = {
  }

  override def onStart(): Unit = {
  }

  override def onStop(): Unit = {
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case ReserveSlots(applicationId, shuffleId, masterLocations, slaveLocations, splitThreashold,
    splitMode, storageHint) =>

    case CommitFiles(applicationId, shuffleId, masterIds, slaveIds, mapAttempts) =>

    case GetWorkerInfos =>

    case ThreadDump =>

    case Destroy(shuffleKey, masterLocations, slaveLocations) =>
  }

  override def handlePushData(pushData: PushData, callback: RpcResponseCallback): Unit = {
  }

  override def handlePushMergedData(
      pushMergedData: PushMergedData, callback: RpcResponseCallback): Unit = {
  }

  override def handleOpenStream(shuffleKey: String, fileName: String, startMapIndex: Int,
    endMapIndex: Int): FileInfo = {
    null;
  }
}

private[deploy] object Worker {
  def main(args: Array[String]): Unit = {
  }
}
