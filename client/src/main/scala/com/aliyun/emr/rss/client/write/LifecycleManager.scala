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

package com.aliyun.emr.rss.client.write

import com.aliyun.emr.rss.common.RssConf
import com.aliyun.emr.rss.common.protocol.message.ControlMessages._
import com.aliyun.emr.rss.common.rpc._

class LifecycleManager(appId: String, val conf: RssConf) extends RpcEndpoint {

  override val rpcEnv: RpcEnv = null
  override def onStart(): Unit = {
  }

  override def onStop(): Unit = {
  }

  def getRssMetaServiceHost: String = {
    null
  }

  def getRssMetaServicePort: Int = {
    0
  }

  override def receive: PartialFunction[Any, Unit] = {
    case RemoveExpiredShuffle =>
    case msg: GetBlacklist =>
    case StageEnd(applicationId, shuffleId) =>
    case UnregisterShuffle(applicationId, shuffleId, _) =>
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RegisterShuffle(applicationId, shuffleId, numMappers, numPartitions) =>
    case Revive(applicationId, shuffleId, mapId, attemptId, reduceId, epoch, oldPartition, cause) =>
    case PartitionSplit(applicationId, shuffleId, reduceId, epoch, oldPartition) =>
    case MapperEnd(applicationId, shuffleId, mapId, attemptId, numMappers) =>
    case GetReducerFileGroup(applicationId: String, shuffleId: Int) =>
    case StageEnd(applicationId, shuffleId) =>
  }
}
