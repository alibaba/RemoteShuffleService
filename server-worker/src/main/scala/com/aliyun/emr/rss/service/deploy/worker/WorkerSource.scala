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
import com.aliyun.emr.rss.common.internal.Logging
import com.aliyun.emr.rss.common.metrics.MetricsSystem
import com.aliyun.emr.rss.common.metrics.source.AbstractSource

class WorkerSource(essConf: RssConf)
    extends AbstractSource(essConf, MetricsSystem.ROLE_WOKRER) with Logging {
  override val sourceName = "worker"

  import WorkerSource._
  // add counters
  addCounter(PushDataFailCount)

  // add Timers
  addTimer(CommitFilesTime)
  addTimer(ReserveSlotsTime)
  addTimer(FlushDataTime)
  addTimer(MasterPushDataTime)
  addTimer(SlavePushDataTime)

  addTimer(FetchChunkTime)
  addTimer(OpenStreamTime)
  addTimer(TakeBufferTime)
  addTimer(SortTime)

  // start cleaner thread
  startCleaner()
}

object WorkerSource {
  val ServletPath = "/metrics/prometheus"

  val CommitFilesTime = "CommitFilesTime"

  val ReserveSlotsTime = "ReserveSlotsTime"

  val FlushDataTime = "FlushDataTime"

  val OpenStreamTime = "OpenStreamTime"

  val FetchChunkTime = "FetchChunkTime"


  // push data
  val MasterPushDataTime = "MasterPushDataTime"
  val SlavePushDataTime = "SlavePushDataTime"
  val PushDataFailCount = "PushDataFailCount"

  // flush
  val TakeBufferTime = "TakeBufferTime"

  val RegisteredShuffleCount = "RegisteredShuffleCount"

  // slots
  val SlotsUsed = "SlotsUsed"

  // memory
  val NettyMemory = "NettyMemory"
  val SortTime = "SortTime"
  val SortMemory = "SortMemory"
  val SortingFiles = "SortingFiles"
  val DiskBuffer = "DiskBuffer"
  val PausePushDataCount = "PausePushData"
  val PausePushDataAndReplicateCount = "PausePushDataAndReplicate"
}
