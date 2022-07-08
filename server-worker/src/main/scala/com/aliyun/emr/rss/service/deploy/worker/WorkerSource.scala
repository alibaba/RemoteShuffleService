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
import com.aliyun.emr.rss.common.metrics.source.{AbstractSource, JVMCPUSource, JVMSource, NetWorkSource}

class WorkerSource(conf: RssConf, metricsSystem: MetricsSystem)
  extends AbstractSource(conf, MetricsSystem.ROLE_WOKRER) with Logging {
  override val sourceName = "worker"

  import WorkerSource._
  // add counters
  addCounter(PUSH_DATA_FAIL_COUNT)

  // add Timers
  addTimer(COMMIT_FILE_TIME)
  addTimer(RESERVE_SLOTS_TIME)
  addTimer(FLUSH_DATA_TIME)
  addTimer(MASTER_PUSH_DATA_TIME)
  addTimer(SLAVE_PUSH_DATA_TIME)

  addTimer(FETCH_CHUNK_TIME)
  addTimer(OPEN_STREAM_TIME)
  addTimer(TAKE_BUFFER_TIME)
  addTimer(SORT_TIME)

  // start cleaner thread
  startCleaner()

  metricsSystem.registerSource(this)
  metricsSystem.registerSource(new NetWorkSource(conf, MetricsSystem.ROLE_WOKRER))
  metricsSystem.registerSource(new JVMSource(conf, MetricsSystem.ROLE_WOKRER))
  metricsSystem.registerSource(new JVMCPUSource(conf, MetricsSystem.ROLE_WOKRER))
}

object WorkerSource {
  val SERVLET_PATH = "/metrics/prometheus"

  val COMMIT_FILE_TIME = "CommitFilesTime"
  val RESERVE_SLOTS_TIME = "ReserveSlotsTime"
  val FLUSH_DATA_TIME = "FlushDataTime"
  val OPEN_STREAM_TIME = "OpenStreamTime"
  val FETCH_CHUNK_TIME = "FetchChunkTime"


  // push data time
  val MASTER_PUSH_DATA_TIME = "MasterPushDataTime"
  val SLAVE_PUSH_DATA_TIME = "SlavePushDataTime"

  // push data
  val PUSH_DATA_FAIL_COUNT = "PushDataFailCount"
  val PAUSE_PUSH_DATA_COUNT = "PausePushData"
  val PAUSE_PUSH_DATA_AND_REPLICATE_COUNT = "PausePushDataAndReplicate"

  // flush
  val TAKE_BUFFER_TIME = "TakeBufferTime"
  val DISK_BUFFER = "DiskBuffer"

  val REGISTERED_SHUFFLE_COUNT = "RegisteredShuffleCount"

  // slots
  val TOTAL_SLOTS = "TotalSlots"
  val USED_SLOTS = "SlotsUsed"
  val AVAILABLE_SLOTS = "SlotsAvailable"

  // memory using
  val NETTY_MEMORY = "NettyMemory"
  val SORT_TIME = "SortTime"
  val SORT_MEMORY = "SortMemory"
  val SORTING_FILES = "SortingFiles"
}
