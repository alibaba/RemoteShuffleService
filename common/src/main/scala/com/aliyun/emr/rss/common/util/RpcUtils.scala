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

package com.aliyun.emr.rss.common.util

import com.aliyun.emr.rss.common.RssConf
import com.aliyun.emr.rss.common.rpc.RpcTimeout

private[rss] object RpcUtils {

  /** Returns the configured number of times to retry connecting */
  def numRetries(conf: RssConf): Int = {
    conf.getInt("rss.rpc.numRetries", 3)
  }

  /** Returns the configured number of milliseconds to wait on each retry */
  def retryWaitMs(conf: RssConf): Long = {
    conf.getTimeAsMs("rss.rpc.retry.wait", "3s")
  }

  /** Returns the default Spark timeout to use for RPC ask operations. */
  def askRpcTimeout(conf: RssConf): RpcTimeout = {
    RpcTimeout(conf, "rss.network.timeout", "240s")
  }

  /** Returns the default Spark timeout to use for RPC remote endpoint lookup. */
  def lookupRpcTimeout(conf: RssConf): RpcTimeout = {
    RpcTimeout(conf, "rss.rpc.lookupTimeout", "60s")
  }

  /** Returns the executor register shuffle timeout to use for RPC ask operations. */
  def registerShuffleRpcTimeout(conf: RssConf): RpcTimeout = {
    RpcTimeout(conf, "rss.registerShuffle.timeout", "240s")
  }

  /** Returns the executor revive timeout to use for RPC ask operations. */
  def reviveRpcTimeout(conf: RssConf): RpcTimeout = {
    RpcTimeout(conf, "rss.revive.timeout", "240s")
  }

  /** Returns the executor mapperEnd timeout to use for RPC ask operations. */
  def mapperEndRpcTimeout(conf: RssConf): RpcTimeout = {
    RpcTimeout(conf, "rss.mapperEnd.timeout", "240s")
  }

  /** Returns the executor getReducerFileGroup timeout to use for RPC ask operations. */
  def getReducerFileGroupResponseRpcTimeout(conf: RssConf): RpcTimeout = {
    RpcTimeout(conf, "rss.getReducerFileGroup.timeout", "240s")
  }

  /** Returns the master RequestSlots timeout to use for RPC ask operations. */
  def requestSlotsRpcTimeout(conf: RssConf): RpcTimeout = {
    RpcTimeout(conf, "rss.requestSlots.timeout", "30s")
  }

  /** Returns the driver ReserveSlots timeout to use for RPC ask operations. */
  def reserveSlotsRpcTimeout(conf: RssConf): RpcTimeout = {
    RpcTimeout(conf, "rss.reserveSlots.timeout", "30s")
  }

  /** Returns the master RequestSlots timeout to use for RPC ask operations. */
  def releaseSlotsRpcTimeout(conf: RssConf): RpcTimeout = {
    RpcTimeout(conf, "rss.releaseSlots.timeout", "30s")
  }

  /** Returns the driver destroy timeout to use for RPC ask operations. */
  def destroyRpcTimeout(conf: RssConf): RpcTimeout = {
    RpcTimeout(conf, "rss.destroy.timeout", "30s")
  }

  /** Returns the driver commitFiles timeout to use for RPC ask operations. */
  def commitFilesRpcTimeout(conf: RssConf): RpcTimeout = {
    RpcTimeout(conf, "rss.commitFiles.timeout", "60s")
  }

  /** Returns the master applicationLost timeout to use for RPC ask operations. */
  def applicationLostRpcTimeout(conf: RssConf): RpcTimeout = {
    RpcTimeout(conf, "rss.applicationLost.timeout", "120s")
  }

  /** Returns the master GetClusterLoadStatus timeout to use for RPC ask operations. */
  def getClusterLoadStatusTimeout(conf: RssConf): RpcTimeout = {
    RpcTimeout(conf, "rss.getClusterLoadStatus.timeout", "30s")
  }

  private val MAX_MESSAGE_SIZE_IN_MB = Int.MaxValue / 1024 / 1024

  /** Returns the configured max message size for messages in bytes. */
  def maxMessageSizeBytes(conf: RssConf): Int = {
    val maxSizeInMB = conf.getInt("rss.rpc.message.maxSize", 128)
    if (maxSizeInMB > MAX_MESSAGE_SIZE_IN_MB) {
      throw new IllegalArgumentException(
        s"rss.rpc.message.maxSize should not be greater than $MAX_MESSAGE_SIZE_IN_MB MB")
    }
    maxSizeInMB * 1024 * 1024
  }

}
