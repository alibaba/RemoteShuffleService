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

package org.apache.spark.shuffle.rss

import org.apache.spark.SparkConf

import com.aliyun.emr.rss.client.LifecycleManager
import com.aliyun.emr.rss.common.RssConf
import com.aliyun.emr.rss.common.internal.Logging

class RssShuffleFallbackPolicyRunner(sparkConf: SparkConf) extends Logging {

  private lazy val essConf = RssShuffleManager.fromSparkConf(sparkConf)

  def applyAllFallbackPolicy(lifecycleManager: LifecycleManager, numPartitions: Int): Boolean = {
    applyForceFallbackPolicy() || applyShufflePartitionsFallbackPolicy(numPartitions) ||
      applyClusterLoadFallbackPolicy(lifecycleManager, numPartitions)
  }

  /**
   * if rss.force.fallback is true, fallback to external shuffle
   * @return return rss.force.fallback
   */
  def applyForceFallbackPolicy(): Boolean = RssConf.forceFallback(essConf)

  /**
   * if shuffle partitions > rss.max.partition.number, fallback to external shuffle
   * @param numPartitions shuffle partitions
   * @return return if shuffle partitions bigger than limit
   */
  def applyShufflePartitionsFallbackPolicy(numPartitions: Int): Boolean = {
    val confNumPartitions = RssConf.maxPartitionNumSupported(essConf)
    val needFallback = numPartitions >= confNumPartitions
    if (needFallback) {
      logInfo(s"Shuffle num of partitions: $numPartitions" +
        s" is bigger than the limit: $confNumPartitions," +
        s" need fallback to spark shuffle")
    }
    needFallback
  }

  /**
   * if rss cluster is under high load, fallback to external shuffle
   * @return if rss cluster's slots used percent is overhead the limit
   */
  def applyClusterLoadFallbackPolicy(lifecycleManager: LifecycleManager, numPartitions: Int):
    Boolean = {
    if (!RssConf.clusterLoadFallbackEnabled(essConf)) {
      return false
    }

    val needFallback = lifecycleManager.isClusterOverload(numPartitions)
    if (needFallback) {
      logWarning(s"Cluster is overload: $needFallback")
    }
    needFallback
  }

}
