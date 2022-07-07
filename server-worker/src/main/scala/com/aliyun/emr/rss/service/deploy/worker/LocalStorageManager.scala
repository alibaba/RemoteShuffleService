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

import java.io.{File, IOException}
import java.util
import java.util.concurrent.ConcurrentHashMap

import com.aliyun.emr.rss.common.RssConf
import com.aliyun.emr.rss.common.protocol.{PartitionLocation, PartitionSplitMode}

private[worker] final class LocalStorageManager(
  conf: RssConf,
  worker: Worker) {

  private val workingDirs: util.ArrayList[File] = null

  // shuffleKey -> (fileName -> writer)
  private val writers =
    new ConcurrentHashMap[String, ConcurrentHashMap[String, FileWriter]]()

  @throws[IOException]
  def createWriter(appId: String, shuffleId: Int, location: PartitionLocation,
    splitThreshold: Long, splitMode: PartitionSplitMode): FileWriter = {
    return null;
  }

  @throws[IOException]
  def createWriter(
    appId: String,
    shuffleId: Int,
    reduceId: Int,
    epoch: Int,
    mode: PartitionLocation.Mode,
    splitThreshold: Long,
    splitMode: PartitionSplitMode): FileWriter = {
    return null;
  }

  def getWriter(shuffleKey: String, fileName: String): FileWriter = {
    return null;
  }

  def shuffleKeySet(): util.Set[String] = writers.keySet()

  def cleanupExpiredShuffleKey(expiredShuffleKeys: util.HashSet[String]): Unit = {
  }

  def close(): Unit = {
  }

  private def flushFileWriters(): Unit = {}
}
