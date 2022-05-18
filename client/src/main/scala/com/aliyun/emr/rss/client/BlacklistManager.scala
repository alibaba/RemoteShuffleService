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

package com.aliyun.emr.rss.client

import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.function.Predicate

import scala.collection.JavaConverters.collectionAsScalaIterableConverter

import com.aliyun.emr.rss.common.internal.Logging
import com.aliyun.emr.rss.common.meta.{PartitionLocationInfo, WorkerInfo}
import com.aliyun.emr.rss.common.protocol.RpcNameConstants.WORKER_EP
import com.aliyun.emr.rss.common.rpc.{RpcAddress, RpcEnv}
import com.aliyun.emr.rss.common.rpc.netty.{NettyRpcEndpointRef, NettyRpcEnv}

class BlacklistManager(rpcEnv: RpcEnv,
  allocatedWorkers: ConcurrentHashMap[Int, ConcurrentHashMap[WorkerInfo, PartitionLocationInfo]])
  extends Logging {
  val blacklists = ConcurrentHashMap.newKeySet[WorkerInfo]()
  val unknownWorkers = ConcurrentHashMap.newKeySet[WorkerInfo]()

  val resurrectionThread = new Thread(new Runnable {
    override def run(): Unit = {
      while (true) {
        val iter = blacklists.iterator()
        while (iter.hasNext) {
          val worker = iter.next()
          if (worker.endpoint == null) {
            try {
              worker.setupEndpoint(rpcEnv.setupEndpointRef(
                RpcAddress.apply(worker.host, worker.rpcPort), WORKER_EP))
              worker.endpoint.asInstanceOf[NettyRpcEndpointRef].client =
                rpcEnv.asInstanceOf[NettyRpcEnv].clientFactory.createClient(worker.host,
                  worker.rpcPort)
            } catch {
              case _: Throwable =>
              // ignore and continue
            }
          }
          if (worker.endpoint != null) {
            iter.remove()
          }
        }

        val unknownIter = unknownWorkers.iterator()
        while (unknownIter.hasNext) {
          val unknownWorker = unknownIter.next()
          var workerUsed = false
          for (item <- allocatedWorkers.values().asScala) {
            workerUsed = item.containsKey(unknownWorker) | workerUsed
          }
          if (!workerUsed) {
            unknownIter.remove()
            blacklists.remove(unknownWorker)
          }
        }

        Thread.sleep(50)
      }
    }
  }, "blacklist-manager-thread")
  resurrectionThread.start()

  val snapshotThread = new Thread(new Runnable {
    override def run(): Unit = {
      while (true) {
        logDebug(s"blacklist : ${blacklists}  unknown workers : ${unknownWorkers} ")
        Thread.sleep(10 * 1000)
      }
    }
  }, "blacklist-manager-snapshot")
  snapshotThread.start()

  def contains(worker: WorkerInfo): Boolean = {
    blacklists.contains(worker) || unknownWorkers.contains(worker)
  }

  def addBlacklist(worker: WorkerInfo): Unit = {
    blacklists.add(worker)
  }

  def addBlacklists(blacklist: java.util.List[WorkerInfo]): Unit = {
    if (blacklist != null) {
      this.blacklists.removeIf(new Predicate[WorkerInfo]() {
        override def test(t: WorkerInfo): Boolean = {
          t.endpoint != null
        }
      })
      this.blacklists.addAll(blacklist)
    }
  }

  def addUnknownWorker(unknowns: java.util.List[WorkerInfo]): Unit = {
    if (unknowns != null) {
      unknownWorkers.addAll(unknowns)
    }
  }

  def getBlacklist(): util.Set[WorkerInfo] = {
    val nSet = new util.HashSet[WorkerInfo]()
    nSet.addAll(blacklists)
    nSet.addAll(unknownWorkers)
    nSet
  }
}
