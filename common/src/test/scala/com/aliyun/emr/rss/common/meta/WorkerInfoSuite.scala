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

package com.aliyun.emr.rss.common.meta

import java.util.{ArrayList => jArrayList}
import java.util.{Map => jMap}
import java.util
import java.util.concurrent.{Future, ThreadLocalRandom}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

import org.junit.Assert.{assertEquals, assertNotEquals, assertNotNull}

import com.aliyun.emr.RssFunSuite
import com.aliyun.emr.rss.common.util.ThreadUtils

class WorkerInfoSuite extends RssFunSuite {

  test("test") {
    def run(block: () => Unit = () => {}): Unit = {
      block()
    }
    val block = () => {
      println("inside")
    }
    run(block)
  }

  private def check(host: String, rpcPort: Int, pushPort: Int, fetchPort: Int, replicatePort: Int
    , workerInfos: jMap[WorkerInfo, util.HashMap[String, Integer]],
    allocationMap: util.HashMap[String, Integer]): Unit = {
    val worker = new WorkerInfo(host, rpcPort, pushPort, fetchPort, replicatePort, null)
    val realWorker = workerInfos.get(worker)
    assertNotNull(s"Worker $worker didn't exist.", realWorker)
  }

  test("multi-thread modify same WorkerInfo.") {
    val numSlots = 10000
    val disks = new util.HashMap[String, DiskInfo]()
    disks.put("disk1", new DiskInfo("disk1", Int.MaxValue, 1.0, 0))
    disks.put("disk2", new DiskInfo("disk2", Int.MaxValue, 1.0, 0))
    disks.put("disk3", new DiskInfo("disk3", Int.MaxValue, 1.0, 0))
    val worker = new WorkerInfo("localhost", 10000, 10001, 10002, 10003, disks, null)

    val allocatedSlots = new AtomicInteger(0)
    val shuffleKey = "appId-shuffleId"
    val es = ThreadUtils.newDaemonFixedThreadPool(8, "workerInfo-unit-test")

    val futures = new ArrayBuffer[Future[_]]()
    (0 until 8).foreach { _ =>
      futures += es.submit(new Runnable {
        override def run(): Unit = {
          val rand = ThreadLocalRandom.current()
          while (true) {
            val allocatedSlot = allocatedSlots.get()
            if (allocatedSlot >= numSlots) {
              return
            }
            var requireSlot = rand.nextInt(100)
            val newAllocatedSlot = Math.min(numSlots, allocatedSlot + requireSlot)
            requireSlot = newAllocatedSlot - allocatedSlot
            if (allocatedSlots.compareAndSet(allocatedSlot, newAllocatedSlot)) {
              val allocationMap = new util.HashMap[String, Integer]()
              allocationMap.put("disk1", requireSlot)
              worker.allocateSlots(shuffleKey, allocationMap)
            }
          }
        }
      })
    }
    futures.foreach(_.get())
    futures.clear()

    assertEquals(numSlots, allocatedSlots.get())
    assertEquals(numSlots, worker.usedSlots())

    (0 until 8).foreach { _ =>
      futures += es.submit(new Runnable {
        override def run(): Unit = {
          val rand = ThreadLocalRandom.current()
          while (true) {
            val allocatedSlot = allocatedSlots.get()
            if (allocatedSlot <= 0) {
              return
            }
            var releaseSlot = rand.nextInt(100)
            val newAllocatedSlot = Math.max(0, allocatedSlot - releaseSlot)
            releaseSlot = allocatedSlot - newAllocatedSlot
            if (allocatedSlots.compareAndSet(allocatedSlot, newAllocatedSlot)) {
              val allocations = new util.HashMap[String, Integer]()
              allocations.put("disk1", releaseSlot)
              worker.releaseSlots(shuffleKey, allocations)
            }
          }
          worker.releaseSlots(shuffleKey)
        }
      })
    }
    futures.foreach(_.get())
    futures.clear()

    assertEquals(0, allocatedSlots.get())
    assertEquals(0, worker.usedSlots())

    ThreadUtils.shutdown(es, 800.millisecond)
  }

  test("WorkerInfo not equals when host different.") {
    val worker1 = new WorkerInfo("h1", 10001, 10002, 10003, 1000, null, null)
    val worker2 = new WorkerInfo("h2", 10001, 10002, 10003, 1000, null, null)
    assertNotEquals(worker1, worker2)
  }

  test("WorkerInfo not equals when rpc port different.") {
    val worker1 = new WorkerInfo("h1", 10001, 10002, 10003, 1000, null, null)
    val worker2 = new WorkerInfo("h1", 20001, 10002, 10003, 1000, null, null)
    assertNotEquals(worker1, worker2)
  }

  test("WorkerInfo not equals when push port different.") {
    val worker1 = new WorkerInfo("h1", 10001, 10002, 10003, 1000,null, null)
    val worker2 = new WorkerInfo("h1", 10001, 20002, 10003, 1000,null, null)
    assertNotEquals(worker1, worker2)
  }

  test("WorkerInfo not equals when fetch port different.") {
    val worker1 = new WorkerInfo("h1", 10001, 10002, 10003, 1000, null, null)
    val worker2 = new WorkerInfo("h1", 10001, 10002, 20003, 1000, null, null)
    assertNotEquals(worker1, worker2)
  }

  test("WorkerInfo equals when numSlots different.") {
    val worker1 = new WorkerInfo("h1", 10001, 10002, 10003, 1000, null, null)
    val worker2 = new WorkerInfo("h1", 10001, 10002, 10003, 2000, null, null)
    assertEquals(worker1, worker2)
  }
}
