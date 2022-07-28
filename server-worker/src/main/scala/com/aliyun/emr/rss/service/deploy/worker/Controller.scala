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

import java.io.IOException
import java.util.{ArrayList => jArrayList, HashMap => jHashMap, List => jList, Set => jSet}
import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import java.util.function.BiFunction

import scala.collection.JavaConverters._

import io.netty.util.{HashedWheelTimer, Timeout, TimerTask}

import com.aliyun.emr.rss.common.RssConf
import com.aliyun.emr.rss.common.internal.Logging
import com.aliyun.emr.rss.common.meta.{PartitionLocationInfo, WorkerInfo}
import com.aliyun.emr.rss.common.metrics.MetricsSystem
import com.aliyun.emr.rss.common.protocol.{PartitionLocation, PartitionSplitMode, PartitionType, StorageHint}
import com.aliyun.emr.rss.common.protocol.message.ControlMessages._
import com.aliyun.emr.rss.common.protocol.message.StatusCode
import com.aliyun.emr.rss.common.rpc._
import com.aliyun.emr.rss.common.util.Utils

private[deploy] class Controller(
    override val rpcEnv: RpcEnv,
    val conf: RssConf,
    val metricsSystem: MetricsSystem)
  extends RpcEndpoint with Logging {

  var workerSource: WorkerSource = _
  var localStorageManager: LocalStorageManager = _
  var registered: AtomicBoolean = _
  var shuffleMapperAttempts: ConcurrentHashMap[String, Array[Int]] = _
  var workerInfo: WorkerInfo = _
  var partitionLocationInfo: PartitionLocationInfo = _
  var timer: HashedWheelTimer = _
  var commitThreadPool: ThreadPoolExecutor = _
  var asyncReplyPool: ScheduledExecutorService = _

  def init(worker: Worker): Unit = {
    workerSource = worker.workerSource
    localStorageManager = worker.localStorageManager
    registered = worker.registered
    shuffleMapperAttempts = worker.shuffleMapperAttempts
    workerInfo = worker.workerInfo
    partitionLocationInfo = worker.partitionLocationInfo
    timer = worker.timer
    commitThreadPool = worker.commitThreadPool
    asyncReplyPool = worker.asyncReplyPool
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case ReserveSlots(applicationId, shuffleId, masterLocations, slaveLocations, splitThreshold,
    splitMode, partitionType) =>
      val shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId)
      workerSource.sample(WorkerSource.ReserveSlotsTime, shuffleKey) {
        logDebug(s"Received ReserveSlots request, $shuffleKey, " +
          s"master partitions: ${masterLocations.asScala.map(_.getUniqueId).mkString(",")}; " +
          s"slave partitions: ${slaveLocations.asScala.map(_.getUniqueId).mkString(",")}.")
        handleReserveSlots(context, applicationId, shuffleId, masterLocations,
          slaveLocations, splitThreshold, splitMode, partitionType)
        logDebug(s"ReserveSlots for $shuffleKey succeed.")
      }

    case CommitFiles(applicationId, shuffleId, masterIds, slaveIds, mapAttempts) =>
      val shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId)
      workerSource.sample(WorkerSource.CommitFilesTime, shuffleKey) {
        logDebug(s"Received CommitFiles request, $shuffleKey, master files" +
          s" ${masterIds.asScala.mkString(",")}; slave files ${slaveIds.asScala.mkString(",")}.")
        val commitFilesTimeMs = Utils.timeIt({
          handleCommitFiles(context, shuffleKey, masterIds, slaveIds, mapAttempts)
        })
        logDebug(s"Done processed CommitFiles request with shuffleKey $shuffleKey, in " +
          s"$commitFilesTimeMs ms.")
      }

    case GetWorkerInfos =>
      handleGetWorkerInfos(context)

    case ThreadDump =>
      handleThreadDump(context)

    case Destroy(shuffleKey, masterLocations, slaveLocations) =>
      handleDestroy(context, shuffleKey, masterLocations, slaveLocations)
  }

  private def handleReserveSlots(
      context: RpcCallContext,
      applicationId: String,
      shuffleId: Int,
      masterLocations: jList[PartitionLocation],
      slaveLocations: jList[PartitionLocation],
      splitThreshold: Long,
      splitMode: PartitionSplitMode,
      partitionType: PartitionType): Unit = {
    val shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId)
    if (!localStorageManager.hasAvailableWorkingDirs) {
      val msg = "Local storage has no available dirs!"
      logError(s"[handleReserveSlots] $msg")
      context.reply(ReserveSlotsResponse(StatusCode.ReserveSlotFailed, msg))
      return
    }
    val masterPartitions = new jArrayList[PartitionLocation]()
    try {
      for (ind <- 0 until masterLocations.size()) {
        val location = masterLocations.get(ind)
        val writer = localStorageManager.createWriter(applicationId, shuffleId, location,
          splitThreshold, splitMode, partitionType)
        masterPartitions.add(new WorkingPartition(location, writer))
      }
    } catch {
      case e: Exception =>
        logError(s"CreateWriter for $shuffleKey failed.", e)
    }
    if (masterPartitions.size() < masterLocations.size()) {
      val msg = s"Not all master partition satisfied for $shuffleKey"
      logWarning(s"[handleReserveSlots] $msg, will destroy writers.")
      masterPartitions.asScala.foreach(_.asInstanceOf[WorkingPartition].getFileWriter.destroy())
      context.reply(ReserveSlotsResponse(StatusCode.ReserveSlotFailed, msg))
      return
    }

    val slavePartitions = new jArrayList[PartitionLocation]()
    try {
      for (ind <- 0 until slaveLocations.size()) {
        val location = slaveLocations.get(ind)
        val writer = localStorageManager.createWriter(applicationId, shuffleId,
          location, splitThreshold, splitMode, partitionType)
        slavePartitions.add(new WorkingPartition(location, writer))
      }
    } catch {
      case e: Exception =>
        logError(s"CreateWriter for $shuffleKey failed.", e)
    }
    if (slavePartitions.size() < slaveLocations.size()) {
      val msg = s"Not all slave partition satisfied for $shuffleKey"
      logWarning(s"[handleReserveSlots] $msg, destroy writers.")
      masterPartitions.asScala.foreach(_.asInstanceOf[WorkingPartition].getFileWriter.destroy())
      slavePartitions.asScala.foreach(_.asInstanceOf[WorkingPartition].getFileWriter.destroy())
      context.reply(ReserveSlotsResponse(StatusCode.ReserveSlotFailed, msg))
      return
    }

    // reserve success, update status
    partitionLocationInfo.addMasterPartitions(shuffleKey, masterPartitions)
    partitionLocationInfo.addSlavePartitions(shuffleKey, slavePartitions)

    logDebug(s"allocate slots ${masterLocations.asScala.map(_.toString).mkString(",")}" +
      s"  ,  ${slaveLocations.asScala.map(_.toString).mkString(",")} ")
    workerInfo.allocateSlots(
      shuffleKey,
      Utils.diskSlotsDistribution(masterLocations, slaveLocations)
    )
    logInfo(
      s"Reserved ${masterPartitions.size()} master location" +
        s" and ${slavePartitions.size()} slave location for $shuffleKey " +
        s"master: ${masterPartitions}\nslave: ${slavePartitions}."
    )
    context.reply(ReserveSlotsResponse(StatusCode.Success))
  }

  private def commitFiles(
      shuffleKey: String,
      uniqueIds: jList[String],
      committedIds: jSet[String],
      failedIds: jSet[String],
      committedStorageHints: java.util.HashMap[String, StorageHint],
      writtenList: LinkedBlockingQueue[Long],
      master: Boolean = true): CompletableFuture[Void] = {
    var future: CompletableFuture[Void] = null

    if (uniqueIds != null) {
      uniqueIds.asScala.foreach { uniqueId =>
        val task = CompletableFuture.runAsync(new Runnable {
          override def run(): Unit = {
            try {
              val location = if (master) {
                partitionLocationInfo.getMasterLocation(shuffleKey, uniqueId)
              } else {
                partitionLocationInfo.getSlaveLocation(shuffleKey, uniqueId)
              }

              if (location == null) {
                logWarning(s"Get Partition Location for $shuffleKey $uniqueId but didn't exist.")
                return
              }

              val fileWriter = location.asInstanceOf[WorkingPartition].getFileWriter
              val bytes = fileWriter.close()
              if (bytes > 0L) {
                committedStorageHints.put(uniqueId, fileWriter.getStorageHint)
                writtenList.add(bytes)
                committedIds.add(uniqueId)
              }
            } catch {
              case e: IOException =>
                logError(s"Commit file for $shuffleKey $uniqueId failed.", e)
                failedIds.add(uniqueId)
            }
          }
        }, commitThreadPool)

        if (future == null) {
          future = task
        } else {
          future = CompletableFuture.allOf(future, task)
        }
      }
    }

    future
  }

  private def handleCommitFiles(
      context: RpcCallContext,
      shuffleKey: String,
      masterIds: jList[String],
      slaveIds: jList[String],
      mapAttempts: Array[Int]): Unit = {
    // return null if shuffleKey does not exist
    if (!partitionLocationInfo.containsShuffle(shuffleKey)) {
      logError(s"Shuffle $shuffleKey doesn't exist!")
      context.reply(
        CommitFilesResponse(
          StatusCode.ShuffleNotRegistered,
          List.empty.asJava,
          List.empty.asJava,
          masterIds,
          slaveIds
        )
      )
      return
    }

    // close and flush files.
    shuffleMapperAttempts.putIfAbsent(shuffleKey, mapAttempts)

    // Use ConcurrentSet to avoid excessive lock contention.
    val committedMasterIds = ConcurrentHashMap.newKeySet[String]()
    val committedSlaveIds = ConcurrentHashMap.newKeySet[String]()
    val failedMasterIds = ConcurrentHashMap.newKeySet[String]()
    val failedSlaveIds = ConcurrentHashMap.newKeySet[String]()
    val committedMasterStorageHints = new jHashMap[String, StorageHint]()
    val committedSlaveStorageHints = new jHashMap[String, StorageHint]()
    val committedWrittenSize = new LinkedBlockingQueue[Long]()

    val masterFuture =
      commitFiles(
        shuffleKey,
        masterIds,
        committedMasterIds,
        failedMasterIds,
        committedMasterStorageHints,
        committedWrittenSize
      )
    val slaveFuture = commitFiles(
      shuffleKey,
      slaveIds,
      committedSlaveIds,
      failedSlaveIds,
      committedSlaveStorageHints,
      committedWrittenSize,
      false
    )

    val future = if (masterFuture != null && slaveFuture != null) {
      CompletableFuture.allOf(masterFuture, slaveFuture)
    } else if (masterFuture != null) {
      masterFuture
    } else if (slaveFuture != null) {
      slaveFuture
    } else {
      null
    }

    def reply(): Unit = {
      // release slots before reply.
      val releaseMasterLocations =
        partitionLocationInfo.removeMasterPartitions(shuffleKey, masterIds)
      val releaseSlaveLocations = partitionLocationInfo.removeSlavePartitions(shuffleKey, slaveIds)
      logDebug(
        s"$shuffleKey remove" +
          s" slots count ${releaseMasterLocations._2 + releaseSlaveLocations._2}"
      )
      workerInfo.releaseSlots(shuffleKey, releaseMasterLocations._1)
      workerInfo.releaseSlots(shuffleKey, releaseSlaveLocations._1)

      val committedMasterIdList = new jArrayList[String](committedMasterIds)
      val committedSlaveIdList = new jArrayList[String](committedSlaveIds)
      val failedMasterIdList = new jArrayList[String](failedMasterIds)
      val failedSlaveIdList = new jArrayList[String](failedSlaveIds)
      val committedMasterStorageAndDiskHintList =
        new jHashMap[String, StorageHint](committedMasterStorageHints)
      val committedSlaveStorageAndDiskHintList =
        new jHashMap[String, StorageHint](committedSlaveStorageHints)
      val totalWritten = committedWrittenSize.asScala.sum
      val fileCount = committedWrittenSize.size()
      // reply
      if (failedMasterIds.isEmpty && failedSlaveIds.isEmpty) {
        logInfo(s"CommitFiles for $shuffleKey success with ${committedMasterIds.size()}" +
          s" master partitions and ${committedSlaveIds.size()} slave partitions!")
        context.reply(
          CommitFilesResponse(
            StatusCode.Success,
            committedMasterIdList,
            committedSlaveIdList,
            List.empty.asJava,
            List.empty.asJava,
            committedMasterStorageAndDiskHintList,
            committedSlaveStorageAndDiskHintList,
            totalWritten,
            fileCount
          )
        )
      } else {
        logWarning(s"CommitFiles for $shuffleKey failed with ${failedMasterIds.size()} master" +
          s" partitions and ${failedSlaveIds.size()} slave partitions!")
        context.reply(
          CommitFilesResponse(
            StatusCode.PartialSuccess,
            committedMasterIdList,
            committedSlaveIdList,
            failedMasterIdList,
            failedSlaveIdList,
            committedMasterStorageAndDiskHintList,
            committedSlaveStorageAndDiskHintList,
            totalWritten,
            fileCount
          )
        )
      }
    }

    if (future != null) {
      val result = new AtomicReference[CompletableFuture[Unit]]()
      val flushTimeout = RssConf.flushTimeout(conf)

      val timeout = timer.newTimeout(new TimerTask {
        override def run(timeout: Timeout): Unit = {
          if (result.get() != null) {
            result.get().cancel(true)
            logWarning(s"After waiting $flushTimeout s, cancel all commit file jobs.")
          }
        }
      }, flushTimeout, TimeUnit.SECONDS)

      result.set(future.handleAsync(new BiFunction[Void, Throwable, Unit] {
        override def apply(v: Void, t: Throwable): Unit = {
          if (null != t) {
            t match {
              case _: CancellationException =>
                logWarning("While handling commitFiles, canceled.")
              case ee: ExecutionException =>
                logError("While handling commitFiles, ExecutionException raised.", ee)
              case ie: InterruptedException =>
                logWarning("While handling commitFiles, interrupted.")
                Thread.currentThread().interrupt()
                throw ie
              case _: TimeoutException =>
                logWarning(s"While handling commitFiles, timeout after $flushTimeout s.")
              case throwable: Throwable =>
                logError("While handling commitFiles, exception occurs.", throwable)
            }
          } else {
            // finish, cancel timeout job first.
            timeout.cancel()
            reply()
          }
        }
      }, asyncReplyPool)) // should not use commitThreadPool in case of block by commit files.
    } else {
      // If both of two futures are null, then reply directly.
      reply()
    }
  }

  private def handleDestroy(
      context: RpcCallContext,
      shuffleKey: String,
      masterLocations: jList[String],
      slaveLocations: jList[String]): Unit = {
    // check whether shuffleKey has registered
    if (!partitionLocationInfo.containsShuffle(shuffleKey)) {
      logWarning(s"Shuffle $shuffleKey not registered!")
      context.reply(DestroyResponse(
        StatusCode.ShuffleNotRegistered, masterLocations, slaveLocations))
      return
    }

    val failedMasters = new jArrayList[String]()
    val failedSlaves = new jArrayList[String]()

    // destroy master locations
    if (masterLocations != null && !masterLocations.isEmpty) {
      masterLocations.asScala.foreach { loc =>
        val allocatedLoc = partitionLocationInfo.getMasterLocation(shuffleKey, loc)
        if (allocatedLoc == null) {
          failedMasters.add(loc)
        } else {
          allocatedLoc.asInstanceOf[WorkingPartition].getFileWriter.destroy()
        }
      }
      // remove master locations from WorkerInfo
      partitionLocationInfo.removeMasterPartitions(shuffleKey, masterLocations)
    }
    // destroy slave locations
    if (slaveLocations != null && !slaveLocations.isEmpty) {
      slaveLocations.asScala.foreach { loc =>
        val allocatedLoc = partitionLocationInfo.getSlaveLocation(shuffleKey, loc)
        if (allocatedLoc == null) {
          failedSlaves.add(loc)
        } else {
          allocatedLoc.asInstanceOf[WorkingPartition].getFileWriter.destroy()
        }
      }
      // remove slave locations from worker info
      partitionLocationInfo.removeSlavePartitions(shuffleKey, slaveLocations)
    }
    // reply
    if (failedMasters.isEmpty && failedSlaves.isEmpty) {
      logInfo(s"Destroy ${masterLocations.size()} master location and ${slaveLocations.size()}" +
        s" slave locations for $shuffleKey successfully.")
      context.reply(DestroyResponse(StatusCode.Success, null, null))
    } else {
      logInfo(s"Destroy ${failedMasters.size()}/${masterLocations.size()} master location and" +
        s"${failedSlaves.size()}/${slaveLocations.size()} slave location for" +
          s" $shuffleKey PartialSuccess.")
      context.reply(DestroyResponse(StatusCode.PartialSuccess, failedMasters, failedSlaves))
    }
  }

  private def handleGetWorkerInfos(context: RpcCallContext): Unit = {
    val list = new jArrayList[WorkerInfo]()
    list.add(workerInfo)
    context.reply(GetWorkerInfosResponse(StatusCode.Success, list.asScala.toList: _*))
  }

  private def handleThreadDump(context: RpcCallContext): Unit = {
    val threadDump = Utils.getThreadDump()
    context.reply(ThreadDumpResponse(threadDump))
  }

  def isRegistered(): Boolean = {
    registered.get()
  }
}
