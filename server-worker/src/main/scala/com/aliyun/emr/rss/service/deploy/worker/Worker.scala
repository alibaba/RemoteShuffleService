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
import java.nio.ByteBuffer
import java.util.{ArrayList => jArrayList, HashSet => jHashSet, List => jList}
import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import java.util.function.BiFunction

import scala.collection.JavaConverters._

import io.netty.buffer.ByteBuf
import io.netty.util.{HashedWheelTimer, Timeout, TimerTask}
import io.netty.util.internal.ConcurrentSet

import com.aliyun.emr.rss.common.RssConf
import com.aliyun.emr.rss.common.exception.{AlreadyClosedException, RssException}
import com.aliyun.emr.rss.common.haclient.RssHARetryClient
import com.aliyun.emr.rss.common.internal.Logging
import com.aliyun.emr.rss.common.meta.{PartitionLocationInfo, WorkerInfo}
import com.aliyun.emr.rss.common.metrics.MetricsSystem
import com.aliyun.emr.rss.common.network.TransportContext
import com.aliyun.emr.rss.common.network.buffer.NettyManagedBuffer
import com.aliyun.emr.rss.common.network.client.{RpcResponseCallback, TransportClientBootstrap, TransportClientFactory}
import com.aliyun.emr.rss.common.network.protocol.{PushData, PushMergedData}
import com.aliyun.emr.rss.common.network.server.{ChannelsLimiter, FileInfo, MemoryTracker, TransportServer, TransportServerBootstrap}
import com.aliyun.emr.rss.common.protocol.{PartitionLocation, PartitionSplitMode, RpcNameConstants}
import com.aliyun.emr.rss.common.protocol.PartitionLocation.StorageHint
import com.aliyun.emr.rss.common.protocol.TransportModuleConstants._
import com.aliyun.emr.rss.common.protocol.message.ControlMessages._
import com.aliyun.emr.rss.common.protocol.message.StatusCode
import com.aliyun.emr.rss.common.rpc._
import com.aliyun.emr.rss.common.unsafe.Platform
import com.aliyun.emr.rss.common.util.{ThreadUtils, Utils}
import com.aliyun.emr.rss.server.common.http.{HttpServer, HttpServerInitializer}
import com.aliyun.emr.rss.service.deploy.worker.WorkerSource._
import com.aliyun.emr.rss.service.deploy.worker.http.HttpRequestHandler

private[deploy] class Worker(
    override val rpcEnv: RpcEnv,
    val conf: RssConf,
    val metricsSystem: MetricsSystem)
  extends RpcEndpoint with PushDataHandler with OpenStreamHandler with Registerable with Logging {

  // whether this Worker registered to Master successfully
  private val registered = new AtomicBoolean(false)
  private val shuffleMapperAttempts = new ConcurrentHashMap[String, Array[Int]]()
  private val rssHARetryClient = new RssHARetryClient(rpcEnv, conf)
  private val workerSource = new WorkerSource(conf, metricsSystem)
  private val memoryTracker = MemoryTracker.initialize(conf)
  private val partitionsSorter = new PartitionFilesSorter(memoryTracker, conf, workerSource)
  private val partitionLocationInfo = new PartitionLocationInfo
  private val localStorageManager = new LocalStorageManager(conf, workerSource, this)
  memoryTracker.registerMemoryListener(localStorageManager)

  private val defaultIOThread = localStorageManager.numDisks * 2
  private val pushServer = createTransportServer(
    conf, PUSH_MODULE, RssConf.pushServerPort(conf), defaultIOThread, true)
  private val pushClientFactory = createTransportClientFactory(conf, PUSH_MODULE, defaultIOThread)
  private val replicateServer = createTransportServer(
    conf, REPLICATE_MODULE, RssConf.replicateServerPort(conf), defaultIOThread, true)
  private val fetchServer = createTransportServer(
    conf, FETCH_MODULE, RssConf.fetchServerPort(conf), defaultIOThread, false)

  // Threads
  private val forwardMessageScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("worker-forward-message-scheduler")
  private val replicateThreadPool =
    ThreadUtils.newDaemonCachedThreadPool(
      "worker-replicate-data", RssConf.workerReplicateNumThreads(conf))
  // shared ExecutorService for flush
  private val commitThreadPool = ThreadUtils.newDaemonCachedThreadPool(
    "Worker-CommitFiles", RssConf.workerAsyncCommitFileThreads(conf))
  private val asyncReplyPool = ThreadUtils.newDaemonSingleThreadScheduledExecutor("async-reply")

  private val diskFlushTimer = new HashedWheelTimer()
  // (workerInfo -> last connect timeout timestamp)
  private val unavailablePeers = new ConcurrentHashMap[WorkerInfo, Long]()

  private var logAvailableFlushBuffersTask: ScheduledFuture[_] = _
  private var heartbeater: ScheduledFuture[_] = _
  private var fastFailTaskChecker: ScheduledFuture[_] = _

  private val heartbeatIntervalMs = RssConf.workerTimeoutMs(conf) / 4
  private val workerSlotsNum = RssConf.workerNumSlots(conf, localStorageManager.numDisks)
  private val replicateFastFailDuration = RssConf.replicateFastFailDurationMs(conf)

  private val host = rpcEnv.address.host
  private val rpcPort = rpcEnv.address.port
  private val pushPort = pushServer.getPort
  private val fetchPort = fetchServer.getPort
  private val replicatePort = replicateServer.getPort

  Utils.checkHost(host)
  assert(pushPort > 0, s"Worker push port should > 0, current port is $pushPort.")
  assert(fetchPort > 0, s"Worker fetch port should > 0, current port is $fetchPort.")
  assert(replicatePort > 0, s"Worker replicate port should > 0, current port is $replicatePort.")

  // worker info
  private val workerInfo = new WorkerInfo(
    host, rpcPort, pushPort, fetchPort, replicatePort, workerSlotsNum, self)

  workerSource.addGauge(REGISTERED_SHUFFLE_COUNT, _ => partitionLocationInfo.shuffleKeySet.size())
  workerSource.addGauge(TOTAL_SLOTS, _ => workerInfo.numSlots)
  workerSource.addGauge(USED_SLOTS, _ => workerInfo.usedSlots())
  workerSource.addGauge(AVAILABLE_SLOTS, _ => workerInfo.freeSlots())
  workerSource.addGauge(SORT_MEMORY, _ => memoryTracker.getSortMemoryCounter.get())
  workerSource.addGauge(SORTING_FILES, _ => partitionsSorter.getSortingCount)
  workerSource.addGauge(DISK_BUFFER, _ => memoryTracker.getDiskBufferCounter.get())
  workerSource.addGauge(NETTY_MEMORY, _ => memoryTracker.getNettyMemoryCounter.get())
  workerSource.addGauge(PAUSE_PUSH_DATA_COUNT, _ => memoryTracker.getPausePushDataCounter)
  workerSource.addGauge(PAUSE_PUSH_DATA_AND_REPLICATE_COUNT,
    _ => memoryTracker.getPausePushDataAndReplicateCounter)

  def updateNumSlots(numSlots: Int): Unit = {
    workerInfo.setNumSlots(numSlots)
    heartBeatToMaster()
  }

  private def heartBeatToMaster(): Unit = {
    val shuffleKeys = new jHashSet[String]
    shuffleKeys.addAll(partitionLocationInfo.shuffleKeySet)
    shuffleKeys.addAll(localStorageManager.shuffleKeySet())
    val response = rssHARetryClient.askSync[HeartbeatResponse](
      HeartbeatFromWorker(host, rpcPort, pushPort, fetchPort, replicatePort, workerInfo.numSlots,
        shuffleKeys)
      , classOf[HeartbeatResponse])
    if (response.registered) {
      cleanTaskQueue.put(response.expiredShuffleKeys)
    } else {
      logError("Worker not registered in master, clean all shuffle data and register again.")
      // Clean all shuffle related metadata and data
      cleanup(shuffleKeys)
      try {
        registerWithMaster()
      } catch {
        case e: Throwable =>
          logError("Re-register worker failed after worker lost.", e)
          // Register failed then stop server
          stop()
      }
    }
  }

  override def onStart(): Unit = {
    logInfo(s"Starting Worker $host:$pushPort:$fetchPort:$replicatePort" +
      s" with ${workerInfo.numSlots} slots.")
    registerWithMaster()

    // start heartbeat
    heartbeater = forwardMessageScheduler.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = Utils.tryLogNonFatalError {
        heartBeatToMaster()
      }
    }, heartbeatIntervalMs, heartbeatIntervalMs, TimeUnit.MILLISECONDS)

    fastFailTaskChecker = forwardMessageScheduler.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = Utils.tryLogNonFatalError {
        unavailablePeers.entrySet().asScala.foreach(entry => {
          if (System.currentTimeMillis() - entry.getValue > replicateFastFailDuration) {
            unavailablePeers.remove(entry.getKey)
          }
        })
      }
    }, 0, replicateFastFailDuration, TimeUnit.MILLISECONDS)
  }

  override def onStop(): Unit = {
    logInfo("Stopping RSS Worker.")

    if (heartbeater != null) {
      heartbeater.cancel(true)
      heartbeater = null
    }
    if (logAvailableFlushBuffersTask != null) {
      logAvailableFlushBuffersTask.cancel(true)
      logAvailableFlushBuffersTask = null
    }
    if (fastFailTaskChecker != null) {
      fastFailTaskChecker.cancel(true)
      fastFailTaskChecker = null
    }

    forwardMessageScheduler.shutdownNow()
    replicateThreadPool.shutdownNow()
    commitThreadPool.shutdownNow()
    asyncReplyPool.shutdownNow()
    partitionsSorter.close()

    if (null != localStorageManager) {
      localStorageManager.close()
    }

    rssHARetryClient.close()
    replicateServer.close()
    fetchServer.close()
    logInfo("RSS Worker is stopped.")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case ReserveSlots(applicationId, shuffleId, masterLocations, slaveLocations, splitThreshold,
    splitMode, storageHint) =>
      val shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId)
      workerSource.sample(RESERVE_SLOTS_TIME, shuffleKey) {
        logDebug(s"Received ReserveSlots request, $shuffleKey, " +
          s"master partitions: ${masterLocations.asScala.map(_.getUniqueId).mkString(",")}; " +
          s"slave partitions: ${slaveLocations.asScala.map(_.getUniqueId).mkString(",")}.")
        handleReserveSlots(context, applicationId, shuffleId, masterLocations,
          slaveLocations, splitThreshold, splitMode, storageHint)
        logDebug(s"ReserveSlots for $shuffleKey succeed.")
      }

    case CommitFiles(applicationId, shuffleId, masterIds, slaveIds, mapAttempts) =>
      val shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId)
      workerSource.sample(COMMIT_FILE_TIME, shuffleKey) {
        logDebug(s"Received CommitFiles request, $shuffleKey, master files" +
          s" ${masterIds.asScala.mkString(",")}; slave files ${slaveIds.asScala.mkString(",")}.")
        val commitFilesTimeMs = Utils.timeIt {
          handleCommitFiles(context, shuffleKey, masterIds, slaveIds, mapAttempts)
        }
        logDebug(s"Done processed CommitFiles request with shuffleKey $shuffleKey, in " +
          s"${commitFilesTimeMs}ms.")
      }

    case GetWorkerInfos =>
      handleGetWorkerInformation(context)

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
      storageHint: StorageHint): Unit = {
    val shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId)
    if (!localStorageManager.hasAvailableWorkingDirs) {
      val msg = "Local storage has no available dirs!"
      logError(s"[handleReserveSlots] $msg")
      context.reply(ReserveSlotsResponse(StatusCode.ReserveSlotFailed, msg))
      return
    }
    val masterPartitions = new jArrayList[PartitionLocation]()
    try {
      for (index <- 0 until masterLocations.size) {
        val location = masterLocations.get(index)
        val writer = localStorageManager.createWriter(applicationId, shuffleId, location,
          splitThreshold, splitMode)
        masterPartitions.add(new WorkingPartition(location, writer))
      }
    } catch {
      case e: Exception =>
        logError(s"CreateWriter for $shuffleKey failed.", e)
    }
    if (masterPartitions.size < masterLocations.size) {
      val msg = s"Not all master partition satisfied for $shuffleKey"
      logWarning(s"[handleReserveSlots] $msg, will destroy writers.")
      masterPartitions.asScala.foreach(_.asInstanceOf[WorkingPartition].getFileWriter.destroy())
      context.reply(ReserveSlotsResponse(StatusCode.ReserveSlotFailed, msg))
      return
    }

    val slavePartitions = new jArrayList[PartitionLocation]()
    try {
      for (index <- 0 until slaveLocations.size) {
        val location = slaveLocations.get(index)
        val writer = localStorageManager.createWriter(applicationId, shuffleId,
          location, splitThreshold, splitMode)
        slavePartitions.add(new WorkingPartition(location, writer))
      }
    } catch {
      case e: Exception =>
        logError(s"CreateWriter for $shuffleKey failed.", e)
    }
    if (slavePartitions.size < slaveLocations.size) {
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
    workerInfo.allocateSlots(shuffleKey, masterPartitions.size + slavePartitions.size)
    logInfo(s"Reserved ${masterPartitions.size} master location and ${slavePartitions.size}" +
      s" slave location for $shuffleKey master: $masterPartitions\nslave: $slavePartitions.")
    context.reply(ReserveSlotsResponse(StatusCode.Success))
  }

  private def commitFiles(
      shuffleKey: String,
      uniqueIds: jList[String],
      committedIds: ConcurrentSet[String],
      failedIds: ConcurrentSet[String],
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
              val bytes = fileWriter.close
              if (bytes > 0L) {
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
      context.reply(CommitFilesResponse(
        StatusCode.ShuffleNotRegistered, new jArrayList[String](), new jArrayList[String](),
        masterIds, slaveIds))
      return
    }

    // close and flush files.
    shuffleMapperAttempts.putIfAbsent(shuffleKey, mapAttempts)

    // Use ConcurrentSet to avoid excessive lock contention.
    val committedMasterIds = new ConcurrentSet[String]()
    val committedSlaveIds = new ConcurrentSet[String]()
    val failedMasterIds = new ConcurrentSet[String]()
    val failedSlaveIds = new ConcurrentSet[String]()

    val masterFuture = commitFiles(shuffleKey, masterIds, committedMasterIds, failedMasterIds)
    val slaveFuture = commitFiles(shuffleKey, slaveIds, committedSlaveIds, failedSlaveIds, false)

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
      val numSlotsReleased =
        partitionLocationInfo.removeMasterPartitions(shuffleKey, masterIds) +
        partitionLocationInfo.removeSlavePartitions(shuffleKey, slaveIds)
      workerInfo.releaseSlots(shuffleKey, numSlotsReleased)

      val committedMasterIdList = new jArrayList[String](committedMasterIds)
      val committedSlaveIdList = new jArrayList[String](committedSlaveIds)
      val failedMasterIdList = new jArrayList[String](failedMasterIds)
      val failedSlaveIdList = new jArrayList[String](failedSlaveIds)
      // reply
      if (failedMasterIds.isEmpty && failedSlaveIds.isEmpty) {
        logInfo(s"CommitFiles for $shuffleKey success with ${committedMasterIds.size()}" +
          s" master partitions and ${committedSlaveIds.size()} slave partitions!")
        context.reply(CommitFilesResponse(
          StatusCode.Success, committedMasterIdList, committedSlaveIdList,
          new jArrayList[String](), new jArrayList[String]()))
      } else {
        logWarning(s"CommitFiles for $shuffleKey failed with ${failedMasterIds.size()} master" +
          s" partitions and ${failedSlaveIds.size()} slave partitions!")
        context.reply(CommitFilesResponse(StatusCode.PartialSuccess, committedMasterIdList,
          committedSlaveIdList, failedMasterIdList, failedSlaveIdList))
      }
    }

    if (future != null) {
      val result = new AtomicReference[CompletableFuture[Unit]]()
      val flushTimeout = RssConf.flushTimeout(conf)

      val timeout = diskFlushTimer.newTimeout(new TimerTask {
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

  private def handleGetWorkerInformation(context: RpcCallContext): Unit = {
    val list = new jArrayList[WorkerInfo]()
    list.add(workerInfo)
    context.reply(GetWorkerInfosResponse(StatusCode.Success, list.asScala.toList: _*))
  }

  private def handleThreadDump(context: RpcCallContext): Unit = {
    val threadDump = Utils.getThreadDump()
    context.reply(ThreadDumpResponse(threadDump))
  }

  private def getMapAttempt(body: ByteBuf): (Int, Int) = {
    // header: mapId attemptId batchId compressedTotalSize
    val header = new Array[Byte](8)
    body.getBytes(body.readerIndex(), header)
    val mapId = Platform.getInt(header, Platform.BYTE_ARRAY_OFFSET)
    val attemptId = Platform.getInt(header, Platform.BYTE_ARRAY_OFFSET + 4)
    (mapId, attemptId)
  }

  override def handlePushData(pushData: PushData, callback: RpcResponseCallback): Unit = {
    val shuffleKey = pushData.shuffleKey
    val mode = PartitionLocation.getMode(pushData.mode)
    val body = pushData.body.asInstanceOf[NettyManagedBuffer].getBuf
    val isMaster = mode == PartitionLocation.Mode.Master

    val key = s"${pushData.requestId}"
    if (isMaster) {
      workerSource.startTimer(MASTER_PUSH_DATA_TIME, key)
    } else {
      workerSource.startTimer(SLAVE_PUSH_DATA_TIME, key)
    }

    // find FileWriter responsible for the data
    val location = if (isMaster) {
      partitionLocationInfo.getMasterLocation(shuffleKey, pushData.partitionUniqueId)
    } else {
      partitionLocationInfo.getSlaveLocation(shuffleKey, pushData.partitionUniqueId)
    }

    val wrappedCallback = new RpcResponseCallback() {
      override def onSuccess(response: ByteBuffer): Unit = {
        if (isMaster) {
          workerSource.stopTimer(MASTER_PUSH_DATA_TIME, key)
          if (response.remaining() > 0) {
            val resp = ByteBuffer.allocate(response.remaining())
            resp.put(response)
            resp.flip()
            callback.onSuccess(resp)
          } else {
            callback.onSuccess(response)
          }
        } else {
          workerSource.stopTimer(SLAVE_PUSH_DATA_TIME, key)
          callback.onSuccess(response)
        }
      }

      override def onFailure(e: Throwable): Unit = {
        logError(s"[handlePushData.onFailure] partitionLocation: $location")
        workerSource.incCounter(PUSH_DATA_FAIL_COUNT)
        callback.onFailure(new Exception(StatusCode.PushDataFailSlave.getMessage(), e))
      }
    }

    if (location == null) {
      val (mapId, attemptId) = getMapAttempt(body)
      if (shuffleMapperAttempts.containsKey(shuffleKey) &&
          -1 != shuffleMapperAttempts.get(shuffleKey)(mapId)) {
        // partition data has already been committed
        logInfo(s"Receive push data from speculative task(shuffle $shuffleKey, map $mapId, " +
          s" attempt $attemptId), but this mapper has already been ended.")
        wrappedCallback.onSuccess(ByteBuffer.wrap(Array[Byte](StatusCode.StageEnded.getValue)))
      } else {
        val msg = s"Partition location wasn't found for task(shuffle $shuffleKey, map $mapId, " +
          s"attempt $attemptId, uniqueId ${pushData.partitionUniqueId})."
        logWarning(s"[handlePushData] $msg")
        callback.onFailure(new Exception(StatusCode.PushDataFailPartitionNotFound.getMessage))
      }
      return
    }
    val fileWriter = location.asInstanceOf[WorkingPartition].getFileWriter
    val exception = fileWriter.getException
    if (exception != null) {
      logWarning(s"[handlePushData] fileWriter $fileWriter has Exception $exception")
      val message = if (isMaster) {
        StatusCode.PushDataFailMain.getMessage
      } else {
        StatusCode.PushDataFailSlave.getMessage
      }
      callback.onFailure(new Exception(message, exception))
      return
    }
    if (isMaster && fileWriter.getFileLength > fileWriter.getSplitThreshold) {
      fileWriter.setSplitFlag()
      if (fileWriter.getSplitMode == PartitionSplitMode.soft) {
        callback.onSuccess(ByteBuffer.wrap(Array[Byte](StatusCode.SoftSplit.getValue)))
      } else {
        callback.onSuccess(ByteBuffer.wrap(Array[Byte](StatusCode.HardSplit.getValue)))
        return
      }
    }
    fileWriter.incrementPendingWrites()

    // for master, send data to slave
    if (location.getPeer != null && isMaster) {
      pushData.body().retain()
      replicateThreadPool.submit(new Runnable {
        override def run(): Unit = {
          val peer = location.getPeer
          val peerWorker = new WorkerInfo(peer.getHost, peer.getRpcPort, peer.getPushPort,
            peer.getFetchPort, peer.getReplicatePort, -1, null)
          if (unavailablePeers.containsKey(peerWorker)) {
            pushData.body().release()
            wrappedCallback.onFailure(new Exception(s"Peer $peerWorker unavailable!"))
            return
          }
          try {
            val client = pushClientFactory.createClient(peer.getHost, peer.getReplicatePort,
              location.getReduceId)
            val newPushData = new PushData(
              PartitionLocation.Mode.Slave.mode(),
              shuffleKey,
              pushData.partitionUniqueId,
              pushData.body)
            client.pushData(newPushData, wrappedCallback)
          } catch {
            case e: Exception =>
              pushData.body().release()
              unavailablePeers.put(peerWorker, System.currentTimeMillis())
              wrappedCallback.onFailure(e)
          }
        }
      })
    } else {
      wrappedCallback.onSuccess(ByteBuffer.wrap(Array[Byte]()))
    }

    try {
      fileWriter.write(body)
    } catch {
      case e: AlreadyClosedException =>
        fileWriter.decrementPendingWrites()
        val (mapId, attemptId) = getMapAttempt(body)
        if (shuffleMapperAttempts.containsKey(shuffleKey)) {
          shuffleMapperAttempts.get(shuffleKey)(mapId)
        }
        logWarning(s"Append data failed for task(shuffle $shuffleKey, map $mapId, attempt" +
          s" $attemptId), caused by ${e.getMessage}")
      case e: Exception =>
        logError("Exception encountered when write.", e)
    }
  }

  override def handlePushMergedData(
      pushMergedData: PushMergedData,
      callback: RpcResponseCallback): Unit = {
    val shuffleKey = pushMergedData.shuffleKey
    val mode = PartitionLocation.getMode(pushMergedData.mode)
    val batchOffsets = pushMergedData.batchOffsets
    val body = pushMergedData.body.asInstanceOf[NettyManagedBuffer].getBuf
    val isMaster = mode == PartitionLocation.Mode.Master

    val key = s"${pushMergedData.requestId}"
    if (isMaster) {
      workerSource.startTimer(MASTER_PUSH_DATA_TIME, key)
    } else {
      workerSource.startTimer(SLAVE_PUSH_DATA_TIME, key)
    }

    val wrappedCallback = new RpcResponseCallback() {
      override def onSuccess(response: ByteBuffer): Unit = {
        if (isMaster) {
          workerSource.stopTimer(MASTER_PUSH_DATA_TIME, key)
          if (response.remaining() > 0) {
            val resp = ByteBuffer.allocate(response.remaining())
            resp.put(response)
            resp.flip()
            callback.onSuccess(resp)
          } else {
            callback.onSuccess(response)
          }
        } else {
          workerSource.stopTimer(SLAVE_PUSH_DATA_TIME, key)
          callback.onSuccess(response)
        }
      }

      override def onFailure(e: Throwable): Unit = {
        workerSource.incCounter(PUSH_DATA_FAIL_COUNT)
        callback.onFailure(new Exception(StatusCode.PushDataFailSlave.getMessage, e))
      }
    }

    // find FileWriters responsible for the data
    val locations = pushMergedData.partitionUniqueIds.map { id =>
      val loc = if (isMaster) {
        partitionLocationInfo.getMasterLocation(shuffleKey, id)
      } else {
        partitionLocationInfo.getSlaveLocation(shuffleKey, id)
      }
      if (loc == null) {
        val (mapId, attemptId) = getMapAttempt(body)
        if (shuffleMapperAttempts.containsKey(shuffleKey)
            && -1 != shuffleMapperAttempts.get(shuffleKey)(mapId)) {
          val msg = s"Receive push data from speculative task(shuffle $shuffleKey, map $mapId," +
            s" attempt $attemptId), but this mapper has already been ended."
          logInfo(msg)
          wrappedCallback.onSuccess(ByteBuffer.wrap(Array[Byte](StatusCode.StageEnded.getValue)))
        } else {
          val msg = s"Partition location wasn't found for task(shuffle $shuffleKey, map $mapId," +
            s" attempt $attemptId, uniqueId $id)."
          logWarning(s"[handlePushMergedData] $msg")
          wrappedCallback.onFailure(new Exception(msg))
        }
        return
      }
      loc
    }

    val fileWriters = locations.map(_.asInstanceOf[WorkingPartition].getFileWriter)
    val fileWriterWithException = fileWriters.find(_.getException != null)
    if (fileWriterWithException.nonEmpty) {
      val exception = fileWriterWithException.get.getException
      logDebug(s"[handlePushMergedData] fileWriter ${fileWriterWithException}" +
          s" has Exception $exception")
      val message = if (isMaster) {
        StatusCode.PushDataFailMain.getMessage
      } else {
        StatusCode.PushDataFailSlave.getMessage
      }
      callback.onFailure(new Exception(message, exception))
      return
    }
    fileWriters.foreach(_.incrementPendingWrites())

    // for master, send data to slave
    if (locations.head.getPeer != null && isMaster) {
      pushMergedData.body().retain()
      replicateThreadPool.submit(new Runnable {
        override def run(): Unit = {
          val location = locations.head
          val peer = location.getPeer
          val peerWorker = new WorkerInfo(peer.getHost,
            peer.getRpcPort, peer.getPushPort, peer.getFetchPort, peer.getReplicatePort, -1, null)
          if (unavailablePeers.containsKey(peerWorker)) {
            pushMergedData.body().release()
            wrappedCallback.onFailure(new Exception(s"Peer $peerWorker unavailable!"))
            return
          }
          try {
            val client = pushClientFactory.createClient(
              peer.getHost, peer.getReplicatePort, location.getReduceId)
            val newPushMergedData = new PushMergedData(
              PartitionLocation.Mode.Slave.mode,
              shuffleKey,
              pushMergedData.partitionUniqueIds,
              batchOffsets,
              pushMergedData.body)
            client.pushMergedData(newPushMergedData, wrappedCallback)
          } catch {
            case e: Exception =>
              pushMergedData.body().release()
              unavailablePeers.put(peerWorker, System.currentTimeMillis)
              wrappedCallback.onFailure(e)
          }
        }
      })
    } else {
      wrappedCallback.onSuccess(ByteBuffer.wrap(Array[Byte]()))
    }

    var index = 0
    var fileWriter: FileWriter = null
    var alreadyClosed = false
    while (index < fileWriters.length) {
      fileWriter = fileWriters(index)
      val offset = body.readerIndex + batchOffsets(index)
      val length = if (index == fileWriters.length - 1) {
        body.readableBytes - batchOffsets(index)
      } else {
        batchOffsets(index + 1) - batchOffsets(index)
      }
      val batchBody = body.slice(offset, length)

      try {
        if (!alreadyClosed) {
          fileWriter.write(batchBody)
        } else {
          fileWriter.decrementPendingWrites()
        }
      } catch {
        case e: AlreadyClosedException =>
          fileWriter.decrementPendingWrites()
          alreadyClosed = true
          val (mapId, attemptId) = getMapAttempt(body)
          if (shuffleMapperAttempts.containsKey(shuffleKey)) {
            shuffleMapperAttempts.get(shuffleKey)(mapId)
          }
          logWarning(s"Append data failed for task(shuffle $shuffleKey, map $mapId, attempt" +
            s" $attemptId), caused by ${e.getMessage}")
        case e: Exception =>
          logError("Exception encountered when write.", e)
      }
      index += 1
    }
  }

  override def handleOpenStream(
      shuffleKey: String,
      fileName: String,
      startMapIndex: Int,
      endMapIndex: Int): FileInfo = {
    // find FileWriter responsible for the data
    val fileWriter = localStorageManager.getWriter(shuffleKey, fileName)
    if (fileWriter eq null) {
      logWarning(s"File $fileName for $shuffleKey was not found!")
      return null
    }
    partitionsSorter.openStream(shuffleKey, fileName, fileWriter, startMapIndex, endMapIndex);
  }

  private def registerWithMaster() {
    var registerTimeout = RssConf.registerWorkerTimeoutMs(conf)
    val delta = 2000
    while (registerTimeout > 0) {
      val rsp = try {
        rssHARetryClient.askSync[RegisterWorkerResponse](
          RegisterWorker(host, rpcPort, pushPort, fetchPort, replicatePort, workerInfo.numSlots),
          classOf[RegisterWorkerResponse])
      } catch {
        case throwable: Throwable =>
          logError(s"Register worker to master failed, will retry after 2s.", throwable)
          null
      }
      // Register successfully
      if (null != rsp && rsp.success) {
        registered.set(true)
        logInfo("Register worker successfully.")
        return
      }
      // Register failed, sleep and retry
      Thread.sleep(delta)
      registerTimeout = registerTimeout - delta
    }
    // If worker register still failed after retry, throw exception to stop worker process
    throw new RssException("Register worker failed.")
  }

  private val cleanTaskQueue = new LinkedBlockingQueue[jHashSet[String]]
  private val cleaner = new Thread("Cleaner") {
    override def run(): Unit = {
      while (true) {
        val expiredShuffleKeys = cleanTaskQueue.take()
        try {
          cleanup(expiredShuffleKeys)
        } catch {
          case e: Throwable =>
            logError("Cleanup failed", e)
        }
      }
    }
  }
  cleaner.setDaemon(true)
  cleaner.start()

  private def cleanup(expiredShuffleKeys: jHashSet[String]): Unit = synchronized {
    expiredShuffleKeys.asScala.foreach { shuffleKey =>
      partitionLocationInfo.getAllMasterLocations(shuffleKey).asScala.foreach { partition =>
        partition.asInstanceOf[WorkingPartition].getFileWriter.destroy()
      }
      partitionLocationInfo.getAllSlaveLocations(shuffleKey).asScala.foreach { partition =>
        partition.asInstanceOf[WorkingPartition].getFileWriter.destroy()
      }
      partitionLocationInfo.removeMasterPartitions(shuffleKey)
      partitionLocationInfo.removeSlavePartitions(shuffleKey)
      shuffleMapperAttempts.remove(shuffleKey)
      logInfo(s"Cleaned up expired shuffle $shuffleKey")
    }
    partitionsSorter.cleanup(expiredShuffleKeys)
    localStorageManager.cleanupExpiredShuffleKey(expiredShuffleKeys)
  }

  def isRegistered: Boolean = {
    registered.get()
  }

  private def createTransportServer(
      conf: RssConf,
      module: String,
      port: Int,
      defaultIOThreads: Int,
      isPushDataServe: Boolean): TransportServer = {
    val closeIdleConnections = RssConf.closeIdleConnections(conf)
    val numThreads = conf.getInt(s"rss.$module.io.threads", defaultIOThreads)
    val transportConf = Utils.fromRssConf(conf, module, numThreads)
    val transportContext = if (isPushDataServe) {
      val limiter = new ChannelsLimiter(module)
      val rpcHandler = new PushDataRpcHandler(transportConf, this)
      new TransportContext(transportConf, rpcHandler, closeIdleConnections, workerSource, limiter)
    } else {
      val rpcHandler = new ChunkFetchRpcHandler(transportConf, workerSource, this)
      new TransportContext(transportConf, rpcHandler, closeIdleConnections, workerSource)
    }
    val serverBootstraps = new jArrayList[TransportServerBootstrap]()
    transportContext.createServer(port, serverBootstraps)
  }

  private def createTransportClientFactory(
      conf: RssConf,
      module: String,
      defaultIOThreads: Int): TransportClientFactory = {
    val closeIdleConnections = RssConf.closeIdleConnections(conf)
    val numThreads = conf.getInt(s"rss.$module.io.threads", defaultIOThreads)
    val transportConf = Utils.fromRssConf(conf, module, numThreads)
    val rpcHandler = new PushDataRpcHandler(transportConf, this)
    val limiter = new ChannelsLimiter(module)
    val transportContext = new TransportContext(
      transportConf, rpcHandler, closeIdleConnections, workerSource, limiter)
    val clientBootstraps = new jArrayList[TransportClientBootstrap]()
    transportContext.createClientFactory(clientBootstraps)
  }
}

private[deploy] object Worker extends Logging {
  def main(args: Array[String]): Unit = {
    val conf = new RssConf
    val workerArgs = new WorkerArguments(args, conf)
    // There are many entries for setting the master address, and we should unify the entries as
    // much as possible. Therefore, if the user manually specifies the address of the Master when
    // starting the Worker, we should set it in the parameters and automatically calculate what the
    // address of the Master should be used in the end.
    if (workerArgs.master != null) {
      conf.set("rss.master.address", RpcAddress.fromRssURL(workerArgs.master).toString)
    }

    val metricsSystem = MetricsSystem.createMetricsSystem("worker", conf, SERVLET_PATH)

    val rpcEnv = RpcEnv.create(
      RpcNameConstants.WORKER_SYS,
      workerArgs.host,
      workerArgs.host,
      workerArgs.port,
      conf,
      Math.max(64, Runtime.getRuntime.availableProcessors))
    rpcEnv.setupEndpoint(RpcNameConstants.WORKER_EP, new Worker(rpcEnv, conf, metricsSystem))

    if (RssConf.metricsSystemEnable(conf)) {
      logInfo(s"Metrics system enabled!")
      metricsSystem.start()

      var port = RssConf.workerPrometheusMetricPort(conf)
      var initialized = false
      while (!initialized) {
        try {
          val httpServer = new HttpServer(
            new HttpServerInitializer(
              new HttpRequestHandler(metricsSystem.getPrometheusHandler)), port)
          httpServer.start
          initialized = true
        } catch {
          case e: Exception =>
            logWarning(s"HttpServer pushPort $port may already exist, try pushPort ${port + 1}.", e)
            port += 1
            Thread.sleep(1000)
        }
      }
    }

    rpcEnv.awaitTermination
  }
}
