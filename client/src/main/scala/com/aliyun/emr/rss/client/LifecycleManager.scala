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
import java.util.concurrent.{ConcurrentHashMap, ScheduledFuture, TimeUnit}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

import io.netty.util.internal.ConcurrentSet

import com.aliyun.emr.rss.common.RssConf
import com.aliyun.emr.rss.common.haclient.RssHARetryClient
import com.aliyun.emr.rss.common.internal.Logging
import com.aliyun.emr.rss.common.meta.{PartitionLocationInfo, WorkerInfo}
import com.aliyun.emr.rss.common.protocol.PartitionLocation
import com.aliyun.emr.rss.common.protocol.RpcNameConstants.{RSS_METASERVICE_EP, RSS_METASERVICE_SYS, WORKER_EP}
import com.aliyun.emr.rss.common.protocol.message.ControlMessages._
import com.aliyun.emr.rss.common.protocol.message.StatusCode
import com.aliyun.emr.rss.common.rpc._
import com.aliyun.emr.rss.common.rpc.netty.{NettyRpcEndpointRef, NettyRpcEnv}
import com.aliyun.emr.rss.common.util.{ThreadUtils, Utils}

class LifecycleManager(appId: String, val conf: RssConf) extends RpcEndpoint with Logging {

  private val lifecycleHost = Utils.localHostName()

  private val RemoveShuffleDelayMs = RssConf.removeShuffleDelayMs(conf)
  private val GetBlacklistDelayMs = RssConf.getBlacklistDelayMs(conf)
  private val ShouldReplicate = RssConf.replicate(conf)
  private val splitThreshold = RssConf.partitionSplitThreshold(conf)
  private val splitMode = RssConf.partitionSplitMode(conf)

  private val unregisterShuffleTime = new ConcurrentHashMap[Int, Long]()

  private val registeredShuffle = new ConcurrentSet[Int]()
  private val shuffleMapperAttempts = new ConcurrentHashMap[Int, Array[Int]]()
  private val reducerFileGroupsMap =
    new ConcurrentHashMap[Int, Array[Array[PartitionLocation]]]()
  private val dataLostShuffleSet = new ConcurrentSet[Int]()
  private val stageEndShuffleSet = new ConcurrentSet[Int]()
  private val shuffleAllocatedWorkers =
    new ConcurrentHashMap[Int, ConcurrentHashMap[WorkerInfo, PartitionLocationInfo]]()
  private def workerSnapshots(shuffleId: Int): util.Map[WorkerInfo, PartitionLocationInfo] =
    shuffleAllocatedWorkers.get(shuffleId)

  // revive request waiting for response
  // shuffleKey -> (partitionId -> set)
  private val reviving =
    new ConcurrentHashMap[Int, ConcurrentHashMap[Integer, util.Set[RpcCallContext]]]()

  private val splitting =
    new ConcurrentHashMap[Int, ConcurrentHashMap[Integer, util.Set[RpcCallContext]]]()

  // register shuffle request waiting for response
  private val registerShuffleRequest = new ConcurrentHashMap[Int, util.Set[RpcCallContext]]()

  // blacklist
  private val blacklistManager = new BlacklistManager(rpcEnv, shuffleAllocatedWorkers)

  // Threads
  private val forwardMessageThread =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("master-forward-message-thread")
  private var checkForShuffleRemoval: ScheduledFuture[_] = _
  private var getBlacklist: ScheduledFuture[_] = _

  // Use independent app heartbeat threads to avoid being blocked by other operations.
  private val heartbeatThread = ThreadUtils.newDaemonSingleThreadScheduledExecutor("app-heartbeat")
  private var appHeartbeat: ScheduledFuture[_] = _
  private val responseCheckerThread = ThreadUtils.
    newDaemonSingleThreadScheduledExecutor("rss-master-resp-checker")

  // init driver rss meta rpc service
  override val rpcEnv: RpcEnv = RpcEnv.create(
    RSS_METASERVICE_SYS,
    lifecycleHost,
    RssConf.driverMetaServicePort(conf),
    conf)
  rpcEnv.setupEndpoint(RSS_METASERVICE_EP, this)

  logInfo(s"Start LifecycleManager on ${rpcEnv.address}")

  private val rssHARetryClient = new RssHARetryClient(rpcEnv, conf)

  // Since method `onStart` is executed when `rpcEnv.setupEndpoint` is executed, and
  // `rssHARetryClient` is initialized after `rpcEnv` is initialized, if method `onStart` contains
  // a reference to `rssHARetryClient`, there may be cases where `rssHARetryClient` is null when
  // `rssHARetryClient` is called. Therefore, it's necessary to uniformly execute the initialization
  // method at the end of the construction of the class to perform the initialization operations.
  private def initialize(): Unit = {
    appHeartbeat = heartbeatThread.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = {
        try {
          require(rssHARetryClient != null, "When sending a heartbeat, client shouldn't be null.")
          val appHeartbeat = HeartBeatFromApplication(appId, ZERO_UUID)
          rssHARetryClient.send(appHeartbeat)
          logDebug("Successfully send app heartbeat.")
        } catch {
          case it: InterruptedException =>
            logWarning("Interrupted while sending app heartbeat.")
            Thread.currentThread().interrupt()
            throw it
          case t: Throwable =>
            logError("Error while send heartbeat", t)
        }
      }
    }, 0, 30, TimeUnit.SECONDS)
  }

  override def onStart(): Unit = {
    checkForShuffleRemoval = forwardMessageThread.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = Utils.tryLogNonFatalError {
        self.send(RemoveExpiredShuffle)
      }
    }, RemoveShuffleDelayMs, RemoveShuffleDelayMs, TimeUnit.MILLISECONDS)

    getBlacklist = forwardMessageThread.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = Utils.tryLogNonFatalError {
        self.send(GetBlacklist(new util.ArrayList[WorkerInfo](
          blacklistManager.getBlacklist())))
      }
    }, GetBlacklistDelayMs, GetBlacklistDelayMs, TimeUnit.MILLISECONDS)
  }

  override def onStop(): Unit = {
    import scala.concurrent.duration._

    checkForShuffleRemoval.cancel(true)
    getBlacklist.cancel(true)
    ThreadUtils.shutdown(forwardMessageThread, 800.millis)

    appHeartbeat.cancel(true)
    ThreadUtils.shutdown(heartbeatThread, 800.millis)

    ThreadUtils.shutdown(responseCheckerThread, 800.millis)

    rssHARetryClient.close()
    if (rpcEnv != null) {
      rpcEnv.shutdown()
      rpcEnv.awaitTermination()
    }
  }

  def getRssMetaServiceHost: String = {
    lifecycleHost
  }

  def getRssMetaServicePort: Int = {
    rpcEnv.address.port
  }

  override def receive: PartialFunction[Any, Unit] = {
    case RemoveExpiredShuffle =>
      removeExpiredShuffle()
    case msg: GetBlacklist =>
      handleGetBlacklist(msg)
    case StageEnd(applicationId, shuffleId) =>
      logInfo(s"Received StageEnd request, ${Utils.makeShuffleKey(applicationId, shuffleId)}.")
      handleStageEnd(null, applicationId, shuffleId)
    case UnregisterShuffle(applicationId, shuffleId, _) =>
      logInfo(s"Received UnregisterShuffle request," +
        s"${Utils.makeShuffleKey(applicationId, shuffleId)}.")
      handleUnregisterShuffle(null, applicationId, shuffleId)
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RegisterShuffle(applicationId, shuffleId, numMappers, numPartitions) =>
      logDebug(s"Received RegisterShuffle request, " +
        s"$applicationId, $shuffleId, $numMappers, $numPartitions.")
      handleRegisterShuffle(context, applicationId, shuffleId, numMappers,
        numPartitions)

    case Revive(applicationId, shuffleId, mapId, attemptId, reduceId, epoch, oldPartition, cause) =>
      logDebug(s"Received Revive request, " +
        s"$applicationId, $shuffleId, $mapId, $attemptId, ,$reduceId," +
        s" $epoch, $oldPartition, $cause.")
      handleRevive(context, applicationId, shuffleId, mapId, attemptId,
        reduceId, epoch, oldPartition, cause)

    case PartitionSplit(applicationId, shuffleId, reduceId, epoch, oldPartition) =>
      logDebug(s"Received split request, " +
        s"$applicationId, $shuffleId, $reduceId, $epoch, $oldPartition")
      handlePartitionSplitRequest(context, applicationId, shuffleId, reduceId, epoch, oldPartition)

    case MapperEnd(applicationId, shuffleId, mapId, attemptId, numMappers) =>
      logDebug(s"Received MapperEnd request, " +
        s"${Utils.makeMapKey(applicationId, shuffleId, mapId, attemptId)}.")
      handleMapperEnd(context, applicationId, shuffleId, mapId, attemptId, numMappers)

    case GetReducerFileGroup(applicationId: String, shuffleId: Int) =>
      logDebug(s"Received GetShuffleFileGroup request," +
        s"${Utils.makeShuffleKey(applicationId, shuffleId)}.")
      handleGetReducerFileGroup(context, shuffleId)

    case StageEnd(applicationId, shuffleId) =>
      logInfo(s"Received StageEnd request, ${Utils.makeShuffleKey(applicationId, shuffleId)}.")
      handleStageEnd(context, applicationId, shuffleId)
  }

  /* ========================================================== *
   |        START OF EVENT HANDLER                              |
   * ========================================================== */

  def handleRegisterShuffle(
    context: RpcCallContext,
    applicationId: String,
    shuffleId: Int,
    numMappers: Int,
    numPartitions: Int): Unit = {
    // check if same request already exists for the same shuffle.
    // If do, just register and return
    registerShuffleRequest.synchronized {
      if (registerShuffleRequest.containsKey(shuffleId)) {
        logInfo("[handleRegisterShuffle] request for same shuffleKey exists, just register")
        registerShuffleRequest.get(shuffleId).add(context)
        return
      } else {
        // check if shuffle is registered
        if (registeredShuffle.contains(shuffleId)) {
          val initialLocs = workerSnapshots(shuffleId)
            .values()
            .asScala
            .flatMap(_.getAllMasterLocationsWithMinEpoch(shuffleId.toString).asScala)
            .filter(_.getEpoch == 0)
            .toList
            .asJava
          logDebug(s"Shuffle $shuffleId already registered, just return.")
          if (initialLocs.size != numPartitions) {
            logWarning(s"Shuffle $shuffleId location size ${initialLocs.size} not equal to " +
              s"numPartitions: $numPartitions!")
          }
          context.reply(RegisterShuffleResponse(StatusCode.Success, initialLocs))
          return
        }
        logInfo(s"New shuffle request, shuffleId $shuffleId, numPartitions: $numPartitions.")
        val set = new util.HashSet[RpcCallContext]()
        set.add(context)
        registerShuffleRequest.put(shuffleId, set)
      }
    }

    // get slots from Master
    val reduceIdList = new util.ArrayList[Integer]
    (0 until numPartitions).foreach(x => reduceIdList.add(new Integer(x)))
    val res = requestSlotsWithRetry(applicationId, shuffleId, reduceIdList)
    if (res.status != StatusCode.Success) {
      logError(s"OfferSlots for $shuffleId failed!")
      registerShuffleRequest.synchronized {
        val set = registerShuffleRequest.get(shuffleId)
        set.asScala.foreach { context =>
          context.reply(RegisterShuffleResponse(StatusCode.SlotNotAvailable, null))
        }
        registerShuffleRequest.remove(shuffleId)
      }
      return
    } else {
      logInfo(s"OfferSlots for ${Utils.makeShuffleKey(applicationId, shuffleId)} Success!")
      logDebug(s" Slots Info: ${res.workerResource}")
    }

    // reserve buffers
    val slots = res.workerResource
    val candidatesWorkers = new util.HashSet(slots.keySet())
    val connectFailedWorkers = new util.ArrayList[WorkerInfo]()
    slots.asScala.foreach(entry => {
      val workerInfo = entry._1
      try {
        workerInfo.endpoint = rpcEnv.setupEndpointRef(
          RpcAddress.apply(workerInfo.host, workerInfo.rpcPort), WORKER_EP)
        workerInfo.endpoint.asInstanceOf[NettyRpcEndpointRef].client =
          rpcEnv.asInstanceOf[NettyRpcEnv].clientFactory.createClient(workerInfo.host,
            workerInfo.rpcPort)
      } catch {
        case t: Throwable =>
          logError(s"Init rpc client for $workerInfo failed", t)
          connectFailedWorkers.add(workerInfo)
      }
    })

    candidatesWorkers.removeAll(connectFailedWorkers)

    recordWorkerFailure(connectFailedWorkers)

    val reserveSlotsSuccess = reserveSlotsWithRetry(applicationId, shuffleId,
      candidatesWorkers.asScala.toList, slots)

    // reserve buffers failed, clear allocated resources
    if (!reserveSlotsSuccess) {
      logError(s"reserve buffer for $shuffleId failed, reply to all.")
      registerShuffleRequest.synchronized {
        val set = registerShuffleRequest.get(shuffleId)
        set.asScala.foreach { context =>
          context.reply(RegisterShuffleResponse(StatusCode.ReserveSlotFailed, null))
        }
        registerShuffleRequest.remove(shuffleId)
      }
      // tell Master to release slots
      requestReleaseSlots(rssHARetryClient, ReleaseSlots(applicationId, shuffleId,
        new util.ArrayList[String](), new util.ArrayList[Integer]()))
      return
    } else {
      logInfo(s"ReserveSlots for ${Utils.makeShuffleKey(applicationId, shuffleId)} success!")
      logDebug(s"Allocated Slots: $slots")
    }

    val allocatedWorkers = new ConcurrentHashMap[WorkerInfo, PartitionLocationInfo]()
    slots.asScala.foreach(entry => {
      val workerInfo = entry._1
      // create client
      val masterLocations = entry._2._1
      val slaveLocations = entry._2._2
      val partitionLocationInfo = new PartitionLocationInfo()
      partitionLocationInfo.addMasterPartitions(shuffleId.toString, masterLocations)
      partitionLocationInfo.addSlavePartitions(shuffleId.toString, slaveLocations)
      allocatedWorkers.put(workerInfo, partitionLocationInfo)
    })

    shuffleAllocatedWorkers.put(shuffleId, allocatedWorkers)

    // register shuffle success, update status
    registeredShuffle.add(shuffleId)
    val locations = slots.asScala.flatMap(_._2._1.asScala).toList
    shuffleMapperAttempts.synchronized {
      if (!shuffleMapperAttempts.containsKey(shuffleId)) {
        val attempts = new Array[Int](numMappers)
        0 until numMappers foreach (idx => attempts(idx) = -1)
        shuffleMapperAttempts.synchronized {
          shuffleMapperAttempts.put(shuffleId, attempts)
        }
      }
    }

    reducerFileGroupsMap.put(shuffleId, new Array[Array[PartitionLocation]](numPartitions))

    logInfo(s"Handle RegisterShuffle Success for $shuffleId.")
    registerShuffleRequest.synchronized {
      val set = registerShuffleRequest.get(shuffleId)
      set.asScala.foreach { context =>
        context.reply(RegisterShuffleResponse(StatusCode.Success, locations.asJava))
      }
      registerShuffleRequest.remove(shuffleId)
    }
  }

  def blacklistPartition(oldPartition: PartitionLocation, cause: StatusCode): Unit = {
    // only blacklist if cause is PushDataFailMain
    val failedWorker = new util.ArrayList[WorkerInfo]()
    if (cause == StatusCode.PushDataFailMain) {
      failedWorker.add(oldPartition.getWorker)
    }
    if (!failedWorker.isEmpty) {
      recordWorkerFailure(failedWorker)
    }
  }

  private def handleRevive(
    context: RpcCallContext,
    applicationId: String,
    shuffleId: Int,
    mapId: Int,
    attemptId: Int,
    reduceId: Int,
    oldEpoch: Int,
    oldPartition: PartitionLocation,
    cause: StatusCode): Unit = {
    // check whether shuffle has registered
    if (!registeredShuffle.contains(shuffleId)) {
      logError(s"[handleRevive] shuffle $shuffleId not registered!")
      context.reply(ChangeLocationResponse(StatusCode.ShuffleNotRegistered, null))
      return
    }
    if (shuffleMapperAttempts.containsKey(shuffleId)
      && shuffleMapperAttempts.get(shuffleId)(mapId) != -1) {
      logWarning(s"[handleRevive] Mapper ended, mapId $mapId, current attemptId $attemptId, " +
        s"ended attemptId ${shuffleMapperAttempts.get(shuffleId)(mapId)}, shuffleId $shuffleId.")
      context.reply(ChangeLocationResponse(StatusCode.MapEnded, null))
      return
    }

    // check if there exists request for the partition, if do just register
    val shuffleReviving = reviving.computeIfAbsent(shuffleId, rpcContextRegisterFunc)
    shuffleReviving.synchronized {
      if (shuffleReviving.containsKey(reduceId)) {
        shuffleReviving.get(reduceId).add(context)
        logInfo(s"For $shuffleId, same partition $reduceId-$oldEpoch is reviving," +
          s"register context.")
        return
      } else {
        // check if new slot for the partition has allocated
        val latestLoc = getLatestPartition(shuffleId, reduceId, oldEpoch)
        if (latestLoc != null) {
          context.reply(ChangeLocationResponse(StatusCode.Success, latestLoc))
          logInfo(s"New partition found, old partition $reduceId-$oldEpoch return it." +
            s" shuffleId: $shuffleId $latestLoc")
          return
        }
        // no newer partition, register and allocate
        val set = new util.HashSet[RpcCallContext]()
        set.add(context)
        shuffleReviving.put(reduceId, set)
      }
    }

    logWarning(s"Do Revive for shuffle ${
      Utils.makeShuffleKey(applicationId, shuffleId)}, oldPartition: $oldPartition, cause: $cause")
    blacklistPartition(oldPartition, cause)
    handleChangePartitionLocation(shuffleReviving, applicationId, shuffleId, reduceId,
      oldPartition)
  }

  private val rpcContextRegisterFunc =
    new util.function.Function[Int, ConcurrentHashMap[Integer, util.Set[RpcCallContext]]]() {
      override def apply(s: Int): ConcurrentHashMap[Integer, util.Set[RpcCallContext]] =
        new ConcurrentHashMap()
    }

  private def handleChangePartitionLocation(
    contexts: ConcurrentHashMap[Integer, util.Set[RpcCallContext]],
    applicationId: String, shuffleId: Int, reduceId: Int, oldPartition: PartitionLocation): Unit = {
    val candidates = workersNotBlacklisted(shuffleId)
    val slots = reallocateSlotsFromCandidates(
      List(oldPartition), candidates)
    if (slots == null) {
      logError("[Update partition] failed for slot not available.")
      contexts.remove(reduceId).asScala.foreach(
        _.reply(ChangeLocationResponse(StatusCode.SlotNotAvailable, null)))
      return
    }

    val reserveSlotsSuccess = reserveSlotsWithRetry(applicationId, shuffleId, candidates, slots)
    if (!reserveSlotsSuccess) {
      logError(s"[Update partition] failed for $shuffleId.")
      contexts.remove(reduceId).asScala.foreach(
        _.reply(ChangeLocationResponse(StatusCode.ReserveSlotFailed, null)))
      return
    }

    slots.asScala.foreach(entry => {
      val partitionLocationInfo = workerSnapshots(shuffleId).get(entry._1)
      partitionLocationInfo.addMasterPartitions(shuffleId.toString, entry._2._1)
      partitionLocationInfo.addSlavePartitions(shuffleId.toString, entry._2._2)
    })
    val (masters, slaves) = slots.asScala.head._2
    val location = if (masters != null && masters.size() > 0) {
      masters.get(0)
    } else {
      slaves.get(0).getPeer
    }

    logInfo(s"[Update partition] success for $shuffleId $location.")
    contexts.remove(reduceId).asScala.foreach(
      _.reply(ChangeLocationResponse(StatusCode.Success, location)))
    logInfo(s"Renew $shuffleId $reduceId partition success.")
  }

  private def getLatestPartition(shuffleId: Int, reduceId: Int, epoch: Int): PartitionLocation = {
    val locs = workerSnapshots(shuffleId).values().asScala
      .flatMap(_.getLocationWithMaxEpoch(shuffleId.toString, reduceId))
    if (!locs.isEmpty) {
      val latestLoc = locs.maxBy(_.getEpoch)
      if (latestLoc.getEpoch > epoch) {
        return latestLoc
      }
    }
    null
  }

  private def handlePartitionSplitRequest(
    context: RpcCallContext,
    applicationId: String,
    shuffleId: Int,
    reduceId: Int,
    oldEpoch: Int,
    oldPartition: PartitionLocation
  ): Unit = {
    val shuffleSplitting = splitting.computeIfAbsent(shuffleId, rpcContextRegisterFunc)
    shuffleSplitting.synchronized {
      if (shuffleSplitting.containsKey(reduceId)) {
        shuffleSplitting.get(reduceId).add(context)
        logInfo(s"For $shuffleId, same $reduceId-$oldEpoch is splitting, register context")
        return
      } else {
        val latestLoc = getLatestPartition(shuffleId, reduceId, oldEpoch)
        if (latestLoc != null) {
          context.reply(ChangeLocationResponse(StatusCode.Success, latestLoc))
          logInfo(s"Split request found new partition, old partition $reduceId-$oldEpoch" +
            s" return it. shuffleId: $shuffleId $latestLoc")
          return
        }
        val set = new util.HashSet[RpcCallContext]()
        set.add(context)
        shuffleSplitting.put(reduceId, set)
      }
    }

    logInfo(s"Relocate partition for shuffle split  ${Utils.makeShuffleKey(applicationId,
      shuffleId)}, oldPartition: $oldPartition")

    handleChangePartitionLocation(shuffleSplitting, applicationId, shuffleId, reduceId,
      oldPartition)
  }

  private def handleMapperEnd(
    context: RpcCallContext,
    applicationId: String,
    shuffleId: Int,
    mapId: Int,
    attemptId: Int,
    numMappers: Int): Unit = {
    var askStageEnd: Boolean = false
    // update max attemptId
    shuffleMapperAttempts.synchronized {
      var attempts = shuffleMapperAttempts.get(shuffleId)
      // it would happen when task with no shuffle data called MapperEnd first
      if (attempts == null) {
        logInfo(s"[handleMapperEnd] $shuffleId not registered, create one.")
        attempts = new Array[Int](numMappers)
        0 until numMappers foreach (ind => attempts(ind) = -1)
        shuffleMapperAttempts.put(shuffleId, attempts)
      }

      if (attempts(mapId) < 0) {
        attempts(mapId) = attemptId
      } else {
        // Mapper with another attemptId called, skip this request
        context.reply(MapperEndResponse(StatusCode.Success))
        return
      }

      if (!attempts.exists(_ < 0)) {
        askStageEnd = true
      }
    }

    if (askStageEnd) {
      // last mapper finished. call mapper end
      logInfo(s"Last MapperEnd, call StageEnd with shuffleKey:" +
        s"${Utils.makeShuffleKey(applicationId, shuffleId)}.")
      self.send(StageEnd(applicationId, shuffleId))
    }

    // reply success
    context.reply(MapperEndResponse(StatusCode.Success))
  }

  private def handleGetReducerFileGroup(
    context: RpcCallContext,
    shuffleId: Int): Unit = {
    logDebug(s"Wait for StageEnd, $shuffleId.")
    var timeout = RssConf.stageEndTimeout(conf)
    val delta = 50
    while (!stageEndShuffleSet.contains(shuffleId)) {
      Thread.sleep(50)
      if (timeout <= 0) {
        logError(s"StageEnd Timeout! $shuffleId.")
        context.reply(GetReducerFileGroupResponse(StatusCode.Failed, null, null))
        return
      }
      timeout = timeout - delta
    }
    logDebug(s"Start getting reduce file group, $shuffleId.")

    if (dataLostShuffleSet.contains(shuffleId)) {
      context.reply(GetReducerFileGroupResponse(StatusCode.Failed, null, null))
    } else {
      val shuffleFileGroup = reducerFileGroupsMap.get(shuffleId)
      context.reply(GetReducerFileGroupResponse(
        StatusCode.Success,
        shuffleFileGroup,
        shuffleMapperAttempts.get(shuffleId)
      ))
    }
  }

  private def handleStageEnd(
    context: RpcCallContext,
    applicationId: String,
    shuffleId: Int): Unit = {
    // check whether shuffle has registered
    if (!registeredShuffle.contains(shuffleId)) {
      logInfo(s"[handleStageEnd]" +
        s"$shuffleId not registered, maybe no shuffle data within this stage.")
      // record in stageEndShuffleSet
      stageEndShuffleSet.add(shuffleId)
      if (context != null) {
        context.reply(StageEndResponse(StatusCode.ShuffleNotRegistered))
      }
      return
    }

    // ask allLocations workers holding partitions to commit files
    val masterPartMap = new ConcurrentHashMap[String, PartitionLocation]
    val slavePartMap = new ConcurrentHashMap[String, PartitionLocation]
    val committedMasterIds = new ConcurrentSet[String]
    val committedSlaveIds = new ConcurrentSet[String]
    val failedMasterIds = new ConcurrentSet[String]
    val failedSlaveIds = new ConcurrentSet[String]

    val allocatedWorkers = shuffleAllocatedWorkers.get(shuffleId)
    val commitFilesFailedWorkers = new ConcurrentSet[WorkerInfo]

    val parallelism = Math.min(workerSnapshots(shuffleId).size(),
      RssConf.rpcMaxParallelism(conf))
    ThreadUtils.parmap(
      allocatedWorkers.asScala.to, "CommitFiles", parallelism) { w2p =>
      val worker = w2p._1
      val partitionLocationInfo = w2p._2
      if (partitionLocationInfo.containsShuffle(shuffleId.toString)) {
        val masterParts = partitionLocationInfo.getAllMasterLocations(shuffleId.toString)
        val slaveParts = partitionLocationInfo.getAllSlaveLocations(shuffleId.toString)
        masterParts.asScala.foreach { p =>
          val partition = new PartitionLocation(p)
          partition.setFetchPort(worker.fetchPort)
          partition.setPeer(null)
          masterPartMap.put(partition.getUniqueId, partition)
        }
        slaveParts.asScala.foreach { p =>
          val partition = new PartitionLocation(p)
          partition.setFetchPort(worker.fetchPort)
          partition.setPeer(null)
          slavePartMap.put(partition.getUniqueId, partition)
        }

        val masterIds = masterParts.asScala.map(_.getUniqueId).asJava
        val slaveIds = slaveParts.asScala.map(_.getUniqueId).asJava

        val res = requestCommitFiles(worker.endpoint,
          CommitFiles(applicationId, shuffleId, masterIds, slaveIds,
            shuffleMapperAttempts.get(shuffleId)))

        if (res.status != StatusCode.Success) {
          commitFilesFailedWorkers.add(worker)
        }

        // record committed partitionIds
        if (res.committedMasterIds != null) {
          committedMasterIds.addAll(res.committedMasterIds)
        }
        if (res.committedSlaveIds != null) {
          committedSlaveIds.addAll(res.committedSlaveIds)
        }

        // record failed partitions
        if (res.failedMasterIds != null) {
          failedMasterIds.addAll(res.failedMasterIds)
        }
        if (res.failedSlaveIds != null) {
          failedSlaveIds.addAll(res.failedSlaveIds)
        }
      }
    }

    recordWorkerFailure(new util.ArrayList[WorkerInfo](commitFilesFailedWorkers))
    // release resources and clear worker info
    workerSnapshots(shuffleId).asScala.foreach { w2p =>
      val partitionLocationInfo = w2p._2
      partitionLocationInfo.removeMasterPartitions(shuffleId.toString)
      partitionLocationInfo.removeSlavePartitions(shuffleId.toString)
    }
    requestReleaseSlots(rssHARetryClient, ReleaseSlots(applicationId, shuffleId,
      new util.ArrayList[String](), new util.ArrayList[Integer]()))

    def hasCommonFailedIds(): Boolean = {
      if (!ShouldReplicate && failedMasterIds.size() != 0) {
        return true
      }
      failedMasterIds.asScala.foreach { id =>
        if (failedSlaveIds.contains(id)) {
          logError(s"For $shuffleId partition $id: data lost.")
          return true
        }
      }
      false
    }

    val dataLost = hasCommonFailedIds()

    if (!dataLost) {
      val committedPartitions = new util.HashMap[String, PartitionLocation]
      committedMasterIds.asScala.foreach { id =>
        committedPartitions.put(id, masterPartMap.get(id))
      }
      committedSlaveIds.asScala.foreach { id =>
        val slavePartition = slavePartMap.get(id)
        val masterPartition = committedPartitions.get(id)
        if (masterPartition ne null) {
          masterPartition.setPeer(slavePartition)
          slavePartition.setPeer(masterPartition)
        } else {
          logWarning(s"Shuffle $shuffleId partition $id: master lost, " +
            s"use slave $slavePartition.")
          committedPartitions.put(id, slavePartition)
        }
      }

      val fileGroups = reducerFileGroupsMap.get(shuffleId)
      val sets = Array.fill(fileGroups.length)(new util.HashSet[PartitionLocation]())
      committedPartitions.values().asScala.foreach { partition =>
        sets(partition.getReduceId).add(partition)
      }
      var i = 0
      while (i < fileGroups.length) {
        fileGroups(i) = sets(i).toArray(new Array[PartitionLocation](0))
        i += 1
      }
    }

    // reply
    if (!dataLost) {
      logInfo(s"Succeed to handle stageEnd for $shuffleId.")
      // record in stageEndShuffleSet
      stageEndShuffleSet.add(shuffleId)
      if (context != null) {
        context.reply(StageEndResponse(StatusCode.Success))
      }
    } else {
      logError(s"Failed to handle stageEnd for $shuffleId, lost file!")
      dataLostShuffleSet.add(shuffleId)
      // record in stageEndShuffleSet
      stageEndShuffleSet.add(shuffleId)
      if (context != null) {
        context.reply(StageEndResponse(StatusCode.PartialSuccess))
      }
    }
  }

  def handleUnregisterShuffle(context: RpcCallContext, appId: String, shuffleId: Int): Unit = {
    // if StageEnd has not been handled, trigger StageEnd
    if (!stageEndShuffleSet.contains(shuffleId)) {
      logInfo(s"Call StageEnd before Unregister Shuffle $shuffleId.")
      handleStageEnd(null, appId, shuffleId)
    }

    if (partitionExists(shuffleId)) {
      logWarning(s"Partition exists for shuffle $shuffleId, " +
        "maybe caused by task rerun or speculative.")
      workerSnapshots(shuffleId).asScala.foreach { w2p =>
        val partitionLocationInfo = w2p._2
        partitionLocationInfo.removeMasterPartitions(shuffleId.toString)
        partitionLocationInfo.removeSlavePartitions(shuffleId.toString)
      }
      requestReleaseSlots(rssHARetryClient, ReleaseSlots(appId, shuffleId,
        new util.ArrayList[String](), new util.ArrayList[Integer]()))
    }

    // add shuffleKey to delay shuffle removal set
    unregisterShuffleTime.put(shuffleId, System.currentTimeMillis())

    logInfo(s"Unregister for $shuffleId success.")
    if (context != null) {
      context.reply(UnregisterShuffleResponse(StatusCode.Success))
    }
  }

  /* ========================================================== *
   |        END OF EVENT HANDLER                                |
   * ========================================================== */

  def reserveSlots(
    applicationId: String, shuffleId: Int, slots: WorkerResource): util.List[WorkerInfo] = {
    val failed = new util.ArrayList[WorkerInfo]()

    slots.asScala.foreach { entry =>
      if (blacklistManager.contains(entry._1)) {
        logWarning(s"[reserve buffer] failed due to blacklist: ${entry._1}")
        failed.add(entry._1)
      } else {
        val res = requestReserveSlots(entry._1.endpoint,
          ReserveSlots(applicationId, shuffleId, entry._2._1, entry._2._2, splitThreshold,
            splitMode))
        if (res.status.equals(StatusCode.Success)) {
          logDebug(s"Successfully allocated " +
            s"partitions buffer for ${Utils.makeShuffleKey(applicationId, shuffleId)}" +
            s" from worker ${entry._1.readableAddress}.")
        } else {
          logError(s"[reserveSlots] Failed to" +
            s" reserve buffers for ${Utils.makeShuffleKey(applicationId, shuffleId)}" +
            s" from worker ${entry._1.readableAddress}. Reason: ${res.reason}")
          failed.add(entry._1)
        }
      }
    }

    recordWorkerFailure(failed)

    failed
  }

  /**
   * reserve buffer with retry, retry on another node will cause slots inconsistent
   */
  def reserveSlotsWithRetry(applicationId: String, shuffleId: Int, candidates: List[WorkerInfo],
    slots: WorkerResource): Boolean = {
    // reserve buffers
    val failed = reserveSlots(applicationId, shuffleId, slots)

    val retryReserveSlotsSuccess = if (!failed.isEmpty) {
      var retrySuccess = true
      logWarning("ReserveSlots failed once, retry again")
      // get failed partition locations
      val failedPartitionLocations = new mutable.HashMap[Int, PartitionLocation]()
      failed.asScala.foreach(workerInfo => {
        val (failedMasterLocations, failedSlaveLocations) = slots.remove(workerInfo)
        if (null != failedMasterLocations) {
          failedMasterLocations.asScala.foreach(failedMaster => {
            failedPartitionLocations += (failedMaster.getReduceId -> failedMaster)
          })
        }
        if (null != failedSlaveLocations) {
          failedSlaveLocations.asScala.foreach(failedSlave => {
            val reduceId = failedSlave.getReduceId
            if (!failedPartitionLocations.contains(reduceId)) {
              failedPartitionLocations += (reduceId -> failedSlave)
            }
          })
        }
        // remove failed slot from total slots, close transport client
        if (workerInfo.endpoint != null) {
          val transportClient = workerInfo.endpoint.asInstanceOf[NettyRpcEndpointRef].client
          if (null != transportClient && transportClient.isActive) {
            transportClient.close()
          }
        } else {
          blacklistManager.addBlacklist(workerInfo)
        }
      })

      val newMapFunc = new util.function.Function[WorkerInfo,
        (util.List[PartitionLocation], util.List[PartitionLocation])] {
        override def apply(w: WorkerInfo):
        (util.List[PartitionLocation], util.List[PartitionLocation]) =
          (new util.LinkedList[PartitionLocation](),
            new util.LinkedList[PartitionLocation]())
      }

      if (ShouldReplicate && failedPartitionLocations.nonEmpty && !slots.isEmpty) {
        // get partition locations that need to destroy
        val destroyPartitionLocations = ListBuffer[PartitionLocation]()
        failedPartitionLocations.values.foreach(partition => {
          if (null != partition.getPeer) {
            destroyPartitionLocations.append(partition.getPeer)
          }
        })
        // destroy success buffers those another peer is failed
        if (destroyPartitionLocations.nonEmpty) {
          val destroyResource = new WorkerResource
          val workInfos = slots.keySet().asScala
          // remove from slots
          destroyPartitionLocations.foreach(partition => {
            var tmpWorkerInfo = new WorkerInfo(partition.getHost, partition.getRpcPort, partition
              .getPushPort, partition.getFetchPort, partition.getReplicatePort)
            val workerInfoWithRpcRef = workInfos.find(worker => worker.equals(tmpWorkerInfo))
              .getOrElse({
                logWarning(s"Cannot find workInfo from previous success workResource:" +
                  s" ${tmpWorkerInfo.readableAddress()}, init according to partition info")
                try {
                  tmpWorkerInfo.endpoint = rpcEnv.setupEndpointRef(
                    RpcAddress.apply(tmpWorkerInfo.host, tmpWorkerInfo.rpcPort),
                    WORKER_EP)
                } catch {
                  case t: Throwable =>
                    logWarning(s"Init rpc client failed for ${tmpWorkerInfo
                      .readableAddress()}, exception: ${t.getMessage}")
                    tmpWorkerInfo = null
                }
                tmpWorkerInfo
              })
            if (null != workerInfoWithRpcRef) {
              val (masterPartitionLocations, slavePartitionLocations) =
                slots.getOrDefault(workerInfoWithRpcRef, (null, null))
              if (partition.getMode.equals(PartitionLocation.Mode.Master) &&
                null != masterPartitionLocations) {
                masterPartitionLocations.remove(partition)
                destroyResource.computeIfAbsent(workerInfoWithRpcRef, newMapFunc)._1.add(partition)
              }
              if (partition.getMode.equals(PartitionLocation.Mode.Slave) &&
                null != slavePartitionLocations) {
                slavePartitionLocations.remove(partition)
                destroyResource.computeIfAbsent(workerInfoWithRpcRef, newMapFunc)._2.add(partition)
              }
            }
          })
          if (!destroyResource.isEmpty) {
            destroyBuffersWithRetry(applicationId, shuffleId, destroyResource)
            logInfo(s"Destroyed peer partitions for reserve buffer failed workers " +
              s"${Utils.makeShuffleKey(applicationId, shuffleId)}, $destroyResource")

            val workerIds = new util.ArrayList[String]()
            val slots = new util.ArrayList[Integer]()
            Utils.workerToAllocatedSlots(destroyResource).asScala.foreach(entry => {
              workerIds.add(entry._1.toUniqueId())
              slots.add(entry._2)
            })
            val msg = ReleaseSlots(applicationId, shuffleId, workerIds, slots)
            requestReleaseSlots(rssHARetryClient, msg)
            logInfo(s"Released slots for reserve buffer failed workers " +
              s"${workerIds.asScala.mkString(",")}" + s"${slots.asScala.mkString(",")}" +
              s"${Utils.makeShuffleKey(applicationId, shuffleId)}, ")
          }
        }
      }
      // get retryCandidates resource and retry reserve buffer
      val retryCandidates = new util.HashSet(slots.keySet())
      // add candidates to avoid revive action passed in slots only 2 worker
      retryCandidates.addAll(candidates.asJava)
      // remove blacklist from retryCandidates
      retryCandidates.removeAll(blacklistManager.getBlacklist())

      val retrySlots = reallocateSlotsFromCandidates(failedPartitionLocations.values.toList,
        retryCandidates.asScala.toList)

      if (null == retrySlots) {
        retrySuccess = false
      } else {
        // retry another worker if failed
        val failedAfterRetry = reserveSlots(applicationId, shuffleId, retrySlots)
        retrySuccess = failedAfterRetry.isEmpty
        if (retrySuccess) {
          // add success buffers to slots
          retrySlots.asScala.foreach(entry => {
            val (masterPartitionLocations, slavePartitionLocations) =
              slots.computeIfAbsent(entry._1, newMapFunc)
            if (null != entry._2._1) masterPartitionLocations.addAll(entry._2._1)
            if (null != entry._2._2) slavePartitionLocations.addAll(entry._2._2)
          })
        } else {
          // destroy success buffers
          val destroyAfterRetry = retrySlots.asScala.filterKeys(!failedAfterRetry.contains(_)).toMap
          destroyBuffersWithRetry(applicationId, shuffleId,
            destroyAfterRetry.asInstanceOf[WorkerResource])
        }
      }

      retrySuccess
    } else true

    // if failed after retry, destroy all allocated buffers
    if (!retryReserveSlotsSuccess) {
      logWarning(s"Reserve buffers $shuffleId still fail after retrying, clear buffers.")
      destroyBuffersWithRetry(applicationId, shuffleId, slots)
    } else {
      logInfo(s"Reserve buffer success for ${Utils.makeShuffleKey(applicationId, shuffleId)}")
    }

    retryReserveSlotsSuccess
  }

  def reallocateSlotsFromCandidates(oldPartitions: List[PartitionLocation],
    candidates: List[WorkerInfo]): WorkerResource = {
    if (candidates.size < 1 || (ShouldReplicate && candidates.size < 2)) {
      logError("Not enough candidates for revive")
      return null
    }

    val newMapFunc =
      new util.function.Function[WorkerInfo,
        (util.List[PartitionLocation], util.List[PartitionLocation])] {
        override def apply(w: WorkerInfo):
        (util.List[PartitionLocation], util.List[PartitionLocation]) =
          (new util.LinkedList[PartitionLocation](), new util.LinkedList[PartitionLocation]())
      }

    val slots = new WorkerResource()
    oldPartitions.foreach(partitionLocation => {
      val masterIndex = Random.nextInt(candidates.size)
      val masterLocation = new PartitionLocation(
        partitionLocation.getReduceId,
        partitionLocation.getEpoch + 1,
        candidates(masterIndex).host,
        candidates(masterIndex).rpcPort,
        candidates(masterIndex).pushPort,
        candidates(masterIndex).fetchPort,
        candidates(masterIndex).replicatePort,
        PartitionLocation.Mode.Master)

      if (ShouldReplicate) {
        val slaveIndex = (masterIndex + 1) % candidates.size
        val slaveLocation = new PartitionLocation(
          partitionLocation.getReduceId,
          partitionLocation.getEpoch + 1,
          candidates(slaveIndex).host,
          candidates(slaveIndex).rpcPort,
          candidates(slaveIndex).pushPort,
          candidates(slaveIndex).fetchPort,
          candidates(slaveIndex).replicatePort,
          PartitionLocation.Mode.Slave,
          masterLocation
        )
        masterLocation.setPeer(slaveLocation)
        val masterAndSlavePairs = slots.computeIfAbsent(candidates(slaveIndex), newMapFunc)
        masterAndSlavePairs._2.add(slaveLocation)
      }

      val masterAndSlavePairs = slots.computeIfAbsent(candidates(masterIndex), newMapFunc)
      masterAndSlavePairs._1.add(masterLocation)
    })
    slots
  }

  def destroyBuffersWithRetry(applicationId: String, shuffleId: Int,
    worker: WorkerResource): (util.List[String], util.List[String]) = {
    val failedMasters = new util.LinkedList[String]()
    val failedSlaves = new util.LinkedList[String]()

    val shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId)
    worker.asScala.foreach(entry => {
      var res = requestDestroy(entry._1.endpoint,
        Destroy(shuffleKey, entry._2._1.asScala.map(_.getUniqueId).asJava,
          entry._2._2.asScala.map(_.getUniqueId).asJava))
      if (res.status != StatusCode.Success) {
        res = requestDestroy(entry._1.endpoint,
          Destroy(shuffleKey, res.failedMasters, res.failedSlaves))
      }
      if (null != res.failedMasters) failedMasters.addAll(res.failedMasters)
      if (null != res.failedSlaves) failedSlaves.addAll(res.failedSlaves)
    })
    (failedMasters, failedSlaves)
  }

  private def removeExpiredShuffle(): Unit = {
    logInfo("Check for expired shuffle.")
    val currentTime = System.currentTimeMillis()
    val keys = unregisterShuffleTime.keys().asScala.toList
    keys.foreach { key =>
      if (unregisterShuffleTime.get(key) < currentTime - RemoveShuffleDelayMs) {
        logInfo(s"Clear shuffle $key.")
        // clear for the shuffle
        registeredShuffle.remove(key)
        registerShuffleRequest.remove(key)
        reducerFileGroupsMap.remove(key)
        dataLostShuffleSet.remove(key)
        shuffleMapperAttempts.remove(key)
        stageEndShuffleSet.remove(key)
        reviving.remove(key)
        splitting.remove(key)
        unregisterShuffleTime.remove(key)
        shuffleAllocatedWorkers.remove(key)

        requestUnregisterShuffle(rssHARetryClient, UnregisterShuffle(appId, key))
      }
    }
  }

  private def handleGetBlacklist(msg: GetBlacklist): Unit = {
    val res = requestGetBlacklist(rssHARetryClient, msg)
    if (res.statusCode == StatusCode.Success) {
      logInfo(s"Received Blacklist from Master, blacklist: ${res.blacklist} " +
        s"unkown workers: ${res.unknownWorkers}")
      blacklistManager.addBlacklists(res.blacklist)
      blacklistManager.addUnknownWorker(res.unknownWorkers)
    }
  }

  def requestSlotsWithRetry(
    applicationId: String,
    shuffleId: Int,
    reduceIdList: util.ArrayList[Integer]): RequestSlotsResponse = {
    val req = RequestSlots(
      applicationId, shuffleId, reduceIdList, lifecycleHost, ShouldReplicate)
    val res = requestRequestSlots(rssHARetryClient, req)
    if (res.status != StatusCode.Success) {
      requestRequestSlots(rssHARetryClient, req)
    } else {
      res
    }
  }

  private def requestRequestSlots(rssHARetryClient: RssHARetryClient,
    message: RequestSlots): RequestSlotsResponse = {
    val shuffleKey = Utils.makeShuffleKey(message.applicationId, message.shuffleId)
    try {
      rssHARetryClient.askSync[RequestSlotsResponse](message, classOf[RequestSlotsResponse])
    } catch {
      case e: Exception =>
        logError(s"AskSync RegisterShuffle for $shuffleKey failed.", e)
        RequestSlotsResponse(StatusCode.Failed, null)
    }
  }

  private def requestReserveSlots(
    endpoint: RpcEndpointRef, message: ReserveSlots): ReserveSlotsResponse = {
    val shuffleKey = Utils.makeShuffleKey(message.applicationId, message.shuffleId)
    try {
      endpoint.askSync[ReserveSlotsResponse](message)
    } catch {
      case e: Exception =>
        val msg = s"Exception when askSync ReserveSlots for $shuffleKey, ${e.getMessage}"
        ReserveSlotsResponse(StatusCode.Failed, msg)
    }
  }

  private def requestDestroy(endpoint: RpcEndpointRef, message: Destroy): DestroyResponse = {
    try {
      endpoint.askSync[DestroyResponse](message)
    } catch {
      case e: Exception =>
        logError(s"AskSync Destroy for ${message.shuffleKey} failed.", e)
        DestroyResponse(StatusCode.Failed, message.masterLocations, message.slaveLocations)
    }
  }

  private def requestCommitFiles(
    endpoint: RpcEndpointRef, message: CommitFiles): CommitFilesResponse = {
    try {
      endpoint.askSync[CommitFilesResponse](message)
    } catch {
      case e: Exception =>
        logError(s"AskSync CommitFiles for ${message.shuffleId} failed.", e)
        CommitFilesResponse(StatusCode.Failed, null, null, message.masterIds, message.slaveIds)
    }
  }

  private def requestReleaseSlots(rssHARetryClient: RssHARetryClient,
    message: ReleaseSlots): ReleaseSlotsResponse = {
    try {
      rssHARetryClient.askSync[ReleaseSlotsResponse](message, classOf[ReleaseSlotsResponse])
    } catch {
      case e: Exception =>
        logError(s"AskSync ReleaseSlots for ${message.shuffleId} failed.", e)
        ReleaseSlotsResponse(StatusCode.Failed)
    }
  }

  private def requestUnregisterShuffle(rssHARetryClient: RssHARetryClient,
    message: UnregisterShuffle): UnregisterShuffleResponse = {
    try {
      rssHARetryClient.askSync[UnregisterShuffleResponse](
        message, classOf[UnregisterShuffleResponse])
    } catch {
      case e: Exception =>
        logError(s"AskSync UnregisterShuffle for ${message.shuffleId} failed.", e)
        UnregisterShuffleResponse(StatusCode.Failed)
    }
  }

  private def requestGetBlacklist(rssHARetryClient: RssHARetryClient,
    msg: GetBlacklist): GetBlacklistResponse = {
    try {
      rssHARetryClient.askSync[GetBlacklistResponse](msg, classOf[GetBlacklistResponse])
    } catch {
      case e: Exception =>
        logError(s"AskSync GetBlacklist failed.", e)
        GetBlacklistResponse(StatusCode.Failed, null, null)
    }
  }

  private def recordWorkerFailure(failures: util.List[WorkerInfo]): Unit = {
    val failedWorker = new util.ArrayList[WorkerInfo](failures)
    failedWorker.removeAll(blacklistManager.getBlacklist())
    if (failedWorker.isEmpty) {
      return
    }
    logInfo(s"Report Worker Failure: ${failedWorker.asScala}," +
      s" current blacklist ${blacklistManager.getBlacklist()}")
    blacklistManager.addBlacklists(failedWorker)
  }

  def isClusterOverload(numPartitions: Int = 0): Boolean = {
    logInfo(s"Ask Sync Cluster Load Status")
    try {
      rssHARetryClient.askSync[GetClusterLoadStatusResponse](GetClusterLoadStatus(numPartitions),
        classOf[GetClusterLoadStatusResponse]).isOverload
    } catch {
      case e: Exception =>
        logError(s"AskSync Cluster Load Status failed.", e)
        true
    }
  }

  private def partitionExists(shuffleId: Int): Boolean = {
    val workers = workerSnapshots(shuffleId)
    if (workers == null || workers.isEmpty) {
      false
    } else {
      workers.values().asScala.exists(_.containsShuffle(shuffleId.toString))
    }
  }

  private def workersNotBlacklisted(shuffleId: Int): List[WorkerInfo] = {
    workerSnapshots(shuffleId)
      .keySet()
      .asScala
      .filter(w => !blacklistManager.contains(w))
      .toList
  }

  // Initialize at the end of LifecycleManager construction.
  initialize()
}
