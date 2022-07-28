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

package com.aliyun.emr.rss.service.deploy.master

import java.util
import java.util.concurrent.{ScheduledFuture, TimeUnit}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random

import com.aliyun.emr.rss.common.RssConf
import com.aliyun.emr.rss.common.RssConf.haEnabled
import com.aliyun.emr.rss.common.haclient.RssHARetryClient
import com.aliyun.emr.rss.common.internal.Logging
import com.aliyun.emr.rss.common.meta.{DiskInfo, WorkerInfo}
import com.aliyun.emr.rss.common.metrics.MetricsSystem
import com.aliyun.emr.rss.common.metrics.source.{JVMCPUSource, JVMSource}
import com.aliyun.emr.rss.common.protocol.{PartitionLocation, RpcNameConstants}
import com.aliyun.emr.rss.common.protocol.message.ControlMessages._
import com.aliyun.emr.rss.common.protocol.message.StatusCode
import com.aliyun.emr.rss.common.rpc._
import com.aliyun.emr.rss.common.util.{ThreadUtils, Utils}
import com.aliyun.emr.rss.server.common.http.{HttpServer, HttpServerInitializer}
import com.aliyun.emr.rss.service.deploy.master.clustermeta.SingleMasterMetaManager
import com.aliyun.emr.rss.service.deploy.master.clustermeta.ha.{HAHelper, HAMasterMetaManager, MetaHandler}
import com.aliyun.emr.rss.service.deploy.master.http.HttpRequestHandler

private[deploy] class Master(
    override val rpcEnv: RpcEnv,
    val conf: RssConf,
    val metricsSystem: MetricsSystem)
  extends RpcEndpoint with Logging {

  private val statusSystem = if (haEnabled(conf)) {
    val sys = new HAMasterMetaManager(rpcEnv, conf)
    val handler = new MetaHandler(sys)
    handler.setUpMasterRatisServer(conf)
    sys
  } else {
    new SingleMasterMetaManager(rpcEnv, conf)
  }

  // Threads
  private val forwardMessageThread =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("master-forward-message-thread")
  private var checkForWorkerTimeOutTask: ScheduledFuture[_] = _
  private var checkForApplicationTimeOutTask: ScheduledFuture[_] = _
  private val nonEagerHandler = ThreadUtils.newDaemonCachedThreadPool("master-noneager-handler", 64)

  // Config constants
  private val WorkerTimeoutMs = RssConf.workerTimeoutMs(conf)
  private val ApplicationTimeoutMs = RssConf.applicationTimeoutMs(conf)

  // States
  private def workersSnapShot: util.List[WorkerInfo] =
    statusSystem.workers.synchronized(new util.ArrayList[WorkerInfo](statusSystem.workers))

  private def minimumUsableSize = RssConf.diskMinimumUsableSize(conf)

  private val partitionSizeUpdateInitialDelay = RssConf.partitionSizeUpdaterInitialDelay(conf)
  private val partitionSizeUpdateInterval = RssConf.partitionSizeUpdateInterval(conf)
  private val partitionSizeUpdateService =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("partition-size-updater")
  partitionSizeUpdateService.scheduleAtFixedRate(
    new Runnable {
      override def run(): Unit = {
        statusSystem.handleUpdatePartitionSize()
        logInfo(s"Cluster estimate partition size ${statusSystem.estimatedPartitionSize}")
      }
    },
    partitionSizeUpdateInitialDelay,
    partitionSizeUpdateInterval,
    TimeUnit.MILLISECONDS
  )
  private val offerSlotsAlgorithmVersion = RssConf.offerSlotsAlgorithmVersion(conf)

  // init and register master metrics
  private val masterSource = {
    val source = new MasterSource(conf)
    source.addGauge(MasterSource.RegisteredShuffleCount,
      _ => statusSystem.registeredShuffle.size())
    // blacklist worker count
    source.addGauge(MasterSource.BlacklistedWorkerCount,
      _ => statusSystem.blacklist.size())

    // worker count
    source.addGauge(MasterSource.WorkerCount,
      _ => statusSystem.workers.size())
    val clusterSlotsUsageLimit: Double = RssConf.clusterSlotsUsageLimitPercent(conf)
    // worker slots count
    // worker slots used count
    source.addGauge(MasterSource.WorkerSlotsUsedCount,
      _ => workersSnapShot.asScala.map(_.usedSlots()).sum)

    source.addGauge(MasterSource.PartitionSize, _ => statusSystem.estimatedPartitionSize)

    metricsSystem.registerSource(source)
    metricsSystem.registerSource(new JVMSource(conf, MetricsSystem.ROLE_MASTER))
    metricsSystem.registerSource(new JVMCPUSource(conf, MetricsSystem.ROLE_MASTER))
    source
  }

  // start threads to check timeout for workers and applications
  override def onStart(): Unit = {
    checkForWorkerTimeOutTask = forwardMessageThread.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = Utils.tryLogNonFatalError {
        self.send(CheckForWorkerTimeOut)
      }
    }, 0, WorkerTimeoutMs, TimeUnit.MILLISECONDS)

    checkForApplicationTimeOutTask = forwardMessageThread.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = Utils.tryLogNonFatalError {
        self.send(CheckForApplicationTimeOut)
      }
    }, 0, ApplicationTimeoutMs / 2, TimeUnit.MILLISECONDS)
  }

  override def onStop(): Unit = {
    logInfo("Stopping RSS Master.")
    if (checkForWorkerTimeOutTask != null) {
      checkForWorkerTimeOutTask.cancel(true)
    }
    if (checkForApplicationTimeOutTask != null) {
      checkForApplicationTimeOutTask.cancel(true)
    }
    forwardMessageThread.shutdownNow()
    logInfo("RSS Master is stopped.")
  }

  override def onDisconnected(address: RpcAddress): Unit = {
    // The disconnected client could've been either a worker or an app; remove whichever it was
    logDebug(s"Client $address got disassociated.")
  }

  def executeWithLeaderChecker[T](context: RpcCallContext, f: => T): Unit =
    if (HAHelper.checkShouldProcess(context, statusSystem)) f

  override def receive: PartialFunction[Any, Unit] = {
    case CheckForWorkerTimeOut =>
      executeWithLeaderChecker(null, timeoutDeadWorkers())
    case CheckForApplicationTimeOut =>
      executeWithLeaderChecker(null, timeoutDeadApplications())
    case WorkerLost(host, rpcPort, pushPort, fetchPort, replicatePort, requestId) =>
      logDebug(s"Received worker lost $host:$rpcPort:$pushPort:$fetchPort.")
      executeWithLeaderChecker(null, handleWorkerLost(null, host, rpcPort,
        pushPort, fetchPort, replicatePort, requestId))
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case HeartBeatFromApplication(appId, totalWritten, fileCount, requestId) =>
      logDebug(s"Received heartbeat from app $appId")
      executeWithLeaderChecker(context,
        handleHeartBeatFromApplication(context, appId, totalWritten, fileCount, requestId))

    case RegisterWorker(host, rpcPort, pushPort, fetchPort, replicatePort, disks, requestId) =>
      logDebug(s"Received RegisterWorker request $requestId, $host:$pushPort:$replicatePort" +
        s" $disks.")
      executeWithLeaderChecker(context, handleRegisterWorker(context, host, rpcPort, pushPort,
        fetchPort, replicatePort, disks, requestId))

    case requestSlots @ RequestSlots(_, _, _, _, _, _) =>
      logTrace(s"Received RequestSlots request $requestSlots.")
      executeWithLeaderChecker(context, handleRequestSlots(context, requestSlots))

    case ReleaseSlots(applicationId, shuffleId, workerIds, slots, requestId) =>
      logTrace(s"Received ReleaseSlots request $requestId, $applicationId, $shuffleId," +
          s"workers ${workerIds.asScala.mkString(",")}, slots ${slots.asScala.mkString(",")}")
      executeWithLeaderChecker(context,
        handleReleaseSlots(context, applicationId, shuffleId, workerIds, slots, requestId))

    case UnregisterShuffle(applicationId, shuffleId, requestId) =>
      logDebug(s"Received UnregisterShuffle request $requestId, $applicationId, $shuffleId")
      executeWithLeaderChecker(context,
        handleUnregisterShuffle(context, applicationId, shuffleId, requestId))

    case msg: GetBlacklist =>
      executeWithLeaderChecker(context, handleGetBlacklist(context, msg))

    case ApplicationLost(appId, requestId) =>
      logDebug(s"Received ApplicationLost request $requestId, $appId.")
      executeWithLeaderChecker(context, handleApplicationLost(context, appId, requestId))

    case HeartbeatFromWorker(
    host,
    rpcPort,
    pushPort,
    fetchPort,
    replicatePort,
    disks,
    shuffleKeys,
    requestId) =>
      logDebug(s"Received heartbeat from" +
        s" worker $host:$rpcPort:$pushPort:$fetchPort with $disks.")
      executeWithLeaderChecker(
        context,
        handleHeartBeatFromWorker(
          context,
          host,
          rpcPort,
          pushPort,
          fetchPort,
          replicatePort,
          disks,
          shuffleKeys,
          requestId
        )
      )

    case GetWorkerInfos =>
      executeWithLeaderChecker(context, handleGetWorkerInfos(context))

    case ReportWorkerFailure(failedWorkers: util.List[WorkerInfo], requestId: String) =>
      executeWithLeaderChecker(context,
        handleReportNodeFailure(context, failedWorkers, requestId))

    case GetClusterLoadStatus(numPartitions: Int) =>
      executeWithLeaderChecker(context, handleGetClusterLoadStatus(context, numPartitions))
  }

  private def timeoutDeadWorkers() {
    val currentTime = System.currentTimeMillis()
    var ind = 0
    workersSnapShot.asScala.foreach { worker =>
      if (worker.lastHeartbeat < currentTime - WorkerTimeoutMs
        && !statusSystem.workerLostEvents.contains(worker)) {
        logWarning(s"Worker ${worker.readableAddress()} timeout! Trigger WorkerLost event.")
        // trigger WorkerLost event
        self.send(WorkerLost(worker.host, worker.rpcPort, worker.pushPort, worker.fetchPort,
          worker.replicatePort, RssHARetryClient.genRequestId()))
      }
      ind += 1
    }
  }

  private def timeoutDeadApplications(): Unit = {
    val currentTime = System.currentTimeMillis()
    statusSystem.appHeartbeatTime.keySet().asScala.foreach { key =>
      if (statusSystem.appHeartbeatTime.get(key) < currentTime - ApplicationTimeoutMs) {
        logWarning(s"Application $key timeout, trigger applicationLost event.")
        val requestId = RssHARetryClient.genRequestId()
        var res = self.askSync[ApplicationLostResponse](ApplicationLost(key, requestId))
        var retry = 1
        while (res.status != StatusCode.Success && retry <= 3) {
          res = self.askSync[ApplicationLostResponse](ApplicationLost(key, requestId))
          retry += 1
        }
        if (retry > 3) {
          logWarning(s"Handle ApplicationLost event for $key failed more than 3 times!")
        }
      }
    }
  }

  private def handleHeartBeatFromWorker(
      context: RpcCallContext,
      host: String,
      rpcPort: Int,
      pushPort: Int,
      fetchPort: Int,
      replicatePort: Int,
      disks: util.Map[String, DiskInfo],
      shuffleKeys: util.HashSet[String],
      requestId: String): Unit = {
    val targetWorker = new WorkerInfo(host, rpcPort, pushPort, fetchPort, replicatePort)
    val registered = workersSnapShot
      .asScala
      .find(_ == targetWorker)
      .isDefined
    if (!registered) {
      logWarning(s"Received heartbeat from unknown worker " +
        s"$host:$rpcPort:$pushPort:$fetchPort:$replicatePort.")
    } else {
      statusSystem.handleWorkerHeartBeat(
        host,
        rpcPort,
        pushPort,
        fetchPort,
        replicatePort,
        disks,
        System.currentTimeMillis(),
        requestId
      )
    }

    statusSystem.handleWorkerHeartBeat(
      host,
      rpcPort,
      pushPort,
      fetchPort,
      replicatePort,
      disks,
      System.currentTimeMillis(),
      requestId
    )

    val expiredShuffleKeys = new util.HashSet[String]
    shuffleKeys.asScala.foreach { shuffleKey =>
      if (!statusSystem.registeredShuffle.contains(shuffleKey)) {
        logWarning(s"Shuffle $shuffleKey expired on $host:$rpcPort:$pushPort:$fetchPort.")
        expiredShuffleKeys.add(shuffleKey)
      }
    }
    context.reply(HeartbeatResponse(expiredShuffleKeys, registered))
  }

  private def handleWorkerLost(context: RpcCallContext, host: String, rpcPort: Int, pushPort: Int,
    fetchPort: Int, replicatePort: Int, requestId: String): Unit = {
    val targetWorker = new WorkerInfo(
      host,
      rpcPort,
      pushPort,
      fetchPort,
      replicatePort,
      new util.HashMap[String, DiskInfo](),
      null
    )
    val worker: WorkerInfo = workersSnapShot
      .asScala
      .find(_ == targetWorker)
      .orNull
    if (worker == null) {
      logWarning(s"Unknown worker $host:$rpcPort:$pushPort:$fetchPort" +
        s" for WorkerLost handler!")
      return
    }

    statusSystem.handleWorkerLost(host, rpcPort, pushPort, fetchPort, replicatePort, requestId)

    if (context != null) {
      context.reply(WorkerLostResponse(true))
    }
  }

  def handleRegisterWorker(
      context: RpcCallContext,
      host: String,
      rpcPort: Int,
      pushPort: Int,
      fetchPort: Int,
      replicatePort: Int,
      disks: util.Map[String, DiskInfo],
      requestId: String): Unit = {
    val workerToRegister =
      new WorkerInfo(host, rpcPort, pushPort, fetchPort, replicatePort, disks, null)
    if (workersSnapShot.contains(workerToRegister)) {
      logWarning(s"Receive RegisterWorker while worker" +
        s" ${workerToRegister.toString()} already exists,trigger WorkerLost.")
      if (!statusSystem.workerLostEvents.contains(workerToRegister)) {
        self.send(WorkerLost(host, rpcPort, pushPort, fetchPort, replicatePort,
          RssHARetryClient.genRequestId()))
      }
      context.reply(RegisterWorkerResponse(false, "Worker already registered!"))
    } else if (statusSystem.workerLostEvents.contains(workerToRegister)) {
      logWarning(s"Receive RegisterWorker while worker $workerToRegister " +
        s"in workerLostEvents.")
      context.reply(RegisterWorkerResponse(false, "Worker in workerLostEvents."))
    } else {
      statusSystem.handleRegisterWorker(host, rpcPort, pushPort, fetchPort, replicatePort,
        disks, requestId)
      logInfo(s"Registered worker $workerToRegister.")
      context.reply(RegisterWorkerResponse(true, ""))
    }
  }

  def handleRequestSlots(context: RpcCallContext, requestSlots: RequestSlots): Unit = {
    val numReducers = requestSlots.partitionIdList.size()
    val shuffleKey = Utils.makeShuffleKey(requestSlots.applicationId, requestSlots.shuffleId)

    // offer slots
    val slots = statusSystem.workers.synchronized {
      if (offerSlotsAlgorithmVersion == "V1") {
        MasterUtil.offerSlotsV1(
          workersNotBlacklisted(),
          requestSlots.partitionIdList,
          requestSlots.shouldReplicate
        )
      } else {
        MasterUtil.offerSlotsV2(
          workersNotBlacklisted(),
          requestSlots.partitionIdList,
          requestSlots.shouldReplicate,
          minimumUsableSize
        )
      }
    }

    // reply false if offer slots failed
    if (slots == null || slots.isEmpty) {
      logError(s"Offer slots for $numReducers reducers of $shuffleKey failed!")
      context.reply(RequestSlotsResponse(StatusCode.SlotNotAvailable, new WorkerResource()))
      return
    }

    // register shuffle success, update status
    statusSystem.handleRequestSlots(shuffleKey, requestSlots.hostname,
      Utils.workerSlotsDistribution(slots.asInstanceOf[WorkerResource])
        .asScala.map { case (worker, slots) =>
        worker.toUniqueId() -> slots
      }.asJava, requestSlots.requestId)

    logInfo(s"Offer slots successfully for $numReducers reducers of $shuffleKey" +
      s" on ${slots.size()} workers.")

    val workersNotSelected = workersNotBlacklisted().asScala.filter(!slots.containsKey(_))
    val extraSlotsSize = Math.min(RssConf.offerSlotsExtraSize(conf), workersNotSelected.size)
    if (extraSlotsSize > 0) {
      var index = Random.nextInt(workersNotSelected.size)
      (1 to extraSlotsSize).foreach(_ => {
        slots.put(workersNotSelected(index),
          (new util.ArrayList[PartitionLocation](), new util.ArrayList[PartitionLocation]()))
        index = (index + 1) % workersNotSelected.size
      })
      logInfo(s"Offered extra $extraSlotsSize slots for $shuffleKey")
    }

    context.reply(RequestSlotsResponse(StatusCode.Success, slots.asInstanceOf[WorkerResource]))
  }

  def handleReleaseSlots(
      context: RpcCallContext,
      applicationId: String,
      shuffleId: Int,
      workerIds: util.List[String],
      slots: util.List[util.Map[String, Integer]],
      requestId: String): Unit = {
    val shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId)
    statusSystem.handleReleaseSlots(shuffleKey, workerIds, slots, requestId)
    logInfo(s"[handleReleaseSlots] Release all slots of $shuffleKey")
    context.reply(ReleaseSlotsResponse(StatusCode.Success))
  }

  def handleUnregisterShuffle(
    context: RpcCallContext,
    applicationId: String,
    shuffleId: Int,
    requestId: String): Unit = {
    val shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId)
    statusSystem.handleUnRegisterShuffle(shuffleKey, requestId)
    logInfo(s"Unregister shuffle $shuffleKey")
    context.reply(UnregisterShuffleResponse(StatusCode.Success))
  }

  def handleGetBlacklist(context: RpcCallContext, msg: GetBlacklist): Unit = {
    msg.localBlacklist.removeAll(workersSnapShot)
    context.reply(
      GetBlacklistResponse(StatusCode.Success,
        new util.ArrayList(statusSystem.blacklist), msg.localBlacklist))
  }

  private def handleGetWorkerInfos(context: RpcCallContext): Unit = {
    context.reply(GetWorkerInfosResponse(StatusCode.Success, workersSnapShot.asScala: _*))
  }

  private def handleReportNodeFailure(context: RpcCallContext,
                                      failedWorkers: util.List[WorkerInfo],
                                      requestId: String): Unit = {
    logInfo(s"Receive ReportNodeFailure $failedWorkers, current blacklist" +
        s"${statusSystem.blacklist}")
    statusSystem.handleReportWorkerFailure(failedWorkers, requestId)
    context.reply(OneWayMessageResponse)
  }

  def handleApplicationLost(context: RpcCallContext, appId: String, requestId: String): Unit = {
    nonEagerHandler.submit(new Runnable {
      override def run(): Unit = {
        statusSystem.handleAppLost(appId, requestId)
        logInfo(s"Removed application $appId")
        context.reply(ApplicationLostResponse(StatusCode.Success))
      }
    })
  }

  private def handleHeartBeatFromApplication(
    context: RpcCallContext,
    appId: String,
    totalWritten: Long,
    fileCount: Long,
    requestId: String
  ): Unit = {
    statusSystem.handleAppHeartbeat(
      appId,
      totalWritten,
      fileCount,
      System.currentTimeMillis(),
      requestId
    )
    context.reply(OneWayMessageResponse)
  }

  private def handleGetClusterLoadStatus(context: RpcCallContext, numPartitions: Int): Unit = {
    val clusterSlotsUsageLimit: Double = RssConf.clusterSlotsUsageLimitPercent(conf)
    val (totalSlots, usedSlots, _) = getClusterLoad

    val totalUsedRatio: Double = (usedSlots + numPartitions) / totalSlots.toDouble
    val result = totalUsedRatio >= clusterSlotsUsageLimit
    logInfo(s"Current cluster slots usage:$totalUsedRatio, conf:$clusterSlotsUsageLimit, " +
        s"overload:$result")
    context.reply(GetClusterLoadStatusResponse(result))
  }

  private def getClusterLoad: (Long, Long, Long) = {
    val workers: mutable.Buffer[WorkerInfo] = workersSnapShot.asScala
    if (workers.isEmpty) {
      return (0, 0, 0)
    }

    val clusterSlotsUsageLimit: Double = RssConf.clusterSlotsUsageLimitPercent(conf)

    val (totalSlots, usedSlots, overloadWorkers) = workers.map(workerInfo => {
        val allSlots: Long = workerInfo.disks.values().asScala.map(_.maxSlots).sum
        val usedSlots: Long = workerInfo.usedSlots()
        val flag: Int = if (usedSlots / allSlots.toDouble >= clusterSlotsUsageLimit) 1 else 0
        (allSlots, usedSlots, flag)
      })
      .reduce((pair1, pair2) => {
        (pair1._1 + pair2._1, pair1._2 + pair2._2, pair1._3 + pair2._3)
      })

    (totalSlots, usedSlots, overloadWorkers)
  }

  private def workersNotBlacklisted(
      tmpBlacklist: Set[WorkerInfo] = Set.empty): util.List[WorkerInfo] = {
    workersSnapShot.asScala.filter { w =>
      !statusSystem.blacklist.contains(w) && !tmpBlacklist.contains(w)
    }.asJava
  }

  def getWorkerInfos: String = {
    val sb = new StringBuilder
    workersSnapShot.asScala.foreach { w =>
      sb.append("==========WorkerInfos in Master==========\n")
      sb.append(w).append("\n")

      val workerInfo = requestGetWorkerInfos(w.endpoint)
        .workerInfos.asJava
        .get(0)

      sb.append("==========WorkerInfos in Workers==========\n")
      sb.append(workerInfo).append("\n")

      if (w.hasSameInfoWith(workerInfo)) {
        sb.append("Consist!").append("\n")
      } else {
        sb.append("[ERROR] Inconsistent!").append("\n")
      }
    }

    sb.toString()
  }

  def getThreadDump: String = {
    val sb = new StringBuilder
    val threadDump = Utils.getThreadDump()
    sb.append("==========Master ThreadDump==========\n")
    sb.append(threadDump).append("\n")
    workersSnapShot.asScala.foreach(w => {
      sb.append(s"==========Worker ${w.readableAddress()} ThreadDump==========\n")
      if (w.endpoint == null) {
        w.setupEndpoint(this.rpcEnv.setupEndpointRef(RpcAddress
          .apply(w.host, w.rpcPort), RpcNameConstants.WORKER_EP))
      }
      val res = requestThreadDump(w.endpoint)
      sb.append(res.threadDump).append("\n")
    })

    sb.toString()
  }

  def getHostnameList: String = {
    statusSystem.hostnameSet.asScala.mkString(",")
  }

  private def requestGetWorkerInfos(endpoint: RpcEndpointRef): GetWorkerInfosResponse = {
    try {
      endpoint.askSync[GetWorkerInfosResponse](GetWorkerInfos)
    } catch {
      case e: Exception =>
        logError(s"AskSync GetWorkerInfos failed.", e)
        val result = new util.ArrayList[WorkerInfo]
        result.add(new WorkerInfo("unknown", -1, -1, -1, -1,
          new util.HashMap[String, DiskInfo](), null))
        GetWorkerInfosResponse(StatusCode.Failed, result.asScala: _*)
    }
  }

  private def requestThreadDump(endpoint: RpcEndpointRef): ThreadDumpResponse = {
    try {
      endpoint.askSync[ThreadDumpResponse](ThreadDump)
    } catch {
      case e: Exception =>
        logError(s"AskSync ThreadDump failed.", e)
        ThreadDumpResponse("Unknown")
    }
  }
}

private[deploy] object Master extends Logging {
  def main(args: Array[String]): Unit = {
    val conf = new RssConf()

    val metricsSystem = MetricsSystem.createMetricsSystem("master", conf, MasterSource.ServletPath)

    val masterArgs = new MasterArguments(args, conf)
    val rpcEnv = RpcEnv.create(
      RpcNameConstants.MASTER_SYS,
      masterArgs.host,
      masterArgs.host,
      masterArgs.port,
      conf,
      Math.max(64, Runtime.getRuntime.availableProcessors()))
    val master = new Master(rpcEnv, conf, metricsSystem)
    rpcEnv.setupEndpoint(RpcNameConstants.MASTER_EP, master)

    val handlers = if (RssConf.metricsSystemEnable(conf)) {
      logInfo(s"Metrics system enabled.")
      metricsSystem.start()
      new HttpRequestHandler(master, metricsSystem.getPrometheusHandler)
    } else {
      new HttpRequestHandler(master, null)
    }

    val httpServer = new HttpServer(new HttpServerInitializer(handlers),
      RssConf.masterPrometheusMetricPort(conf))
    httpServer.start()

    rpcEnv.awaitTermination()
  }
}
