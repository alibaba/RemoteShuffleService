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

import java.util.{HashSet => jHashSet}
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._

import io.netty.util.HashedWheelTimer

import com.aliyun.emr.rss.common.RssConf
import com.aliyun.emr.rss.common.RssConf._
import com.aliyun.emr.rss.common.exception.RssException
import com.aliyun.emr.rss.common.haclient.RssHARetryClient
import com.aliyun.emr.rss.common.internal.Logging
import com.aliyun.emr.rss.common.meta.{PartitionLocationInfo, WorkerInfo}
import com.aliyun.emr.rss.common.metrics.MetricsSystem
import com.aliyun.emr.rss.common.metrics.source.{JVMCPUSource, JVMSource, NetWorkSource}
import com.aliyun.emr.rss.common.network.TransportContext
import com.aliyun.emr.rss.common.network.server.{ChannelsLimiter, MemoryTracker}
import com.aliyun.emr.rss.common.protocol.{RpcNameConstants, TransportModuleConstants}
import com.aliyun.emr.rss.common.protocol.message.ControlMessages._
import com.aliyun.emr.rss.common.rpc._
import com.aliyun.emr.rss.common.util.{ThreadUtils, Utils}
import com.aliyun.emr.rss.server.common.http.{HttpServer, HttpServerInitializer}
import com.aliyun.emr.rss.service.deploy.worker.http.HttpRequestHandler

private[deploy] class Worker(
    val conf: RssConf,
    val workerArgs: WorkerArguments) extends Logging {

  val rpcEnv = RpcEnv.create(
    RpcNameConstants.WORKER_SYS,
    workerArgs.host,
    workerArgs.host,
    workerArgs.port,
    conf,
    Math.max(64, Runtime.getRuntime.availableProcessors()))

  private val host = rpcEnv.address.host
  private val rpcPort = rpcEnv.address.port
  Utils.checkHost(host)

  val metricsSystem = MetricsSystem.createMetricsSystem("worker", conf, WorkerSource.ServletPath)
  val workerSource = {
    val source = new WorkerSource(conf)
    metricsSystem.registerSource(source)
    metricsSystem.registerSource(new NetWorkSource(conf, MetricsSystem.ROLE_WOKRER))
    metricsSystem.registerSource(new JVMSource(conf, MetricsSystem.ROLE_WOKRER))
    metricsSystem.registerSource(new JVMCPUSource(conf, MetricsSystem.ROLE_WOKRER))
    source
  }

  val localStorageManager = new LocalStorageManager(conf, workerSource, this)

  val memoryTracker = MemoryTracker.initialize(
    workerPausePushDataRatio(conf),
    workerPauseRepcaliteRatio(conf),
    workerResumeRatio(conf),
    partitionSortMaxMemoryRatio(conf),
    workerDirectMemoryPressureCheckIntervalMs(conf),
    workerDirectMemoryReportIntervalSecond(conf),
    memoryTrimActionThreshold(conf))
  memoryTracker.registerMemoryListener(localStorageManager)

  val partitionsSorter = new PartitionFilesSorter(memoryTracker,
    partitionSortTimeout(conf),
    RssConf.workerFetchChunkSize(conf),
    RssConf.memoryReservedForSingleSort(conf),
    workerSource)

  var controller = new Controller(rpcEnv, conf, metricsSystem)
  rpcEnv.setupEndpoint(RpcNameConstants.WORKER_EP, controller)

  val pushDataHandler = new PushDataHandler()
  val (pushServer, pushClientFactory) = {
    val closeIdleConnections = RssConf.closeIdleConnections(conf)
    val numThreads = conf.getInt("rss.push.io.threads", localStorageManager.numDisks * 2)
    val transportConf = Utils.fromRssConf(conf, TransportModuleConstants.PUSH_MODULE, numThreads)
    val pushServerLimiter = new ChannelsLimiter(TransportModuleConstants.PUSH_MODULE)
    val transportContext: TransportContext =
      new TransportContext(transportConf, pushDataHandler, closeIdleConnections, pushServerLimiter)
    (transportContext.createServer(RssConf.pushServerPort(conf)),
      transportContext.createClientFactory())
  }

  val replicateHandler = new PushDataHandler()
  private val replicateServer = {
    val closeIdleConnections = RssConf.closeIdleConnections(conf)
    val numThreads = conf.getInt("rss.replicate.io.threads", localStorageManager.numDisks * 2)
    val transportConf = Utils.fromRssConf(conf, TransportModuleConstants.REPLICATE_MODULE,
      numThreads)
    val replicateLimiter = new ChannelsLimiter(TransportModuleConstants.REPLICATE_MODULE)
    val transportContext: TransportContext =
      new TransportContext(transportConf, replicateHandler, closeIdleConnections, replicateLimiter)
    transportContext.createServer(RssConf.replicateServerPort(conf))
  }

  var fetchHandler: FetchHandler = _
  private val fetchServer = {
    val closeIdleConnections = RssConf.closeIdleConnections(conf)
    val numThreads = conf.getInt("rss.fetch.io.threads", localStorageManager.numDisks * 2)
    val transportConf = Utils.fromRssConf(conf, TransportModuleConstants.FETCH_MODULE, numThreads)
    fetchHandler = new FetchHandler(transportConf)
    val transportContext: TransportContext =
      new TransportContext(transportConf, fetchHandler, closeIdleConnections)
    transportContext.createServer(RssConf.fetchServerPort(conf))
  }

  private val pushPort = pushServer.getPort
  private val fetchPort = fetchServer.getPort
  private val replicatePort = replicateServer.getPort

  assert(pushPort > 0)
  assert(fetchPort > 0)
  assert(replicatePort > 0)

  // worker info
  val workerInfo = new WorkerInfo(host, rpcPort, pushPort, fetchPort, replicatePort,
    localStorageManager.diskSnapshot, controller.self)

  // whether this Worker registered to Master succesfully
  val registered = new AtomicBoolean(false)

  val shuffleMapperAttempts = new ConcurrentHashMap[String, Array[Int]]()
  val partitionLocationInfo = new PartitionLocationInfo

  private val rssHARetryClient = new RssHARetryClient(rpcEnv, conf)

  // (workerInfo -> last connect timeout timestamp)
  val unavailablePeers = new ConcurrentHashMap[WorkerInfo, Long]()

  // Threads
  private val forwardMessageScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("worker-forward-message-scheduler")
  private var logAvailableFlushBuffersTask: ScheduledFuture[_] = _
  private var sendHeartbeatTask: ScheduledFuture[_] = _
  private var checkFastfailTask: ScheduledFuture[_] = _
  val replicateThreadPool = ThreadUtils.newDaemonCachedThreadPool(
    "worker-replicate-data", RssConf.workerReplicateNumThreads(conf))
  val commitThreadPool = ThreadUtils.newDaemonCachedThreadPool(
    "Worker-CommitFiles", RssConf.workerAsyncCommitFileThreads(conf))
  val asyncReplyPool = ThreadUtils.newDaemonSingleThreadScheduledExecutor("async-reply")
  val timer = new HashedWheelTimer()

  // Configs
  private val HEARTBEAT_MILLIS = RssConf.workerTimeoutMs(conf) / 4
  private val REPLICATE_FAST_FAIL_DURATION = RssConf.replicateFastFailDurationMs(conf)

  private val cleanTaskQueue = new LinkedBlockingQueue[jHashSet[String]]
  var cleaner: Thread = _

  workerSource.addGauge(
    WorkerSource.RegisteredShuffleCount, _ => partitionLocationInfo.shuffleKeySet.size())
  workerSource.addGauge(WorkerSource.SlotsUsed, _ => workerInfo.usedSlots())
  workerSource.addGauge(WorkerSource.SortMemory, _ => memoryTracker.getSortMemoryCounter.get())
  workerSource.addGauge(WorkerSource.SortingFiles, _ => partitionsSorter.getSortingCount)
  workerSource.addGauge(WorkerSource.DiskBuffer, _ => memoryTracker.getDiskBufferCounter.get())
  workerSource.addGauge(WorkerSource.NettyMemory, _ => memoryTracker.getNettyMemoryCounter.get())
  workerSource.addGauge(WorkerSource.PausePushDataCount, _ => memoryTracker.getPausePushDataCounter)
  workerSource.addGauge(WorkerSource.PausePushDataAndReplicateCount,
    _ => memoryTracker.getPausePushDataAndReplicateCounter)

  def heartBeatToMaster(): Unit = {
    val shuffleKeys = new jHashSet[String]
    shuffleKeys.addAll(partitionLocationInfo.shuffleKeySet)
    shuffleKeys.addAll(localStorageManager.shuffleKeySet())
    val response = rssHARetryClient.askSync[HeartbeatResponse](
      HeartbeatFromWorker(host, rpcPort, pushPort, fetchPort, replicatePort,
        localStorageManager.diskSnapshot, shuffleKeys), classOf[HeartbeatResponse])
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
          controller.stop()
      }
    }
  }

  def init(): Unit = {
    logInfo(s"Starting Worker $host:$pushPort:$fetchPort:$replicatePort" +
      s" with ${workerInfo.disks} slots.")
    registerWithMaster()

    // start heartbeat
    sendHeartbeatTask = forwardMessageScheduler.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = Utils.tryLogNonFatalError {
        heartBeatToMaster()
      }
    }, HEARTBEAT_MILLIS, HEARTBEAT_MILLIS, TimeUnit.MILLISECONDS)

    checkFastfailTask = forwardMessageScheduler.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = Utils.tryLogNonFatalError {
        unavailablePeers.entrySet().asScala.foreach(entry => {
          if (System.currentTimeMillis() - entry.getValue >
            REPLICATE_FAST_FAIL_DURATION) {
            unavailablePeers.remove(entry.getKey)
          }
        })
      }
    }, 0, REPLICATE_FAST_FAIL_DURATION, TimeUnit.MILLISECONDS)

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
          httpServer.start()
          initialized = true
        } catch {
          case e: Exception =>
            logWarning(s"HttpServer pushPort $port may already exist, try pushPort ${port + 1}.", e)
            port += 1
            Thread.sleep(1000)
        }
      }
    }

    cleaner = new Thread("Cleaner") {
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

    pushDataHandler.init(this)
    replicateHandler.init(this)
    fetchHandler.init(this)
    controller.init(this)

    cleaner.setDaemon(true)
    cleaner.start()

    rpcEnv.awaitTermination()
    stop()
  }

  def stop(): Unit = {
    logInfo("Stopping RSS Worker.")

    if (sendHeartbeatTask != null) {
      sendHeartbeatTask.cancel(true)
      sendHeartbeatTask = null
    }
    if (logAvailableFlushBuffersTask != null) {
      logAvailableFlushBuffersTask.cancel(true)
      logAvailableFlushBuffersTask = null
    }
    if (checkFastfailTask != null) {
      checkFastfailTask.cancel(true)
      checkFastfailTask = null
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

  private def registerWithMaster() {
    var registerTimeout = RssConf.registerWorkerTimeoutMs(conf)
    val delta = 2000
    while (registerTimeout > 0) {
      val rsp = try {
        rssHARetryClient.askSync[RegisterWorkerResponse](
          RegisterWorker(host, rpcPort, pushPort, fetchPort, replicatePort, workerInfo.disks),
          classOf[RegisterWorkerResponse]
        )
      } catch {
        case throwable: Throwable =>
          logWarning(s"Register worker to master failed, will retry after 2s, exception: ",
            throwable)
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

  def isRegistered(): Boolean = {
    registered.get()
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

    val worker = new Worker(conf, workerArgs)
    worker.init()
  }
}
