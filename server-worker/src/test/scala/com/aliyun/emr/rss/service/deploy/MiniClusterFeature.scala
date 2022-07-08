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

package com.aliyun.emr.rss.service.deploy

import java.util.concurrent.atomic.AtomicInteger

import io.netty.channel.ChannelFuture

import com.aliyun.emr.rss.common.RssConf
import com.aliyun.emr.rss.common.internal.Logging
import com.aliyun.emr.rss.common.metrics.MetricsSystem
import com.aliyun.emr.rss.common.rpc.RpcEnv
import com.aliyun.emr.rss.common.protocol.RpcNameConstants
import com.aliyun.emr.rss.server.common.http.{HttpServer, HttpServerInitializer}
import com.aliyun.emr.rss.service.deploy.master.{Master, MasterArguments, MasterSource}
import com.aliyun.emr.rss.service.deploy.worker.{Worker, WorkerArguments, WorkerSource}

trait MiniClusterFeature extends Logging {
  val workerPromethusPort = new AtomicInteger(12378)
  val masterPort = new AtomicInteger(22378)

  protected def runnerWrap[T](code: => T): Thread = new Thread(new Runnable {
    override def run(): Unit = {
      try code
      catch {
        case e: Exception => logWarning(s"ignore thread exception  ${e.getMessage}")
      }
    }
  }) {}

  protected def getTmpDir(): String = {
    val tmpDir = com.google.common.io.Files.createTempDir()
    logInfo(s"temp dir : ${tmpDir}")
    tmpDir.deleteOnExit()
    tmpDir.getAbsolutePath
  }

  protected def createMaster(map: Map[String, String] = null): (Master, RpcEnv, ChannelFuture) = {
    val conf = new RssConf()
    conf.set("rss.metrics.system.enabled", "false")
    conf.set("rss.master.prometheus.metric.port", s"${workerPromethusPort.incrementAndGet()}")
    if (map != null) {
      map.foreach(m => conf.set(m._1, m._2))
    }

    val masterArguments = new MasterArguments(Array(), conf)

    val rpcEnv = RpcEnv.create(
      RpcNameConstants.MASTER_SYS,
      masterArguments.host,
      masterArguments.host,
      masterArguments.port,
      conf,
      4)

    val metricsSystem = MetricsSystem.createMetricsSystem("master",
      conf, MasterSource.ServletPath)
    val master = new Master(rpcEnv, rpcEnv.address, conf, metricsSystem)
    rpcEnv.setupEndpoint(RpcNameConstants.MASTER_EP, master)

    val handlers = if (RssConf.metricsSystemEnable(conf)) {
      logInfo(s"Metrics system enabled.")
      metricsSystem.start()
      new com.aliyun.emr.rss.service.deploy.master.http
      .HttpRequestHandler(master, metricsSystem.getPrometheusHandler)
    } else {
      new com.aliyun.emr.rss.service.deploy.master.http
      .HttpRequestHandler(master, null)
    }

    val httpServer = new HttpServer(new HttpServerInitializer(handlers),
      RssConf.masterPrometheusMetricPort(conf))
    val channelfuture = httpServer.start()

    Thread.sleep(5000L)
    (master, rpcEnv, channelfuture)
  }

  protected def createWorker(map: Map[String, String] = null): (Worker, RpcEnv, ChannelFuture) = {
    val conf = new RssConf()
    conf.set("rss.worker.base.dirs", getTmpDir())
    conf.set("rss.device.monitor.enabled", "false")
    conf.set("rss.push.data.buffer.size", "256K")
    conf.set("rss.worker.prometheus.metric.port", s"${workerPromethusPort.incrementAndGet()}")
    conf.set("rss.fetch.io.threads", "4")
    conf.set("rss.push.io.threads", "4")
    if (map != null) {
      map.foreach(m => conf.set(m._1, m._2))
    }

    val metricsSystem = MetricsSystem.createMetricsSystem("worker", conf, WorkerSource.SERVLET_PATH)

    val workerArguments = new WorkerArguments(Array(), conf)

    val rpcEnv = RpcEnv.create(
      RpcNameConstants.WORKER_SYS,
      workerArguments.host,
      workerArguments.host,
      workerArguments.port,
      conf,
      4)
    val worker = new Worker(rpcEnv, conf, metricsSystem)
    rpcEnv.setupEndpoint(RpcNameConstants.WORKER_EP, worker)

    var channelFuture: ChannelFuture = null
    if (RssConf.metricsSystemEnable(conf)) {
      logInfo(s"Metrics system enabled!")
      metricsSystem.start()
      var port = RssConf.workerPrometheusMetricPort(conf)
      var initialized = false
      while (!initialized) {
        try {
          val httpServer = new HttpServer(
            new HttpServerInitializer(
              new com.aliyun.emr.rss.service.deploy.worker.http
              .HttpRequestHandler(metricsSystem.getPrometheusHandler)), port)
          channelFuture = httpServer.start()
          initialized = true
        } catch {
          case e: Exception =>
            logWarning(s"HttpServer pushPort $port may already exist, try pushPort ${port + 1}.", e)
            port += 1
            Thread.sleep(1000)
        }
      }
    }
    (worker, rpcEnv, channelFuture)
  }

  def setUpMiniCluster(masterConfs:Map[String,String]=null,workerConfs:Map[String,String]=null):
  (Worker, RpcEnv, Worker, RpcEnv, Worker, RpcEnv, Worker, RpcEnv, Worker, RpcEnv) ={
    val (master, masterRpcEnv, masterMetric) = createMaster(masterConfs)
    val masterThread = runnerWrap(masterRpcEnv.awaitTermination())
    masterThread.start()

    Thread.sleep(5000L)

    val (worker1, workerRpcEnv1, workerMetric1) = createWorker(workerConfs)
    val workerThread1 = runnerWrap(workerRpcEnv1.awaitTermination())
    workerThread1.start()

    val (worker2, workerRpcEnv2, workerMetric2) = createWorker(workerConfs)
    val workerThread2 = runnerWrap(workerRpcEnv2.awaitTermination())
    workerThread2.start()

    val (worker3, workerRpcEnv3, workerMetric3) = createWorker(workerConfs)
    val workerThread3 = runnerWrap(workerRpcEnv3.awaitTermination())
    workerThread3.start()

    val (worker4, workerRpcEnv4, workerMetric4) = createWorker(workerConfs)
    val workerThread4 = runnerWrap(workerRpcEnv4.awaitTermination())
    workerThread4.start()

    val (worker5, workerRpcEnv5, workerMetric5) = createWorker(workerConfs)
    val workerThread5 = runnerWrap(workerRpcEnv5.awaitTermination())
    workerThread5.start()

    Thread.sleep(5000L)

    assert(worker1.isRegistered())
    assert(worker2.isRegistered())
    assert(worker3.isRegistered())

    (worker1,workerRpcEnv1,worker2,workerRpcEnv2,worker3,workerRpcEnv3,
      worker4,workerRpcEnv4,worker5,workerRpcEnv5)
  }
}
