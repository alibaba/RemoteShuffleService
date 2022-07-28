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

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

import com.aliyun.emr.rss.common.RssConf
import com.aliyun.emr.rss.common.internal.Logging
import com.aliyun.emr.rss.common.metrics.MetricsSystem
import com.aliyun.emr.rss.common.rpc.RpcEnv
import com.aliyun.emr.rss.common.protocol.RpcNameConstants
import com.aliyun.emr.rss.server.common.http.{HttpServer, HttpServerInitializer}
import com.aliyun.emr.rss.service.deploy.master.http.HttpRequestHandler

class MasterSuite extends AnyFunSuite
  with BeforeAndAfterAll
  with BeforeAndAfterEach
  with Logging {

  def getTmpDir(): String = {
    val tmpDir = com.google.common.io.Files.createTempDir()
    tmpDir.deleteOnExit()
    tmpDir.getAbsolutePath
  }

  test("test single node startup functionality") {
    val conf = new RssConf()
    conf.set("rss.ha.enabled", "false")
    conf.set("rss.ha.storage.dir", getTmpDir())
    conf.set("rss.worker.base.dirs", getTmpDir())
    conf.set("rss.metrics.system.enabled", "true")
    conf.set("rss.master.prometheus.metric.port", "11112")

    val args = Array("-h", "localhost", "-p", "9097")

    val masterArgs = new MasterArguments(args, conf)

    val rpcEnv = RpcEnv.create(
      RpcNameConstants.MASTER_SYS,
      masterArgs.host,
      masterArgs.host,
      masterArgs.port,
      conf,
      4)

    val metricsSystem = MetricsSystem.createMetricsSystem("master", conf, MasterSource.ServletPath)
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

    Thread.sleep(5000L)

    master.stop()
    rpcEnv.shutdown()
  }
}
