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

package com.aliyun.emr.rss.service.deploy.cluster

import com.aliyun.emr.rss.client.{LifecycleManager, ShuffleClientImpl}

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import io.netty.channel.ChannelFuture
import org.apache.commons.lang3.RandomStringUtils
import org.junit.{Assert, Test}
import com.aliyun.emr.rss.common.RssConf
import com.aliyun.emr.rss.common.rpc.RpcEnv
import com.aliyun.emr.rss.service.deploy.MiniClusterFeature

class ClusterReadWriteTest extends MiniClusterFeature {
  @Test
  def testMiniCluster(): Unit = {
    setUpMiniCluster()

    val APP = "app-1"

    val clientConf = new RssConf()
    clientConf.set("rss.push.data.replicate", "true")
    clientConf.set("rss.push.data.buffer.size", "256K")
    val metaSystem = new LifecycleManager(APP, clientConf)
    val shuffleClient = new ShuffleClientImpl(clientConf)
    shuffleClient.setupMetaServiceRef(metaSystem.self)

    val STR1 = RandomStringUtils.random(1024)
    val DATA1 = STR1.getBytes(StandardCharsets.UTF_8)
    val OFFSET1 = 0
    val LENGTH1 = DATA1.length

    val dataSize1 = shuffleClient.pushData(APP, 1, 0, 0, 0, DATA1, OFFSET1, LENGTH1, 1, 1)
    logInfo(s"push data data size ${dataSize1}")

    val STR2 = RandomStringUtils.random(32 * 1024)
    val DATA2 = STR2.getBytes(StandardCharsets.UTF_8)
    val OFFSET2 = 0
    val LENGTH2 = DATA2.length
    val dataSize2 = shuffleClient.pushData(APP, 1, 0, 0, 0, DATA2, OFFSET2, LENGTH2, 1, 1)
    logInfo(s"push data data size ${dataSize2}")

    val STR3 = RandomStringUtils.random(32 * 1024)
    val DATA3 = STR3.getBytes(StandardCharsets.UTF_8)
    val LENGTH3 = DATA3.length
    val dataSize3 = shuffleClient.mergeData(APP, 1, 0, 0, 0, DATA3, 0, LENGTH3, 1, 1);

    val STR4 = RandomStringUtils.random(16 * 1024)
    val DATA4 = STR4.getBytes(StandardCharsets.UTF_8)
    val LENGTH4 = DATA4.length
    val dataSize4 = shuffleClient.mergeData(APP, 1, 0, 0, 0, DATA4, 0, LENGTH4, 1, 1);
    shuffleClient.pushMergedData(APP, 1, 0, 0)
    Thread.sleep(1000)

    shuffleClient.mapperEnd(APP, 1, 0, 0, 1)

    val inputStream = shuffleClient.readPartition(APP, 1, 0, 0)
    val outputStream = new ByteArrayOutputStream()

    var b = inputStream.read()
    while (b != -1) {
      outputStream.write(b)
      b = inputStream.read()
    }

    val readBytes = outputStream.toByteArray

    assert(readBytes.length == LENGTH1 + LENGTH2 + LENGTH3 + LENGTH4)
    val targetArr = Array.concat(DATA1, DATA2, DATA3, DATA4)
    Assert.assertArrayEquals(targetArr, readBytes)

    Thread.sleep(5000L)
    shuffleClient.shutDown()
    metaSystem.rpcEnv.shutdown()

  }
}
