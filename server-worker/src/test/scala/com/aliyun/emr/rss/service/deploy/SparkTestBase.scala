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

import scala.util.Random

import io.netty.channel.ChannelFuture
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import com.aliyun.emr.rss.common.internal.Logging
import com.aliyun.emr.rss.common.rpc.RpcEnv
import com.aliyun.emr.rss.common.util.Utils
import com.aliyun.emr.rss.service.deploy.master.Master
import com.aliyun.emr.rss.service.deploy.worker.Worker

trait SparkTestBase extends Logging with MiniClusterFeature {
  val sampleSeq = (1 to 78)
    .map(Random.alphanumeric)
    .toList
    .map(v => (v.toUpper, Random.nextInt(12) + 1))

  var tuple: (
    Master,
      RpcEnv,
      Worker,
      RpcEnv,
      Worker,
      RpcEnv,
      Worker,
      Thread,
      Thread,
      Thread,
      Thread,
      ChannelFuture) = _

  def clearMiniCluster(
    tuple: (
      Master,
        RpcEnv,
        Worker,
        RpcEnv,
        Worker,
        RpcEnv,
        Worker,
        Thread,
        Thread,
        Thread,
        Thread,
        ChannelFuture)): Unit = {
    tuple._3.stop()
    tuple._4.shutdown()
    tuple._5.stop()
    tuple._6.shutdown()
    tuple._7.stop()
    tuple._1.stop()
    tuple._2.shutdown()
    Thread.sleep(5000L)
    tuple._9.interrupt()
    tuple._10.interrupt()
    tuple._11.interrupt()
  }

  def setupRssMiniCluster(): (
    Master,
      RpcEnv,
      Worker,
      RpcEnv,
      Worker,
      RpcEnv,
      Worker,
      Thread,
      Thread,
      Thread,
      Thread,
      ChannelFuture
    ) = {
    Thread.sleep(3000L)

    val (master, masterRpcEnv, masterMetric) = createMaster()
    val (worker1, workerRpcEnv1) = createWorker()
    val (worker2, workerRpcEnv2) = createWorker()
    val (worker3, workerRpcEnv) = createWorker()
    val masterThread = runnerWrap(masterRpcEnv.awaitTermination())
    val workerThread1 = runnerWrap(worker1.init())
    val workerThread2 = runnerWrap(worker2.init())
    val workerThread3 = runnerWrap(worker3.init())

    masterThread.start()
    Thread.sleep(5000L)

    workerThread1.start()
    workerThread2.start()
    workerThread3.start()
    Thread.sleep(5000L)

    assert(worker1.isRegistered())
    assert(worker2.isRegistered())
    assert(worker3.isRegistered())

    (
      master,
      masterRpcEnv,
      worker1,
      workerRpcEnv1,
      worker2,
      workerRpcEnv2,
      worker3,
      masterThread,
      workerThread1,
      workerThread2,
      workerThread3,
      masterMetric
    )
  }

  def updateSparkConf(sparkConf: SparkConf, sort: Boolean): SparkConf = {
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.shuffle.manager", "org.apache.spark.shuffle.rss.RssShuffleManager")
    val localhost = Utils.localHostName()
    sparkConf.set("spark.rss.master.address", s"${localhost}:9097")
    sparkConf.set("spark.shuffle.useOldFetchProtocol", "true")
    sparkConf.set("spark.sql.adaptive.enabled", "false")
    sparkConf.set("spark.shuffle.service.enabled", "false")
    sparkConf.set("spark.sql.adaptive.skewJoin.enabled", "false")
    sparkConf.set("spark.sql.adaptive.localShuffleReader.enabled", "false")
    if (sort) {
      sparkConf.set("spark.rss.shuffle.writer.mode", "sort")
    }
    sparkConf
  }

  def combine(sparkSession: SparkSession): collection.Map[Char, (Int, Int)] = {
    val inputRdd = sparkSession.sparkContext.parallelize(sampleSeq, 4)
    val resultWithOutRss = inputRdd
      .combineByKey(
        (k: Int) => (k, 1),
        (acc: (Int, Int), v: Int) => (acc._1 + v, acc._2 + 1),
        (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
      )
      .collectAsMap()
    resultWithOutRss
  }

  def repartition(sparkSession: SparkSession): collection.Map[Char, Int] = {
    val inputRdd = sparkSession.sparkContext.parallelize(sampleSeq, 2)
    val result = inputRdd.repartition(8).reduceByKey((acc, v) => acc + v).collectAsMap()
    result
  }

  def groupBy(sparkSession: SparkSession): collection.Map[Char, String] = {
    val inputRdd = sparkSession.sparkContext.parallelize(sampleSeq, 2)
    val result = inputRdd.groupByKey().sortByKey().collectAsMap()
    result.map(k => (k._1, k._2.toList.sorted.mkString(","))).toMap
  }

  def runsql(sparkSession: SparkSession): Map[String, Long] = {
    import sparkSession.implicits._
    val df = Seq(("fa", "fb"), ("fa1", "fb1"), ("fa2", "fb2"), ("fa3", "fb3")).toDF("fa", "fb")
    df.createOrReplaceTempView("tmp")
    val result = sparkSession.sql("select fa,count(fb) from tmp group by fa order by fa")
    val outMap = result.collect().map(row => row.getString(0) -> row.getLong(1)).toMap
    outMap
  }
}
