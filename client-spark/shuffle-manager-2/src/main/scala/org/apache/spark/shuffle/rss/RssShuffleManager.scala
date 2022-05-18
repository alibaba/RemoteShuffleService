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

package org.apache.spark.shuffle.rss

import java.lang.reflect.Method
import java.util.concurrent.atomic.LongAdder

import io.netty.util.internal.ConcurrentSet
import org.apache.spark._
import org.apache.spark.shuffle._
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.util.Utils

import com.aliyun.emr.rss.client.{LifecycleManager, ShuffleClient}
import com.aliyun.emr.rss.common.RssConf
import com.aliyun.emr.rss.common.internal.Logging

class RssShuffleManager(conf: SparkConf) extends ShuffleManager with Logging {

  private lazy val isDriver: Boolean = SparkEnv.get.executorId == SparkContext.DRIVER_IDENTIFIER

  // Read RssConf from SparkConf
  private lazy val rssConf = RssShuffleManager.fromSparkConf(conf)

  private var newAppId: Option[String] = None
  private var lifecycleManager: Option[LifecycleManager] = None
  private var rssShuffleClient: Option[ShuffleClient] = None

  val sortShuffleManagerName = classOf[org.apache.spark.shuffle.sort.SortShuffleManager].getName
  private lazy val sortShuffleManager = {
    RssShuffleManager.instantiateClass[SortShuffleManager](sortShuffleManagerName, conf, isDriver)
  }
  private val sortShuffleIds = new ConcurrentSet[Int]()

  private lazy val fallbackPolicyRunner = new RssShuffleFallbackPolicyRunner(conf)

  private def initializeLifecycleManager(appId: String): Unit = {
    // Only create LifecycleManager singleton in Driver. When register shuffle multiple times, we
    // need to ensure that LifecycleManager will only be created once. Parallelism needs to be
    // considered in this place, because if there is one RDD that depends on multiple RDDs
    // at the same time, it may bring parallel `register shuffle`, such as Join in Sql.
    if (isDriver && lifecycleManager.isEmpty) {
      lifecycleManager.synchronized {
        if (lifecycleManager.isEmpty) {
          val metaSystem = new LifecycleManager(appId, rssConf)
          lifecycleManager = Some(metaSystem)
          rssShuffleClient = Some(ShuffleClient.get(metaSystem.self, rssConf))
        }
      }
    }
  }

  override def registerShuffle[K, V, C](
      shuffleId: Int,
      numMaps: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    // Note: generate newAppId at driver side, make sure dependency.rdd.context
    // is the same SparkContext among different shuffleIds.
    // This method may be called many times.
    newAppId = Some(RssShuffleManager.genNewAppId(dependency.rdd.context))
    newAppId.foreach(initializeLifecycleManager)

    if (fallbackPolicyRunner.applyAllFallbackPolicy(lifecycleManager.get,
      dependency.partitioner.numPartitions)) {
      logWarning("Fallback to SortShuffleManager!")
      sortShuffleIds.add(shuffleId)
      sortShuffleManager.registerShuffle(shuffleId, numMaps, dependency)
    } else {
      new RssShuffleHandle[K, V, C](
        newAppId.get,
        // If not driver, return dummy rss meta service host and port.
        lifecycleManager.map(_.getRssMetaServiceHost).getOrElse(""),
        lifecycleManager.map(_.getRssMetaServicePort).getOrElse(0),
        shuffleId,
        numMaps,
        dependency)
    }
  }

  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Int,
      context: TaskContext): ShuffleWriter[K, V] = {
    handle match {
      case h: RssShuffleHandle[K@unchecked, V@unchecked, _] =>
        val client = ShuffleClient.get(h.rssMetaServiceHost, h.rssMetaServicePort, rssConf)
        if (RssConf.shuffleWriterMode(rssConf) == "sort") {
          new SortBasedShuffleWriter(h.dependency, h.newAppId, h.numMaps, context,
            rssConf, client)
        } else if (RssConf.shuffleWriterMode(rssConf) == "hash") {
          new HashBasedShuffleWriter(h, mapId, context, rssConf, client)
        } else {
          throw new UnsupportedOperationException(
            s"Unrecognized shuffle write mode! ${RssConf.shuffleWriterMode(rssConf)}")
        }
      case _ => sortShuffleManager.getWriter(handle, mapId, context)
    }
  }

  override def getReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext): ShuffleReader[K, C] = {
    handle match {
      case _: RssShuffleHandle[K@unchecked, C@unchecked, _] =>
        new RssShuffleReader(
          handle.asInstanceOf[RssShuffleHandle[K, _, C]],
          startPartition,
          endPartition,
          context = context,
          conf = rssConf)
      case _ => sortShuffleManager.getReader(handle, startPartition, endPartition, context)
    }
  }

  // remove override for compatibility
  def getReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      startMapIndex: Int,
      endMapIndex: Int): ShuffleReader[K, C] = {
    handle match {
      case _: RssShuffleHandle[K@unchecked, C@unchecked, _] =>
        new RssShuffleReader(
          handle.asInstanceOf[RssShuffleHandle[K, _, C]],
          startPartition,
          endPartition,
          startMapIndex,
          endMapIndex,
          context,
          rssConf)
      case _ =>
        RssShuffleManager.instantiateMethod(sortShuffleManagerName, "getReader")
          .invoke(handle,
            startPartition.asInstanceOf[AnyRef],
            endPartition.asInstanceOf[AnyRef],
            context,
            startMapIndex.asInstanceOf[AnyRef],
            endMapIndex.asInstanceOf[AnyRef])
          .asInstanceOf[ShuffleReader[K, C]]
    }
  }

  override def unregisterShuffle(shuffleId: Int): Boolean = {
    if (sortShuffleIds.contains(shuffleId)) {
      sortShuffleManager.unregisterShuffle(shuffleId)
    } else {
      newAppId match {
        case Some(id) => rssShuffleClient.exists(_.unregisterShuffle(id, shuffleId, isDriver))
        case None => true
      }
    }
  }

  override def shuffleBlockResolver: ShuffleBlockResolver = {
    sortShuffleManager.shuffleBlockResolver
  }

  override def stop(): Unit = {
    rssShuffleClient.foreach(_.shutDown())
    lifecycleManager.foreach(_.stop())
    if (sortShuffleManager != null) {
      sortShuffleManager.stop()
    }
  }
}

object RssShuffleManager {

  /**
   * make rss conf from spark conf
   *
   * @param conf
   * @return
   */
  def fromSparkConf(conf: SparkConf): RssConf = {
    val tmpRssConf = new RssConf()
    for ((key, value) <- conf.getAll if key.startsWith("spark.rss.")) {
      tmpRssConf.set(key.substring("spark.".length), value)
    }
    tmpRssConf
  }

  def genNewAppId(context: SparkContext): String = {
    context.applicationAttemptId match {
      case Some(id) => s"${context.applicationId}_$id"
      case None => s"${context.applicationId}"
    }
  }

  // Create an instance of the class with the given name, possibly initializing it with our conf
  // Copied from SparkEnv
  def instantiateClass[T](className: String, conf: SparkConf, isDriver: Boolean): T = {
    val cls = Utils.classForName(className)
    // Look for a constructor taking a SparkConf and a boolean isDriver, then one taking just
    // SparkConf, then one taking no arguments
    try {
      cls.getConstructor(classOf[SparkConf], java.lang.Boolean.TYPE)
        .newInstance(conf, java.lang.Boolean.valueOf(isDriver))
        .asInstanceOf[T]
    } catch {
      case _: NoSuchMethodException =>
        try {
          cls.getConstructor(classOf[SparkConf]).newInstance(conf).asInstanceOf[T]
        } catch {
          case _: NoSuchMethodException =>
            cls.getConstructor().newInstance().asInstanceOf[T]
        }
    }
  }

  // Create an instance of the getReader method of SortShuffleManager
  def instantiateMethod(className: String, methodName: String): Method = {
    val cls = Utils.classForName(className)
    try {
      cls.getMethod(methodName, classOf[ShuffleHandle], Integer.TYPE, Integer.TYPE,
        classOf[TaskContext], Integer.TYPE, Integer.TYPE)
    } catch {
      case e: NoSuchMethodException =>
        throw new Exception("Get getReader method failed.", e)
    }
  }
}

class RssShuffleHandle[K, V, C](
    val newAppId: String,
    val rssMetaServiceHost: String,
    val rssMetaServicePort: Int,
    shuffleId: Int,
    numMaps: Int,
    dependency: ShuffleDependency[K, V, C])
  extends BaseShuffleHandle(shuffleId, numMaps, dependency) {
}
