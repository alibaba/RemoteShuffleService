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

package com.aliyun.emr.rss.common

import java.util.{Map => JMap}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import com.aliyun.emr.rss.common.internal.Logging
import com.aliyun.emr.rss.common.protocol.PartitionSplitMode
import com.aliyun.emr.rss.common.util.Utils

class RssConf(loadDefaults: Boolean) extends Cloneable with Logging with Serializable {

  import RssConf._

  /** Create a RssConf that loads defaults from system properties and the classpath */
  def this() = this(true)

  private val settings = new ConcurrentHashMap[String, String]()

  if (loadDefaults) {
    loadFromSystemProperties(false)
  }

  private[rss] def loadFromSystemProperties(silent: Boolean): RssConf = {
    // Load any spark.* system properties
    for ((key, value) <- Utils.getSystemProperties if key.startsWith("rss.")) {
      set(key, value, silent)
    }
    this
  }

  /** Set a configuration variable. */
  def set(key: String, value: String): RssConf = {
    set(key, value, false)
  }

  private[rss] def set(key: String, value: String, silent: Boolean): RssConf = {
    if (key == null) {
      throw new NullPointerException("null key")
    }
    if (value == null) {
      throw new NullPointerException("null value for " + key)
    }
    if (!silent) {
      logDeprecationWarning(key)
    }
    settings.put(key, value)
    this
  }

  /** Set multiple parameters together */
  def setAll(settings: Traversable[(String, String)]): RssConf = {
    settings.foreach { case (k, v) => set(k, v) }
    this
  }

  /** Set a parameter if it isn't already configured */
  def setIfMissing(key: String, value: String): RssConf = {
    if (settings.putIfAbsent(key, value) == null) {
      logDeprecationWarning(key)
    }
    this
  }

  /** Remove a parameter from the configuration */
  def remove(key: String): RssConf = {
    settings.remove(key)
    this
  }

  /** Get a parameter; throws a NoSuchElementException if it's not set */
  def get(key: String): String = {
    getOption(key).getOrElse(throw new NoSuchElementException(key))
  }

  /** Get a parameter, falling back to a default if not set */
  def get(key: String, defaultValue: String): String = {
    getOption(key).getOrElse(defaultValue)
  }

  /**
   * Get a time parameter as seconds; throws a NoSuchElementException if it's not set. If no
   * suffix is provided then seconds are assumed.
   * @throws java.util.NoSuchElementException If the time parameter is not set
   * @throws NumberFormatException If the value cannot be interpreted as seconds
   */
  def getTimeAsSeconds(key: String): Long = catchIllegalValue(key) {
    Utils.timeStringAsSeconds(get(key))
  }

  /**
   * Get a time parameter as seconds, falling back to a default if not set. If no
   * suffix is provided then seconds are assumed.
   * @throws NumberFormatException If the value cannot be interpreted as seconds
   */
  def getTimeAsSeconds(key: String, defaultValue: String): Long = catchIllegalValue(key) {
    Utils.timeStringAsSeconds(get(key, defaultValue))
  }

  /**
   * Get a time parameter as milliseconds; throws a NoSuchElementException if it's not set. If no
   * suffix is provided then milliseconds are assumed.
   * @throws java.util.NoSuchElementException If the time parameter is not set
   * @throws NumberFormatException If the value cannot be interpreted as milliseconds
   */
  def getTimeAsMs(key: String): Long = catchIllegalValue(key) {
    Utils.timeStringAsMs(get(key))
  }

  /**
   * Get a time parameter as milliseconds, falling back to a default if not set. If no
   * suffix is provided then milliseconds are assumed.
   * @throws NumberFormatException If the value cannot be interpreted as milliseconds
   */
  def getTimeAsMs(key: String, defaultValue: String): Long = catchIllegalValue(key) {
    Utils.timeStringAsMs(get(key, defaultValue))
  }

  /**
   * Get a size parameter as bytes; throws a NoSuchElementException if it's not set. If no
   * suffix is provided then bytes are assumed.
   * @throws java.util.NoSuchElementException If the size parameter is not set
   * @throws NumberFormatException If the value cannot be interpreted as bytes
   */
  def getSizeAsBytes(key: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsBytes(get(key))
  }

  /**
   * Get a size parameter as bytes, falling back to a default if not set. If no
   * suffix is provided then bytes are assumed.
   * @throws NumberFormatException If the value cannot be interpreted as bytes
   */
  def getSizeAsBytes(key: String, defaultValue: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsBytes(get(key, defaultValue))
  }

  /**
   * Get a size parameter as bytes, falling back to a default if not set.
   * @throws NumberFormatException If the value cannot be interpreted as bytes
   */
  def getSizeAsBytes(key: String, defaultValue: Long): Long = catchIllegalValue(key) {
    Utils.byteStringAsBytes(get(key, defaultValue + "B"))
  }

  /**
   * Get a size parameter as Kibibytes; throws a NoSuchElementException if it's not set. If no
   * suffix is provided then Kibibytes are assumed.
   * @throws java.util.NoSuchElementException If the size parameter is not set
   * @throws NumberFormatException If the value cannot be interpreted as Kibibytes
   */
  def getSizeAsKb(key: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsKb(get(key))
  }

  /**
   * Get a size parameter as Kibibytes, falling back to a default if not set. If no
   * suffix is provided then Kibibytes are assumed.
   * @throws NumberFormatException If the value cannot be interpreted as Kibibytes
   */
  def getSizeAsKb(key: String, defaultValue: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsKb(get(key, defaultValue))
  }

  /**
   * Get a size parameter as Mebibytes; throws a NoSuchElementException if it's not set. If no
   * suffix is provided then Mebibytes are assumed.
   * @throws java.util.NoSuchElementException If the size parameter is not set
   * @throws NumberFormatException If the value cannot be interpreted as Mebibytes
   */
  def getSizeAsMb(key: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsMb(get(key))
  }

  /**
   * Get a size parameter as Mebibytes, falling back to a default if not set. If no
   * suffix is provided then Mebibytes are assumed.
   * @throws NumberFormatException If the value cannot be interpreted as Mebibytes
   */
  def getSizeAsMb(key: String, defaultValue: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsMb(get(key, defaultValue))
  }

  /**
   * Get a size parameter as Gibibytes; throws a NoSuchElementException if it's not set. If no
   * suffix is provided then Gibibytes are assumed.
   * @throws java.util.NoSuchElementException If the size parameter is not set
   * @throws NumberFormatException If the value cannot be interpreted as Gibibytes
   */
  def getSizeAsGb(key: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsGb(get(key))
  }

  /**
   * Get a size parameter as Gibibytes, falling back to a default if not set. If no
   * suffix is provided then Gibibytes are assumed.
   * @throws NumberFormatException If the value cannot be interpreted as Gibibytes
   */
  def getSizeAsGb(key: String, defaultValue: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsGb(get(key, defaultValue))
  }

  /** Get a parameter as an Option */
  def getOption(key: String): Option[String] = {
    Option(settings.get(key)).orElse(getDeprecatedConfig(key, settings))
  }

//  /** Get an optional value, applying variable substitution. */
//  private[rss] def getWithSubstitution(key: String): Option[String] = {
//    getOption(key).map(reader.substitute(_))
//  }

  /** Get all parameters as a list of pairs */
  def getAll: Array[(String, String)] = {
    settings.entrySet().asScala.map(x => (x.getKey, x.getValue)).toArray
  }

  /**
   * Get all parameters that start with `prefix`
   */
  def getAllWithPrefix(prefix: String): Array[(String, String)] = {
    getAll.filter { case (k, v) => k.startsWith(prefix) }
      .map { case (k, v) => (k.substring(prefix.length), v) }
  }

  /**
   * Get a parameter as an integer, falling back to a default if not set
   * @throws NumberFormatException If the value cannot be interpreted as an integer
   */
  def getInt(key: String, defaultValue: Int): Int = catchIllegalValue(key) {
    getOption(key).map(_.toInt).getOrElse(defaultValue)
  }

  /**
   * Get a parameter as a long, falling back to a default if not set
   * @throws NumberFormatException If the value cannot be interpreted as a long
   */
  def getLong(key: String, defaultValue: Long): Long = catchIllegalValue(key) {
    getOption(key).map(_.toLong).getOrElse(defaultValue)
  }

  /**
   * Get a parameter as a double, falling back to a default if not ste
   * @throws NumberFormatException If the value cannot be interpreted as a double
   */
  def getDouble(key: String, defaultValue: Double): Double = catchIllegalValue(key) {
    getOption(key).map(_.toDouble).getOrElse(defaultValue)
  }

  /**
   * Get a parameter as a boolean, falling back to a default if not set
   * @throws IllegalArgumentException If the value cannot be interpreted as a boolean
   */
  def getBoolean(key: String, defaultValue: Boolean): Boolean = catchIllegalValue(key) {
    getOption(key).map(_.toBoolean).getOrElse(defaultValue)
  }

  /** Does the configuration contain a given parameter? */
  def contains(key: String): Boolean = {
    settings.containsKey(key) ||
      configsWithAlternatives.get(key).toSeq.flatten.exists { alt => contains(alt.key) }
  }
//
//  private[rss] def contains(entry: ConfigEntry[_]): Boolean = contains(entry.key)

  /** Copy this object */
  override def clone: RssConf = {
    val cloned = new RssConf(false)
    settings.entrySet().asScala.foreach { e =>
      cloned.set(e.getKey(), e.getValue(), true)
    }
    cloned
  }

  /**
   * By using this instead of System.getenv(), environment variables can be mocked
   * in unit tests.
   */
  private[rss] def getenv(name: String): String = System.getenv(name)

  /**
   * Wrapper method for get() methods which require some specific value format. This catches
   * any [[NumberFormatException]] or [[IllegalArgumentException]] and re-raises it with the
   * incorrectly configured key in the exception message.
   */
  private def catchIllegalValue[T](key: String)(getValue: => T): T = {
    try {
      getValue
    } catch {
      case e: NumberFormatException =>
        // NumberFormatException doesn't have a constructor that takes a cause for some reason.
        throw new NumberFormatException(s"Illegal value for config key $key: ${e.getMessage}")
          .initCause(e)
      case e: IllegalArgumentException =>
        throw new IllegalArgumentException(s"Illegal value for config key $key: ${e.getMessage}", e)
    }
  }
}

object RssConf extends Logging {

  /**
   * Maps deprecated config keys to information about the deprecation.
   *
   * The extra information is logged as a warning when the config is present in the user's
   * configuration.
   */
  private val deprecatedConfigs: Map[String, DeprecatedConfig] = {
    val configs = Seq(
      DeprecatedConfig("none", "1.0",
          "None")
    )

    Map(configs.map { cfg => (cfg.key -> cfg) } : _*)
  }

  /**
   * Maps a current config key to alternate keys that were used in previous version of Spark.
   *
   * The alternates are used in the order defined in this map. If deprecated configs are
   * present in the user's configuration, a warning is logged.
   *
   */
  private val configsWithAlternatives = Map[String, Seq[AlternateConfig]](
    "none" -> Seq(
      AlternateConfig("none", "1.0"))
  )

  /**
   * A view of `configsWithAlternatives` that makes it more efficient to look up deprecated
   * config keys.
   *
   * Maps the deprecated config name to a 2-tuple (new config name, alternate config info).
   */
  private val allAlternatives: Map[String, (String, AlternateConfig)] = {
    configsWithAlternatives.keys.flatMap { key =>
      configsWithAlternatives(key).map { cfg => (cfg.key -> (key -> cfg)) }
    }.toMap
  }

  /**
   * Looks for available deprecated keys for the given config option, and return the first
   * value available.
   */
  def getDeprecatedConfig(key: String, conf: JMap[String, String]): Option[String] = {
    configsWithAlternatives.get(key).flatMap { alts =>
      alts.collectFirst { case alt if conf.containsKey(alt.key) =>
        val value = conf.get(alt.key)
        if (alt.translation != null) alt.translation(value) else value
      }
    }
  }

  /**
   * Logs a warning message if the given config key is deprecated.
   */
  def logDeprecationWarning(key: String): Unit = {
    deprecatedConfigs.get(key).foreach { cfg =>
      logWarning(
        s"The configuration key '$key' has been deprecated as of RSS ${cfg.version} and " +
          s"may be removed in the future. ${cfg.deprecationMessage}")
      return
    }

    allAlternatives.get(key).foreach { case (newKey, cfg) =>
      logWarning(
        s"The configuration key '$key' has been deprecated as of RSS ${cfg.version} and " +
          s"may be removed in the future. Please use the new key '$newKey' instead.")
      return
    }
  }

  /**
   * Holds information about keys that have been deprecated and do not have a replacement.
   *
   * @param key The deprecated key.
   * @param version Version of Spark where key was deprecated.
   * @param deprecationMessage Message to include in the deprecation warning.
   */
  private case class DeprecatedConfig(
      key: String,
      version: String,
      deprecationMessage: String)

  /**
   * Information about an alternate configuration key that has been deprecated.
   *
   * @param key The deprecated config key.
   * @param version The Spark version in which the key was deprecated.
   * @param translation A translation function for converting old config values into new ones.
   */
  private case class AlternateConfig(
      key: String,
      version: String,
      translation: String => String = null)

  // Conf getters

  def pushDataBufferSize(conf: RssConf): Int = {
    conf.getSizeAsBytes("rss.push.data.buffer.size", "64k").toInt
  }

  def pushDataQueueCapacity(conf: RssConf): Int = {
    conf.getInt("rss.push.data.queue.capacity", 512)
  }

  def pushDataMaxReqsInFlight(conf: RssConf): Int = {
    conf.getInt("rss.push.data.maxReqsInFlight", 32)
  }

  def pushDataMaxCompositeSize(conf: RssConf): Int = {
    conf.getSizeAsBytes("rss.push.data.max.composite.size", "64m").toInt
  }

  def fetchChunkTimeoutMs(conf: RssConf): Long = {
    conf.getTimeAsMs("rss.fetch.chunk.timeout", "120s")
  }

  def fetchChunkMaxReqsInFlight(conf: RssConf): Int = {
    conf.getInt("rss.fetch.chunk.maxReqsInFlight", 3)
  }

  def replicate(conf: RssConf): Boolean = {
    conf.getBoolean("rss.push.data.replicate", true)
  }

  def workerTimeoutMs(conf: RssConf): Long = {
    conf.getTimeAsMs("rss.worker.timeout", "120s")
  }

  def applicationTimeoutMs(conf: RssConf): Long = {
    conf.getTimeAsMs("rss.application.timeout", "120s")
  }

  def removeShuffleDelayMs(conf: RssConf): Long = {
    conf.getTimeAsMs("rss.remove.shuffle.delay", "60s")
  }

  def getBlacklistDelayMs(conf: RssConf): Long = {
    conf.getTimeAsMs("rss.get.blacklist.delay", "30s")
  }

  def masterAddress(conf: RssConf): String = {
    conf.get("rss.master.address", masterHost(conf) + ":" + 9097)
  }

  def masterHostsFromAddress(conf: RssConf): String = {
    masterAddress(conf).split(",").map(_.split(":")(0)).mkString(",")
  }

  def masterHost(conf: RssConf): String = {
    conf.get("rss.master.host", Utils.localHostName())
  }

  def masterPort(conf: RssConf): Int = {
    conf.getInt("rss.master.port",
      masterAddress(conf).split(",").head.split(":")(1).toInt)
  }

  def workerReplicateNumThreads(conf: RssConf): Int = {
    conf.getInt("rss.worker.replicate.numThreads", 64)
  }

  def workerAsyncCommitFileThreads(conf: RssConf): Int = {
    conf.getInt("rss.worker.asyncCommitFiles.numThreads", 32)
  }

  def workerFlushBufferSize(conf: RssConf): Long = {
    conf.getSizeAsBytes("rss.worker.flush.buffer.size", "256k")
  }

  def workerFlushQueueCapacity(conf: RssConf): Int = {
    conf.getInt("rss.worker.flush.queue.capacity", 512)
  }

  def workerFetchChunkSize(conf: RssConf): Long = {
    conf.getSizeAsBytes("rss.worker.fetch.chunk.size", "8m")
  }

  def workerNumSlots(conf: RssConf, numDisks: Int): Int = {
    val userNumSlots = conf.getInt("rss.worker.numSlots", -1)
    if (userNumSlots > 0) {
      userNumSlots
    } else {
      workerFlushQueueCapacity(conf: RssConf) * numDisks
    }
  }

  def rpcMaxParallelism(conf: RssConf): Int = {
    conf.getInt("rss.rpc.max.parallelism", 1024)
  }

  def registerShuffleMaxRetry(conf: RssConf): Int = {
    conf.getInt("rss.register.shuffle.max.retry", 3)
  }

  def registerShuffleRetryWait(conf: RssConf): Long = {
    conf.getTimeAsSeconds("rss.register.shuffle.retry.wait", "3s")
  }

  def flushTimeout(conf: RssConf): Long = {
    conf.getTimeAsSeconds("rss.flush.timeout", "120s")
  }

  def fileWriterTimeoutMs(conf: RssConf): Long = {
    conf.getTimeAsMs("rss.filewriter.timeout", "120s")
  }

  def noneEmptyDirExpireDurationMs(conf: RssConf): Long = {
    conf.getTimeAsMs("rss.expire.nonEmptyDir.duration", "3d")
  }

  def noneEmptyDirCleanUpThreshold(conf: RssConf): Int = {
    conf.getInt("rss.expire.nonEmptyDir.cleanUp.threshold", 10)
  }

  def emptyDirExpireDurationMs(conf: RssConf): Long = {
    conf.getTimeAsMs("rss.expire.emptyDir.duration", "2h")
  }

  def workerBaseDirs(conf: RssConf): Array[String] = {
    val baseDirs = conf.get("rss.worker.base.dirs", "")
    if (baseDirs.nonEmpty) {
      baseDirs.split(",")
    } else {
      val prefix = RssConf.workerBaseDirPrefix(conf)
      val number = RssConf.workerBaseDirNumber(conf)
      (1 to number).map(i => s"$prefix$i").toArray
    }
  }

  def workerBaseDirPrefix(conf: RssConf): String = {
    conf.get("rss.worker.base.dir.prefix", "/mnt/disk")
  }

  def workerBaseDirNumber(conf: RssConf): Int = {
    conf.getInt("rss.worker.base.dir.number", 16)
  }

  def stageEndTimeout(conf: RssConf): Long = {
    conf.getTimeAsMs("rss.stage.end.timeout", "240s")
  }

  def limitInFlightTimeoutMs(conf: RssConf): Long = {
    conf.getTimeAsMs("rss.limit.inflight.timeout", "240s")
  }

  def limitInFlightSleepDeltaMs(conf: RssConf): Long = {
    conf.getTimeAsMs("rss.limit.inflight.sleep.delta", "50ms")
  }

  def pushServerPort(conf: RssConf): Int = {
    conf.getInt("rss.pushserver.port", 0)
  }

  def fetchServerPort(conf: RssConf): Int = {
    conf.getInt("rss.fetchserver.port", 0)
  }

  def registerWorkerTimeoutMs(conf: RssConf): Long = {
    conf.getTimeAsMs("rss.register.worker.timeout", "180s")
  }

  def masterPortMaxRetry(conf: RssConf): Int = {
    conf.getInt("rss.master.port.maxretry", 1)
  }

  def pushDataRetryThreadNum(conf: RssConf): Int = {
    conf.getInt("rss.pushdata.retry.thread.num",
      Math.max(8, Runtime.getRuntime().availableProcessors()))
  }

  def metricsSystemEnable(conf: RssConf): Boolean = {
    conf.getBoolean("rss.metrics.system.enabled", defaultValue = true)
  }

  def metricsTimerSlidingSize(conf: RssConf): Int = {
    conf.getInt("rss.metrics.system.timer.sliding.size", 4000)
  }

  // 0 to 1.0
  def metricsSampleRate(conf: RssConf): Double = {
    conf.getDouble("rss.metrics.system.sample.rate", 1)
  }

  def metricsSystemSamplePerfCritical(conf: RssConf): Boolean = {
    conf.getBoolean("rss.metrics.system.sample.perf.critical", false)
  }

  def metricsSlidingWindowSize(conf: RssConf): Int = {
    conf.getInt("rss.metrics.system.sliding.window.size", 4096)
  }

  def innerMetricsSize(conf: RssConf): Int = {
    conf.getInt("rss.inner.metrics.size", 4096)
  }

  def masterPrometheusMetricPort(conf: RssConf): Int = {
    conf.getInt("rss.master.prometheus.metric.port", 9098)
  }

  def workerPrometheusMetricPort(conf: RssConf): Int = {
    conf.getInt("rss.worker.prometheus.metric.port", 9096)
  }

  def clusterLoadFallbackEnabled(conf: RssConf): Boolean = {
    conf.getBoolean("rss.clusterLoad.fallback.enabled", defaultValue = true)
  }

  def offerSlotsExtraSize(conf: RssConf): Int = {
    conf.getInt("rss.offer.slots.extra.size", 2)
  }

  def shuffleWriterMode(conf: RssConf): String = {
    conf.get("rss.shuffle.writer.mode", "hash")
  }

  def sortPushThreshold(conf: RssConf): Long = {
    conf.getSizeAsBytes("rss.sort.push.data.threshold", "64m")
  }

  def driverMetaServicePort(conf: RssConf): Int = {
    val port = conf.getInt("rss.driver.metaService.port", 0)
    if (port != 0) {
      logWarning("The user specifies the port used by the LifecycleManager on the Driver, and its" +
        s" values is $port, which may cause port conflicts and startup failure.")
    }
    port
  }

  def closeIdleConnections(conf: RssConf): Boolean = {
    conf.getBoolean("rss.worker.closeIdleConnections", defaultValue = true)
  }

  def replicateFastFailDurationMs(conf: RssConf): Long = {
    conf.getTimeAsMs("rss.replicate.fastfail.duration", "60s")
  }

  def maxPartitionNumSupported(conf: RssConf): Long = {
    conf.getInt("rss.max.partition.number", 100000)
  }

  def forceFallback(conf: RssConf): Boolean = {
    conf.getBoolean("rss.force.fallback", false)
  }

  def deviceMonitorEnabled(conf: RssConf): Boolean = {
    conf.getBoolean("rss.device.monitor.enabled", true)
  }

  def diskCheckIntervalMs(conf: RssConf): Long = {
    conf.getTimeAsMs("rss.disk.check.interval", "60s")
  }

  def slowFlushIntervalMs(conf: RssConf): Long = {
    conf.getTimeAsMs("rss.slow.flush.interval", "10s")
  }

  def sysBlockDir(conf: RssConf): String = {
    conf.get("rss.sys.block.dir", "/sys/block")
  }

  def createFileWriterRetryCount(conf: RssConf): Int = {
    conf.getInt("rss.create.file.writer.retry.count", 3)
  }

  /**
   * Be aware that [rss.disk.space.safe.watermark.size] cannot be set to fractional values
   * @param conf rss config
   * @return the watermark size in GB
   */
  def diskSpaceSafeWatermarkSizeInGb(conf: RssConf): Long = {
    conf.getSizeAsGb("rss.disk.space.safe.watermark.size", "0GB")
  }

  def workerStatusCheckTimeout(conf: RssConf): Long = {
    conf.getTimeAsSeconds("rss.worker.status.check.timeout", "30s")
  }

  /**
   * Add non empty and non null suffix to a key.
   */
  def concatKeySuffix(key: String, suffix: String): String = {
    if (suffix == null || suffix.isEmpty) return key
    s"${key}.${suffix}"
  }

  /**
   * Ratis related config
   */
  def haEnabled(conf: RssConf): Boolean = {
    conf.getBoolean("rss.ha.enabled", false)
  }

  def haMasterHosts(conf: RssConf): String = {
    conf.get("rss.ha.master.hosts", masterHostsFromAddress(conf))
  }

  def haClientMaxTries(conf: RssConf): Int = {
    conf.getInt("rss.ha.client.maxTries", 15)
  }

  def haStorageDir(conf: RssConf): String = {
    conf.get(HA_RATIS_STORAGE_DIR, HA_RATIS_STORAGE_DIR_DEFAULT)
  }

  def clusterSlotsUsageLimitPercent(conf: RssConf): Double = {
    conf.getDouble("rss.slots.usage.overload.percent", 0.95)
  }

  def partitionSplitThreshold(conf: RssConf): Long = {
    conf.getSizeAsBytes("rss.partition.split.threshold", "256m")
  }

  def partitionSplitMode(conf: RssConf): PartitionSplitMode = {
    val modeStr = conf.get("rss.partition.split.mode", "soft")
    modeStr match {
      case "soft" => PartitionSplitMode.soft
      case "hard" => PartitionSplitMode.hard
      case _ => logWarning(s"Invalid split mode ${modeStr}, use soft mode by default")
        PartitionSplitMode.soft
    }
  }

  def clientSplitPoolSize(conf: RssConf): Int = {
    conf.getInt("rss.client.split.pool.size", 8)
  }

  def partitionSortTimeout(conf: RssConf): Long = {
    conf.getTimeAsMs("rss.partition.sort.timeout", "220s")
  }

  def partitionSortMaxMemoryRatio(conf: RssConf): Double = {
    conf.getDouble("rss.partition.sort.memory.max.ratio", 0.1)
  }

  def memoryTrimActionThreshold(conf: RssConf): Int = {
    conf.getInt("rss.memory.trim.action.threshold", 10)
  }

  def workerPausePushDataRatio(conf: RssConf): Double = {
    conf.getDouble("rss.pause.pushdata.memory.ratio", 0.85)
  }

  def workerPauseRepcaliteRatio(conf: RssConf): Double = {
    conf.getDouble("rss.pause.replicate.memory.ratio", 0.95)
  }

  def workerResumeRatio(conf: RssConf): Double = {
    conf.getDouble("rss.resume.memory.ratio", 0.5)
  }

  def memoryReservedForSingleSort(conf: RssConf): Long = {
    conf.getSizeAsBytes("rss.worker.reserveForSingleSort.memory", "1mb")
  }

  def workerDirectMemoryPressureCheckIntervalMs(conf: RssConf): Int = {
    conf.getInt("rss.worker.memory.check.interval", 10)
  }

  def workerDirectMemoryReportIntervalSecond(conf: RssConf): Int = {
    Utils.timeStringAsSeconds(conf.get("rss.worker.memory.report.interval",
      "10s")).toInt
  }

  val WorkingDirName = "hadoop/rss-worker/shuffle_data"

  // If we want to use multi-raft group we can
  // add "rss.ha.service.ids" each for one raft group
  val HA_SERVICE_ID_KEY = "rss.ha.service.id"
  val HA_NODES_KEY = "rss.ha.nodes"
  val HA_ADDRESS_KEY = "rss.ha.address"
  val HA_RATIS_PORT_KEY = "rss.ha.port"
  val HA_RATIS_PORT_DEFAULT = 9872

  val HA_RPC_TYPE_KEY: String = "rss.ha.rpc.type"
  val HA_RPC_TYPE_DEFAULT: String = "NETTY"

  val HA_RATIS_STORAGE_DIR: String = "rss.ha.storage.dir"
  val HA_RATIS_STORAGE_DIR_DEFAULT: String = "/tmp/ratis"
  val HA_RATIS_SEGMENT_SIZE_KEY: String = "rss.ha.ratis.segment.size"

  val HA_RATIS_SEGMENT_SIZE_DEFAULT: String = "4MB"
  val HA_RATIS_SEGMENT_PREALLOCATED_SIZE_KEY: String = "rss.ratis.segment.preallocated.size"
  val HA_RATIS_SEGMENT_PREALLOCATED_SIZE_DEFAULT: String = "4MB"

  //  Ratis Log Appender configurations
  val HA_RATIS_LOG_APPENDER_QUEUE_NUM_ELEMENTS = "rss.ratis.log.appender.queue.num-elements"
  val HA_RATIS_LOG_APPENDER_QUEUE_NUM_ELEMENTS_DEFAULT = 1024
  val HA_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT = "rss.ratis.log.appender.queue.byte-limit"
  val HA_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT_DEFAULT = "32MB"
  val HA_RATIS_LOG_PURGE_GAP = "rss.ratis.log.purge.gap"
  val HA_RATIS_LOG_PURGE_GAP_DEFAULT = 1000000

  //  Ratis server configurations
  val HA_RATIS_SERVER_REQUEST_TIMEOUT_KEY = "rss.ratis.server.request.timeout"
  val HA_RATIS_SERVER_REQUEST_TIMEOUT_DEFAULT = "3s"
  val HA_RATIS_SERVER_RETRY_CACHE_TIMEOUT_KEY = "rss.ratis.server.retry.cache.timeout"
  val HA_RATIS_SERVER_RETRY_CACHE_TIMEOUT_DEFAULT = "600s"
  val HA_RATIS_MINIMUM_TIMEOUT_KEY = "rss.ratis.minimum.timeout"
  val HA_RATIS_MINIMUM_TIMEOUT_DEFAULT = "3s"

  //  Ratis Leader Election configurations
  val HA_RATIS_SERVER_FAILURE_TIMEOUT_DURATION_KEY = "rss.ratis.server.failure.timeout.duration"
  val HA_RATIS_SERVER_FAILURE_TIMEOUT_DURATION_DEFAULT = "120s"

  val HA_RATIS_SERVER_ROLE_CHECK_INTERVAL_KEY = "rss.ratis.server.role.check.interval"
  val HA_RATIS_SERVER_ROLE_CHECK_INTERVAL_DEFAULT = "1s"

  // Ratis snapshot configurations
  val HA_RATIS_SNAPSHOT_AUTO_TRIGGER_ENABLED_KEY = "rss.ha.ratis.snapshot.auto.trigger.enabled"
  val HA_RATIS_SNAPSHOT_AUTO_TRIGGER_ENABLED_DEFAULT = false
  val HA_RATIS_SNAPSHOT_AUTO_TRIGGER_THRESHOLD_KEY = "rss.ha.ratis.snapshot.auto.trigger.threshold"
  val HA_RATIS_SNAPSHOT_AUTO_TRIGGER_THRESHOLD_DEFAULT = 200000
  val HA_RATIS_SNAPSHOT_RETENTION_FILE_NUM_KEY = "rss.ratis.snapshot.retention.file.num"
  val HA_RATIS_SNAPSHOT_RETENTION_FILE_NUM_DEFAULT = 3
}
