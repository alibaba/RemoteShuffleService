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

import java.io.{File, IOException}
import java.nio.channels.{ClosedByInterruptException, FileChannel}
import java.util.{ArrayList, HashSet => JHashSet, Set => JSet}
import java.util.concurrent.{ConcurrentHashMap, Executors, LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.function.IntUnaryOperator

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

import com.google.common.util.concurrent.ThreadFactoryBuilder
import io.netty.buffer.{CompositeByteBuf, Unpooled}

import com.aliyun.emr.rss.common.RssConf
import com.aliyun.emr.rss.common.exception.RssException
import com.aliyun.emr.rss.common.internal.Logging
import com.aliyun.emr.rss.common.metrics.source.AbstractSource
import com.aliyun.emr.rss.common.network.server.MemoryTracker
import com.aliyun.emr.rss.common.network.server.MemoryTracker.MemoryTrackerListener
import com.aliyun.emr.rss.common.protocol.{PartitionLocation, PartitionSplitMode}
import com.aliyun.emr.rss.common.util.{ThreadUtils, Utils}

private[worker] case class FlushTask(
    buffer: CompositeByteBuf,
    fileChannel: FileChannel,
    notifier: FileWriter.FlushNotifier)

private[worker] final class DiskFlusher(
    val workingDir: File,
    queueCapacity: Int,
    workerSource: AbstractSource,
    val deviceMonitor: DeviceMonitor,
    val threadCount: Int) extends DeviceObserver with Logging {
  private lazy val diskFlusherId = System.identityHashCode(this)
  private val workingQueue = new LinkedBlockingQueue[FlushTask](queueCapacity)
  private val bufferQueue = new LinkedBlockingQueue[CompositeByteBuf](queueCapacity)
  private val writeActionPool =
    ThreadUtils.newDaemonFixedThreadPool(threadCount, workingDir.getName + "-flusher")
  @volatile private var lastBeginFlushTime: Long = -1
  val stopFlag = new AtomicBoolean(false)
  private val rand = new Random()

  for (_ <- 0 until queueCapacity) {
    bufferQueue.put(Unpooled.compositeBuffer(256))
  }

  private val worker = new Thread(s"$this") {
    override def run(): Unit = {
      while (!stopFlag.get()) {
        val task = workingQueue.take()
        writeActionPool.submit(new Runnable {
          override def run(): Unit = {
            val key = s"DiskFlusher-$workingDir-${rand.nextInt()}"
            workerSource.sample(WorkerSource.FLUSH_DATA_TIME, key) {
              if (!task.notifier.hasException) {
                try {
                  lastBeginFlushTime = System.nanoTime()
                  task.fileChannel.write(task.buffer.nioBuffers())
                } catch {
                  case _: ClosedByInterruptException =>
                  case e: IOException =>
                    task.notifier.setException(e)
                    stopFlag.set(true)
                    logError(s"$this write failed, report to DeviceMonitor, exeption: $e")
                    reportError(workingDir, e, DeviceErrorType.ReadOrWriteFailure)
                }
                lastBeginFlushTime = -1
              }
              returnBuffer(task.buffer)
              task.notifier.numPendingFlushes.decrementAndGet()
            }
          }
        })
      }
    }
  }
  worker.setDaemon(true)
  worker.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler {
    override def uncaughtException(t: Thread, e: Throwable): Unit = {
      logError(s"$this thread terminated.", e)
    }
  })
  worker.start()

  deviceMonitor.registerDiskFlusher(this)

  def takeBuffer(timeoutMs: Long): CompositeByteBuf = {
    bufferQueue.poll(timeoutMs, TimeUnit.MILLISECONDS)
  }

  def returnBuffer(buffer: CompositeByteBuf): Unit = {
    MemoryTracker.instance().releaseDiskBuffer(buffer.readableBytes())
    buffer.removeComponents(0, buffer.numComponents())
    buffer.clear()

    bufferQueue.put(buffer)
  }

  def addTask(task: FlushTask, timeoutMs: Long): Boolean = {
    workingQueue.offer(task, timeoutMs, TimeUnit.MILLISECONDS)
  }

  override def notifyError(
      deviceName: String,
      dirs: ListBuffer[File] = null,
      deviceErrorType: DeviceErrorType): Unit = {
    logError(s"$this is notified Device $deviceName Error $deviceErrorType! Stop Flusher.")
    stopFlag.set(true)
    try {
      worker.interrupt()
      writeActionPool.shutdown()
    } catch {
      case e: Exception =>
        logError(s"Exception when interrupt worker: $worker.", e)
    }
    workingQueue.asScala.foreach { task =>
      task.buffer.removeComponents(0, task.buffer.numComponents())
      task.buffer.clear()
    }
    deviceMonitor.unregisterDiskFlusher(this)
  }

  override def reportError(
      workingDir: File,
      e: IOException,
      deviceErrorType: DeviceErrorType): Unit = {
    deviceMonitor.reportDeviceError(workingDir, e, deviceErrorType)
  }

  def getLastFlushTime: Long = lastBeginFlushTime

  def bufferQueueInfo(): String = s"$this available buffers: ${bufferQueue.size()}"

  override def hashCode(): Int = {
    workingDir.hashCode()
  }

  override def equals(obj: Any): Boolean = {
    obj.isInstanceOf[DiskFlusher] && obj.asInstanceOf[DiskFlusher].workingDir.equals(workingDir)
  }

  override def toString(): String = s"DiskFlusher@$diskFlusherId-" + workingDir.toString
}

private[worker] final class LocalStorageManager(
    conf: RssConf,
    workerSource: AbstractSource,
    worker: Worker)
  extends DeviceObserver with Logging with MemoryTrackerListener{

  val isolatedWorkingDirs =
    new ConcurrentHashMap[File, DeviceErrorType](RssConf.workerBaseDirs(conf).length)

  val diskFlusherThreadCount = RssConf.diskFlusherThreadCount(conf)

  private val workingDirs: ArrayList[File] = {
    val baseDirs = RssConf.workerBaseDirs(conf).map(new File(_, RssConf.WorkingDirName))
    val availableDirs = new mutable.HashSet[File]()
    baseDirs.foreach { dir =>
      if (!DeviceMonitor.checkDiskReadAndWrite(conf, ListBuffer[File](dir))) {
        availableDirs += dir
      } else {
        logWarning(s"Exception when trying to create a file in $dir, add to blacklist.")
        isolatedWorkingDirs.put(dir, DeviceErrorType.ReadOrWriteFailure)
      }
    }
    if (availableDirs.size <= 0) {
      throw new IOException("No available working directory.")
    }
    new ArrayList[File](availableDirs.asJava)
  }

  def workingDirsSnapshot(): ArrayList[File] =
    workingDirs.synchronized(new ArrayList[File](workingDirs))

  def hasAvailableWorkingDirs(): Boolean = workingDirsSnapshot().size() > 0

  private val dirOperators: ConcurrentHashMap[File, ThreadPoolExecutor] = {
    val cleaners = new ConcurrentHashMap[File, ThreadPoolExecutor]()
    workingDirsSnapshot().asScala.foreach { dir =>
      cleaners.put(dir,
        ThreadUtils.newDaemonCachedThreadPool(s"Disk-cleaner-${dir.getAbsoluteFile}", 1))
    }
    cleaners
  }

  private val deviceMonitor = DeviceMonitor.createDeviceMonitor(conf, this, workingDirsSnapshot())

  private val diskFlusher: ConcurrentHashMap[File, DiskFlusher] = {
    val queueCapacity = RssConf.workerFlushQueueCapacity(conf)
    val flushers = new ConcurrentHashMap[File, DiskFlusher]()
    workingDirsSnapshot().asScala.foreach { dir =>
      flushers.put(dir,
        new DiskFlusher(dir, queueCapacity, workerSource, deviceMonitor, diskFlusherThreadCount))
    }
    flushers
  }
  private val actionService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
    .setNameFormat("StorageManager-action-thread").build)

  deviceMonitor.startCheck()

  override def notifyError(
      deviceName: String,
      dirs: ListBuffer[File],
      deviceErrorType: DeviceErrorType): Unit = this.synchronized {
    val availableDisks = numDisks()
    // add to isolatedWorkingDirs and decrease current slots and report
    dirs.foreach { dir =>
      workingDirs.synchronized {
        workingDirs.remove(dir)
      }

      val operator = dirOperators.remove(dir)
      if (operator != null) {
        operator.shutdown()
      }
      diskFlusher.remove(dir)

      isolatedWorkingDirs.put(dir, deviceErrorType)
    }

    if (availableDisks != numDisks()) {
      val numSlots = RssConf.workerNumSlots(conf, numDisks())
      logError(s"LocalStorageManager is notified DeviceError," +
        s"dirs $dirs, updated numSlots: $numSlots")
      worker.updateNumSlots(numSlots)
    }
  }

  override def notifyHealthy(dirs: ListBuffer[File]): Unit = this.synchronized {
    val availableDisks = numDisks()
    val queueCapacity = RssConf.workerFlushQueueCapacity(conf)
    dirs.foreach { dir =>
      isolatedWorkingDirs.remove(dir)
      if (!diskFlusher.containsKey(dir)) {
        diskFlusher.put(dir, new DiskFlusher(dir, queueCapacity, workerSource,
          deviceMonitor, diskFlusherThreadCount))
      }
      if (!dirOperators.containsKey(dir)) {
        dirOperators.put(dir,
          ThreadUtils.newDaemonCachedThreadPool(s"Disk-cleaner-${dir.getAbsoluteFile}", 1))
      }
      workingDirs.synchronized {
        if (!workingDirs.contains(dir)) {
          workingDirs.add(dir)
        }
      }
    }

    if (availableDisks != numDisks()) {
      val numSlots = RssConf.workerNumSlots(conf, numDisks())
      logInfo(s"LocalStorageManager is notified healthy," +
        s"dirs $dirs, updated numSlots: $numSlots")
      worker.updateNumSlots(numSlots)
    }
  }

  private def isolateDirs(
      dirs: ListBuffer[File],
      errorType: DeviceErrorType): Unit = this.synchronized {
    val availableDisks = numDisks()
    dirs.foreach { dir =>
      workingDirs.synchronized {
        workingDirs.remove(dir)
      }
      isolatedWorkingDirs.put(dir, errorType)
    }
    if (availableDisks != numDisks()) {
      val numSlots = RssConf.workerNumSlots(conf, numDisks())
      logError(s"LocalStorageManager is notified high disk usage," +
        s"dirs $dirs, updated numSlots: $numSlots")
      worker.updateNumSlots(numSlots)
    }
  }

  override def notifyHighDiskUsage(dirs: ListBuffer[File]): Unit = {
    isolateDirs(dirs, DeviceErrorType.InsufficientDiskSpace)
  }

  override def notifySlowFlush(dirs: ListBuffer[File]): Unit = {
    isolateDirs(dirs, DeviceErrorType.FlushTimeout)
  }

  private val counter = new AtomicInteger()
  private val counterOperator = new IntUnaryOperator() {
    override def applyAsInt(operand: Int): Int = {
      val dirs = workingDirsSnapshot()
      if (dirs.size() > 0) {
        (operand + 1) % dirs.size()
      } else {
        0
      }
    }
  }

  // shuffleKey -> (fileName -> writer)
  private val writers = new ConcurrentHashMap[String, ConcurrentHashMap[String, FileWriter]]()

  private def getNextIndex() = counter.getAndUpdate(counterOperator)

  private val newMapFunc =
    new java.util.function.Function[String, ConcurrentHashMap[String, FileWriter]]() {
      override def apply(key: String): ConcurrentHashMap[String, FileWriter] =
        new ConcurrentHashMap()
    }

  def numDisks(): Int = workingDirs.synchronized {
    workingDirs.size()
  }

  @throws[IOException]
  def createWriter(
      appId: String,
      shuffleId: Int,
      location: PartitionLocation,
      splitThreshold: Long,
      splitMode: PartitionSplitMode): FileWriter = {
    if (!hasAvailableWorkingDirs()) {
      throw new IOException("No available working dirs!")
    }
    createWriter(appId, shuffleId, location.getReduceId, location.getEpoch,
      location.getMode, splitThreshold, splitMode)
  }

  @throws[IOException]
  def createWriter(
      appId: String,
      shuffleId: Int,
      reduceId: Int,
      epoch: Int,
      mode: PartitionLocation.Mode,
      splitThreshold: Long,
      splitMode: PartitionSplitMode): FileWriter = {
    val fileName = s"$reduceId-$epoch-${mode.mode()}"

    var retryCount = 0
    var exception: IOException = null
    val maxRetryCount = RssConf.createFileWriterRetryCount(conf)
    while (retryCount < maxRetryCount) {
      val index = getNextIndex()
      val dirs = workingDirsSnapshot()
      val dir = dirs.get(index % dirs.size())
      val shuffleDir = new File(dir, s"$appId/$shuffleId")
      val file = new File(shuffleDir, fileName)

      try {
        shuffleDir.mkdirs()
        val createFileSuccess = file.createNewFile()
        if (!createFileSuccess) {
          throw new RssException("create app shuffle data dir or file failed")
        }
        val fileWriter = new FileWriter(file, diskFlusher.get(dir), dir,
          RssConf.workerFetchChunkSize(conf),
          RssConf.workerFlushBufferSize(conf),
          workerSource, conf, deviceMonitor, splitThreshold, splitMode)
        deviceMonitor.registerFileWriter(fileWriter)
        val shuffleKey = Utils.makeShuffleKey(appId, shuffleId)
        val shuffleMap = writers.computeIfAbsent(shuffleKey, newMapFunc)
        shuffleMap.put(fileName, fileWriter)
        return fileWriter
      } catch {
        case t: Throwable =>
          logError("Create Writer failed, report to DeviceMonitor", t)
          exception = new IOException(t)
          deviceMonitor.reportDeviceError(dir, exception, DeviceErrorType.ReadOrWriteFailure)
      }
      retryCount += 1
    }

    throw exception
  }

  def getWriter(shuffleKey: String, fileName: String): FileWriter = {
    val shuffleMap = writers.get(shuffleKey)
    if (shuffleMap ne null) {
      shuffleMap.get(fileName)
    } else {
      null
    }
  }

  def shuffleKeySet(): JSet[String] = writers.keySet()

  def cleanupExpiredShuffleKey(expiredShuffleKeys: JHashSet[String]): Unit = {
    val workingDirs = workingDirsSnapshot()
    workingDirs.addAll(isolatedWorkingDirs.asScala.filterNot { entry =>
      DeviceErrorType.criticalError(entry._2)
    }.keySet.asJava)

    expiredShuffleKeys.asScala.foreach { shuffleKey =>
      logInfo(s"Cleanup expired shuffle $shuffleKey.")
      writers.remove(shuffleKey)
      val (appId, shuffleId) = Utils.splitShuffleKey(shuffleKey)
      workingDirs.asScala.foreach { workingDir =>
        val dir = new File(workingDir, s"$appId/$shuffleId")
        deleteDirectory(dir, dirOperators.get(workingDir))
      }
    }
  }

  private val noneEmptyDirExpireDurationMs = RssConf.noneEmptyDirExpireDurationMs(conf)
  private val noneEmptyDirCleanUpThreshold = RssConf.noneEmptyDirCleanUpThreshold(conf)
  private val emptyDirExpireDurationMs = RssConf.emptyDirExpireDurationMs(conf)
  private val rssSlowFlushInterval: Long = RssConf.slowFlushIntervalMs(conf)

  private val localStorageScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("local-storage-scheduler")

  localStorageScheduler.scheduleAtFixedRate(new Runnable {
    override def run(): Unit = {
      try {
        // Clean up empty dirs, since the appDir do not delete during expired shuffle key cleanup
        cleanupExpiredAppDirs(false, System.currentTimeMillis() - emptyDirExpireDurationMs)

        // Clean up non-empty dirs which has not been modified
        // in the past {{noneEmptyExpireDurationsMs}}, since
        // non empty dirs may exist after cluster restart.
        cleanupExpiredAppDirs(true, System.currentTimeMillis() - noneEmptyDirExpireDurationMs)
      } catch {
        case exception: Exception =>
          logError(s"Cleanup expired shuffle data failed.", exception)
      }
    }
  }, 30, 30, TimeUnit.MINUTES)


  localStorageScheduler.scheduleAtFixedRate(new Runnable {
    override def run(): Unit = {
      val currentTime = System.nanoTime()
      diskFlusher.values().asScala.foreach(flusher => {
        logInfo(flusher.bufferQueueInfo())
        val lastFlushTime = flusher.getLastFlushTime
        if (lastFlushTime != -1 &&
          currentTime - lastFlushTime  > rssSlowFlushInterval * 1000 * 1000) {
          flusher.reportError(
            flusher.workingDir, new IOException("Slow Flusher!"), DeviceErrorType.FlushTimeout)
        }
      })
    }
  }, rssSlowFlushInterval, rssSlowFlushInterval, TimeUnit.MILLISECONDS)

  private def cleanupExpiredAppDirs(
      deleteRecursively: Boolean,
      expireDuration: Long): Unit = {
    val workingDirs = workingDirsSnapshot()
    workingDirs.addAll(isolatedWorkingDirs.asScala.filterNot { entry =>
      DeviceErrorType.criticalError(entry._2)
    }.keySet.asJava)

    workingDirs.asScala.foreach { workingDir =>
      var appDirs = workingDir.listFiles

      if (appDirs != null) {
        if (deleteRecursively) {
          appDirs = appDirs.sortBy(_.lastModified()).take(noneEmptyDirCleanUpThreshold)

          for (appDir <- appDirs if appDir.lastModified() < expireDuration) {
            val executionThreadPool = dirOperators.get(workingDir)
            deleteDirectory(appDir, executionThreadPool)
            logInfo(s"Deleted expired app dirs $appDir.")
          }
        } else {
          for (appDir <- appDirs if appDir.lastModified() < expireDuration) {
            if (appDir.list().isEmpty) {
              deleteFileWithRetry(appDir)
              logInfo(s"Deleted expired app dirs $appDir.")
            }
          }
        }
      }
    }
  }

  private def deleteDirectory(dir: File, threadPool: ThreadPoolExecutor): Unit = {
    val allContents = dir.listFiles
    if (allContents != null) {
      for (file <- allContents) {
        deleteDirectory(file, threadPool)
      }
    }
    threadPool.submit(new Runnable {
      override def run(): Unit = {
        deleteFileWithRetry(dir)
      }
    })
  }

  private def deleteFileWithRetry(file: File): Unit = {
    if (file.exists()) {
      var retryCount = 0
      var deleteSuccess = false
      while (!deleteSuccess && retryCount <= 3) {
        deleteSuccess = file.delete()
        retryCount = retryCount + 1
        if (!deleteSuccess) {
          Thread.sleep(200 * retryCount)
        }
      }
      if (deleteSuccess) {
        logInfo(s"Deleted expired app dirs $file.")
      } else {
        logWarning(s"Delete expired app dirs $file failed.")
      }
    }
  }

  def close(): Unit = {
    if (null != dirOperators) {
      dirOperators.asScala.foreach(_._2.shutdownNow)
    }
    localStorageScheduler.shutdownNow()
    if (null != deviceMonitor) {
      deviceMonitor.close()
    }
  }

  private def flushFileWriters(): Unit = {
    writers.entrySet().asScala.foreach { u =>
      u.getValue.asScala.foreach {
        f => f._2.flushOnMemoryPressure()
      }
    }
  }

  override def onPause(moduleName: String): Unit = {}

  override def onResume(moduleName: String): Unit = {}

  override def onTrim(): Unit = {
    actionService.submit(new Runnable {
      override def run(): Unit = {
        flushFileWriters()
      }
    })
  }
}
