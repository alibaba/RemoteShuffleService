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
import java.nio.file.{Files, Paths}
import java.util
import java.util.concurrent.{ConcurrentHashMap, Executors, LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, LongAdder}
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
import com.aliyun.emr.rss.common.meta.DiskInfo
import com.aliyun.emr.rss.common.metrics.source.AbstractSource
import com.aliyun.emr.rss.common.network.server.MemoryTracker
import com.aliyun.emr.rss.common.network.server.MemoryTracker.MemoryTrackerListener
import com.aliyun.emr.rss.common.protocol.{PartitionLocation, PartitionSplitMode, PartitionType, StorageHint}
import com.aliyun.emr.rss.common.util.{ThreadUtils, Utils}

private[worker] case class FlushTask(
  buffer: CompositeByteBuf,
  fileChannel: FileChannel,
  notifier: FileWriter.FlushNotifier)

private[worker] final class DiskFlusher(
  val workingDir: File,
  workerSource: AbstractSource,
  val deviceMonitor: DeviceMonitor,
  val threadCount: Int,
  val mountPoint: String,
  val diskType: StorageHint.Type) extends DeviceObserver with Logging {
  private lazy val diskFlusherId = System.identityHashCode(this)
  private val assumedQueueSize = 1024 * 1024
  private val workingQueues = new Array[LinkedBlockingQueue[FlushTask]](threadCount)
  private val bufferQueue = new LinkedBlockingQueue[CompositeByteBuf](assumedQueueSize)
  private val workers = new Array[Thread](threadCount)
  private val nextWorkerIndex = new AtomicInteger()
  private val flushCount = new LongAdder
  private val flushTotalTime = new LongAdder
  private val usedSlots = new LongAdder

  @volatile
  private var lastBeginFlushTime: Long = -1
  def getLastFlushTime: Long = lastBeginFlushTime

  val stopFlag = new AtomicBoolean(false)
  val rand = new Random()

  init()

  private def init(): Unit = {

    for (_ <- 0 until assumedQueueSize) {
      bufferQueue.put(Unpooled.compositeBuffer(256))
    }
    for (index <- 0 until (threadCount)) {
      workingQueues(index) = new LinkedBlockingQueue[FlushTask](assumedQueueSize)
      workers(index) = new Thread(s"$this-$index") {
        override def run(): Unit = {
          while (!stopFlag.get()) {
            val task = workingQueues(index).take()
            val key = s"DiskFlusher-$workingDir-${rand.nextInt()}"
            workerSource.sample(WorkerSource.FlushDataTime, key) {
              if (!task.notifier.hasException) {
                try {
                  val flushBeginTime = System.nanoTime()
                  lastBeginFlushTime = flushBeginTime
                  task.fileChannel.write(task.buffer.nioBuffers())
                  flushTotalTime.add(System.nanoTime() - flushBeginTime)
                  flushCount.increment()
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
        }
      }
      workers(index).setDaemon(true)
      workers(index).setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler {
        override def uncaughtException(t: Thread, e: Throwable): Unit = {
          logError(s"$this thread terminated.", e)
        }
      })
      workers(index).start()
    }

    deviceMonitor.registerDiskFlusher(this)
  }

  def getWorkerIndex: Int = {
    val nextIndex = nextWorkerIndex.getAndIncrement()
    if (nextIndex > threadCount) {
      nextWorkerIndex.set(0)
    }
    nextIndex % threadCount
  }

  def averageFlushTime(): Double = {
    val tmpFlushTotalTime = flushTotalTime.sumThenReset()
    val tmpFlushCount = flushCount.sumThenReset()
    if (tmpFlushCount != 0) {
      tmpFlushTotalTime / tmpFlushCount
    } else {
      0
    }
  }

  def addWriter(): Unit = {
    usedSlots.increment()
  }

  def removeWriter(): Unit = {
    usedSlots.decrement()
  }

  def getUsedSlots(): Long = {
    usedSlots.sum();
  }

  def takeBuffer(timeoutMs: Long): CompositeByteBuf = {
    bufferQueue.poll(timeoutMs, TimeUnit.MILLISECONDS)
  }

  def returnBuffer(buffer: CompositeByteBuf): Unit = {
    MemoryTracker.instance().releaseDiskBuffer(buffer.readableBytes())
    buffer.removeComponents(0, buffer.numComponents())
    buffer.clear()

    bufferQueue.put(buffer)
  }

  def addTask(task: FlushTask, timeoutMs: Long, workerIndex: Int): Boolean = {
    workingQueues(workerIndex).offer(task, timeoutMs, TimeUnit.MILLISECONDS)
  }

  override def notifyError(deviceName: String, dirs: ListBuffer[File] = null,
    deviceErrorType: DeviceErrorType): Unit = {
    logError(s"$this is notified Device $deviceName Error $deviceErrorType! Stop Flusher.")
    stopFlag.set(true)
    try {
      workers.foreach(_.interrupt())
    } catch {
      case e: Exception =>
        logError(s"Exception when interrupt worker: $workers, $e")
    }
    workingQueues.foreach { queue =>
      queue.asScala.foreach { task =>
        task.buffer.removeComponents(0, task.buffer.numComponents())
        task.buffer.clear()
      }
    }
    deviceMonitor.unregisterDiskFlusher(this)
  }

  override def reportError(workingDir: File, e: IOException,
    deviceErrorType: DeviceErrorType): Unit = {
    deviceMonitor.reportDeviceError(workingDir, e, deviceErrorType)
  }

  def bufferQueueInfo(): String = s"$this used buffers: ${bufferQueue.size()}"

  override def hashCode(): Int = {
    workingDir.hashCode()
  }

  override def equals(obj: Any): Boolean = {
    obj.isInstanceOf[DiskFlusher] &&
      obj.asInstanceOf[DiskFlusher].workingDir.equals(workingDir)
  }

  override def toString(): String = {
    s"DiskFlusher@$diskFlusherId-" + workingDir.toString
  }
}

private[worker] final class LocalStorageManager(
  conf: RssConf,
  workerSource: AbstractSource,
  worker: Worker) extends DeviceObserver with Logging with MemoryTrackerListener{

  val isolatedWorkingDirs =
    new ConcurrentHashMap[File, DeviceErrorType](RssConf.workerBaseDirs(conf).length)

  private val workingDirMetas: mutable.HashMap[String, (Long, Int, StorageHint.Type)] =
    new mutable.HashMap[String, (Long, Int, StorageHint.Type)]()
    new ConcurrentHashMap[String, ConcurrentHashMap[File, FileWriter]]()
  // mountpoint -> filewriter
  val workingDirWriters = new ConcurrentHashMap[File, util.ArrayList[FileWriter]]()

  private val workingDirWriterListFunc =
    new java.util.function.Function[File, util.ArrayList[FileWriter]]() {
      override def apply(t: File): util.ArrayList[FileWriter] = new util.ArrayList[FileWriter]()
    }

  private val workingDirs: util.ArrayList[File] = {
    val baseDirs =
      RssConf.workerBaseDirs(conf).map { case (workdir, maxSpace, flusherThread, storageHint) =>
        val actualWorkingDirFile = new File(workdir, RssConf.WorkingDirName)
        workingDirMetas +=
          actualWorkingDirFile.getAbsolutePath -> (maxSpace, flusherThread, storageHint)
        (actualWorkingDirFile, maxSpace, flusherThread, storageHint)
      }
    val availableDirs = new mutable.HashSet[File]()

    baseDirs.foreach { case (file, _, _, _) =>
      if (!DeviceMonitor.checkDiskReadAndWrite(conf, ListBuffer[File](file))) {
        availableDirs += file
      } else {
        logWarning(s"Exception when trying to create a file in ${file}, add to blacklist.")
        isolatedWorkingDirs.put(file, DeviceErrorType.ReadOrWriteFailure)
      }
    }
    if (availableDirs.size <= 0) {
      throw new IOException("No available working directory.")
    }
    new util.ArrayList[File](availableDirs.asJava)
  }

  def workingDirsSnapshot(): util.ArrayList[File] =
    workingDirs.synchronized(new util.ArrayList[File](workingDirs))

  def hasAvailableWorkingDirs(): Boolean = workingDirsSnapshot().size() > 0

  val writerFlushBufferSize: Long = RssConf.workerFlushBufferSize(conf)

  private val dirOperators: ConcurrentHashMap[File, ThreadPoolExecutor] = {
    val cleaners = new ConcurrentHashMap[File, ThreadPoolExecutor]()
    workingDirsSnapshot().asScala.foreach {
      dir => cleaners.put(dir,
        ThreadUtils.newDaemonCachedThreadPool(s"Disk-cleaner-${dir.getAbsoluteFile}", 1))
    }
    cleaners
  }

  val (deviceInfos, mountInfos, workingDirMountInfos) =
    DeviceInfo.getDeviceAndMountInfos(workingDirsSnapshot())
  private val deviceMonitor = DeviceMonitor.createDeviceMonitor(conf, this, deviceInfos, mountInfos)

  private val diskFlushers: ConcurrentHashMap[File, DiskFlusher] = {
    val flushers = new ConcurrentHashMap[File, DiskFlusher]()
    workingDirsSnapshot().asScala.foreach(file => {
      val workingDirPath = file.getAbsolutePath
      val flusher = new DiskFlusher(
        file,
        workerSource,
        deviceMonitor,
        workingDirMetas.get(workingDirPath).get._2,
        workingDirMountInfos.get(workingDirPath).mountPoint,
        workingDirMetas.get(workingDirPath).get._3
      )
      flushers.put(file, flusher)
    })
    flushers
  }

  private val actionService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
    .setNameFormat("StorageManager-action-thread").build)

  deviceMonitor.startCheck()

  def workingDirsSnapshot(mountPoint: String): util.ArrayList[File] = {
    if (mountPoint != StorageHint.UNKNOWN_DISK) {
      new util.ArrayList[File](mountInfos.get(mountPoint)
        .dirInfos.filter(workingDirs.contains(_)).toList.asJava)
    } else {
      logDebug("mount point is invalid")
      workingDirsSnapshot()
    }
  }

  override def notifyError(deviceName: String, dirs: ListBuffer[File],
    deviceErrorType: DeviceErrorType): Unit = this.synchronized {
    val availableDisks = numDisks()
    // add to isolatedWorkingDirs and decrease current slots and report
    dirs.foreach(dir => {
      workingDirs.synchronized {
        workingDirs.remove(dir)
      }

      val operator = dirOperators.remove(dir)
      if (operator != null) {
        operator.shutdown()
      }
      diskFlushers.remove(dir)

      isolatedWorkingDirs.put(dir, deviceErrorType)
    })
  }

  override def notifyHealthy(dirs: ListBuffer[File]): Unit = this.synchronized {
    dirs.foreach(dir => {
      isolatedWorkingDirs.remove(dir)
      if (!diskFlushers.containsKey(dir)) {
        diskFlushers.put(dir, new DiskFlusher(
          dir,
          workerSource,
          deviceMonitor,
          workingDirMetas.get(dir.getAbsolutePath).get._2,
          mountInfos.get(dir.getAbsolutePath).mountPoint,
          workingDirMetas.get(dir.getAbsolutePath).get._3
        ))
      }
      if (!dirOperators.containsKey(dir)) {
        dirOperators.put(dir,
          ThreadUtils.newDaemonCachedThreadPool(s"Disk-cleaner-${dir.getAbsoluteFile}", 1))
      }
      workingDirs.synchronized{
        if (!workingDirs.contains(dir)) {
          workingDirs.add(dir)
        }
      }
    })
  }

  def isolateDirs(dirs: ListBuffer[File], errorType: DeviceErrorType): Unit = this.synchronized {
    dirs.foreach(dir => {workingDirs.synchronized {
        workingDirs.remove(dir)
      }
      isolatedWorkingDirs.put(dir, errorType)
    })
  }

  override def notifyHighDiskUsage(dirs: ListBuffer[File]): Unit = {
    isolateDirs(dirs, DeviceErrorType.InsufficientDiskSpace)
  }

  override def notifySlowFlush(dirs: ListBuffer[File]): Unit = {
    isolateDirs(dirs, DeviceErrorType.FlushTimeout)
  }

  private val fetchChunkSize = RssConf.workerFetchChunkSize(conf)

  private val counter = new AtomicInteger()
  private val counterOperator = new IntUnaryOperator() {
    override def applyAsInt(operand: Int): Int = {
      val dirs = workingDirsSnapshot()
      if (dirs.size() > 0) {
        (operand + 1) % dirs.size()
      } else 0
    }
  }

  // shuffleKey -> (fileName -> writer)
  private val writers =
    new ConcurrentHashMap[String, ConcurrentHashMap[String, FileWriter]]()

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
    splitMode: PartitionSplitMode,
    partitionType: PartitionType): FileWriter = {
    if (!hasAvailableWorkingDirs()) {
      throw new IOException("No available working dirs!")
    }

    val partitionId = location.getId
    val epoch = location.getEpoch
    val mode = location.getMode
    val fileName = s"$partitionId-$epoch-${mode.mode()}"

    var retryCount = 0
    var exception: IOException = null
    val suggestedMountPoint = location.getStorageHint.getMountPoint
    while (retryCount < RssConf.createFileWriterRetryCount(conf)) {
      val dirs = workingDirsSnapshot(suggestedMountPoint)
      val index = getNextIndex()
      val dir = dirs.get(index % dirs.size())
      val shuffleDir = new File(dir, s"$appId/$shuffleId")
      val file = new File(shuffleDir, fileName)

      try {
        shuffleDir.mkdirs()
        val createFileSuccess = file.createNewFile()
        if (!createFileSuccess) {
          throw new RssException("create app shuffle data dir or file failed")
        }
        val fileWriter = new FileWriter(
          file,
          diskFlushers.get(dir),
          dir,
          fetchChunkSize,
          writerFlushBufferSize,
          workerSource,
          conf,
          deviceMonitor,
          splitThreshold,
          splitMode,
          partitionType)
        deviceMonitor.registerFileWriter(fileWriter)
        val shuffleKey = Utils.makeShuffleKey(appId, shuffleId)
        val list = workingDirWriters.computeIfAbsent(dir, workingDirWriterListFunc)
        list.synchronized {list.add(fileWriter)}
        val shuffleMap = writers.computeIfAbsent(shuffleKey, newMapFunc)
        shuffleMap.put(fileName, fileWriter)
        location.getStorageHint.setMountPoint(
          workingDirMountInfos.get(dir.getAbsolutePath).mountPoint)
        logDebug(s"location $location set disk hint to ${location.getStorageHint} ")
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

  def shuffleKeySet(): util.Set[String] = writers.keySet()

  def cleanupExpiredShuffleKey(expiredShuffleKeys: util.HashSet[String]): Unit = {
    val workingDirs = workingDirsSnapshot()
    workingDirs.addAll(isolatedWorkingDirs.asScala.filterNot(entry => {
      DeviceErrorType.criticalError(entry._2)
    }).keySet.asJava)

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

  private val localStorageScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("local-storage-scheduler")

  localStorageScheduler.scheduleAtFixedRate(new Runnable {
    override def run(): Unit = {
      try {
        // Clean up empty dirs, since the appDir do not delete during expired shuffle key cleanup
        cleanupExpiredAppDirs(expireDuration =
          System.currentTimeMillis() - emptyDirExpireDurationMs)

        // Clean up non-empty dirs which has not been modified
        // in the past {{noneEmptyExpireDurationsMs}}, since
        // non empty dirs may exist after cluster restart.
        cleanupExpiredAppDirs(deleteRecursively = true,
          System.currentTimeMillis() - noneEmptyDirExpireDurationMs)
      } catch {
        case exception: Exception =>
          logWarning(s"Cleanup expired shuffle data exception: ${exception.getMessage}")
      }
    }
  }, 30, 30, TimeUnit.MINUTES)

  val essSlowFlushInterval: Long = RssConf.slowFlushIntervalMs(conf)
  localStorageScheduler.scheduleAtFixedRate(new Runnable {
    override def run(): Unit = {
      val currentTime = System.nanoTime()
      diskFlushers.values().asScala.foreach(flusher => {
        logDebug(flusher.bufferQueueInfo())
        val lastFlushTime = flusher.getLastFlushTime
        if (lastFlushTime != -1 &&
          currentTime - lastFlushTime  > essSlowFlushInterval * 1000 * 1000) {
          flusher.reportError(flusher.workingDir, new IOException("Slow Flusher!"),
            DeviceErrorType.FlushTimeout)
        }
      })
    }
  }, essSlowFlushInterval, essSlowFlushInterval, TimeUnit.MILLISECONDS)

  private def cleanupExpiredAppDirs(
    deleteRecursively: Boolean = false, expireDuration: Long): Unit = {
    val workingDirs = workingDirsSnapshot()
    workingDirs.addAll(isolatedWorkingDirs.asScala.filterNot(entry => {
      DeviceErrorType.criticalError(entry._2)
    }).keySet.asJava)

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
      dirOperators.asScala.foreach(entry => {
        entry._2.shutdownNow()
      })
    }
    localStorageScheduler.shutdownNow()
    if (null != deviceMonitor) {
      deviceMonitor.close()
    }
  }

  private def flushFileWriters(): Unit = {
    writers.entrySet().asScala.foreach(u =>
      u.getValue.asScala
        .foreach(f => f._2.flushOnMemoryPressure()))
  }

  override def onPause(moduleName: String): Unit = {
  }

  override def onResume(moduleName: String): Unit = {
  }

  override def onTrim(): Unit = {
    actionService.submit(new Runnable {
      override def run(): Unit = {
        flushFileWriters()
      }
    })
  }

  def diskSnapshot: util.Map[String, DiskInfo] = {
    val snapshot = new util.HashMap[String, DiskInfo]()
    snapshot.putAll(
      mountInfos.asScala.map { case (mountPoint, mountInfo) =>
        val mountPointRelatedDirs = mountInfo.dirInfos
        val totalUsage = mountPointRelatedDirs.map { dir =>
          val writers = workingDirWriters.get(dir)
          if (writers != null && writers.size() > 0) {
            writers.asScala.map(_.getFileLength).sum
          } else {
            0
          }
        }.sum
        val totalConfiguredCapacity = mountPointRelatedDirs
          .map(file => workingDirMetas.get(file.getAbsolutePath).get._1).sum
        val fileSystemReportedUsableSpace = Files.getFileStore(
          Paths.get(mountInfo.mountPointFile.getPath)).getUsableSpace
        // if a mount point is not a valid mount point, getUsableSpace will return 0.
        val workingDirUsableSpace = Math.min(totalConfiguredCapacity - totalUsage,
          fileSystemReportedUsableSpace)
        val flushTimeSum = mountInfo.dirInfos.map(dir => diskFlushers.get(dir).averageFlushTime())
        logDebug(s"flush time list $flushTimeSum")
        val flushTimeCount = flushTimeSum.size
        val flushTimeAverage = if (flushTimeCount > 0) {
          flushTimeSum.sum / flushTimeCount
        } else {
          // if a disk average flush time is zero means that this disk is idle.
          0
        }
        val usedSlots = mountInfo.dirInfos.map(dir => diskFlushers.get(dir).getUsedSlots()).sum
        mountPoint -> new DiskInfo(mountPoint, workingDirUsableSpace, flushTimeAverage, usedSlots)
      }.toMap.asJava
    )
    if (log.isDebugEnabled()) {
      logDebug(s"Worker disk snapshot at ${System.currentTimeMillis()} snapshot: $snapshot")
    }
    snapshot
  }
}
