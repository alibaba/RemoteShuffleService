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

import java.io.{BufferedReader, File, FileInputStream, InputStreamReader, IOException}
import java.nio.charset.Charset
import java.util
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.io.Source

import io.netty.util.internal.ConcurrentSet
import org.apache.commons.io.FileUtils
import org.slf4j.LoggerFactory

import com.aliyun.emr.rss.common.RssConf
import com.aliyun.emr.rss.common.RssConf.diskCheckIntervalMs
import com.aliyun.emr.rss.common.util.ThreadUtils
import com.aliyun.emr.rss.common.util.Utils._
import com.aliyun.emr.rss.service.deploy.worker.DeviceErrorType._

trait DeviceMonitor {
  def startCheck() {}

  def registerFileWriter(fileWriter: FileWriter): Unit = {}

  def unregisterFileWriter(fileWriter: FileWriter): Unit = {}

  def registerDiskFlusher(diskFlusher: DiskFlusher): Unit = {}

  def unregisterDiskFlusher(diskFlusher: DiskFlusher): Unit = {}

  def reportDeviceError(
    workingDir: File,
    e: IOException,
    deviceErrorType: DeviceErrorType): Unit = {}

  def close() {}
}

object EmptyDeviceMonitor extends DeviceMonitor

class LocalDeviceMonitor(
    rssConf: RssConf,
    observer: DeviceObserver,
    dirs: util.ArrayList[File]) extends DeviceMonitor {

  val logger = LoggerFactory.getLogger(classOf[LocalDeviceMonitor])

  class ObservedDevice(val deviceInfo: DeviceInfo) {
    var mounts: ListBuffer[MountInfo] = deviceInfo.mounts
    val observers: ConcurrentSet[DeviceObserver] = new ConcurrentSet[DeviceObserver]()

    val sysBlockDir = RssConf.sysBlockDir(rssConf)
    val statFile = new File(s"$sysBlockDir/${deviceInfo.name}/stat")
    val inFlightFile = new File(s"$sysBlockDir/${deviceInfo.name}/inflight")

    var lastReadComplete: Long = -1
    var lastWriteComplete: Long = -1
    var lastReadInFlight: Long = -1
    var lastWriteInFlight: Long = -1

    def addMountInfo(mountInfo: MountInfo): Unit = {
      mounts.append(mountInfo)
    }

    def addObserver(observer: DeviceObserver): Unit = {
      observers.add(observer)
    }

    def removeObserver(observer: DeviceObserver): Unit = {
      observers.remove(observer)
    }

    def notifyObserversOnError(
        dirs: ListBuffer[File],
        deviceErrorType: DeviceErrorType): Unit = this.synchronized {
      // observer.notifyDeviceError might remove itself from observers,
      // so we need to use tmpObservers
      val tmpObservers = new util.HashSet[DeviceObserver](observers)
      tmpObservers.asScala.foreach { ob =>
        deviceErrorType match {
          case FlushTimeout =>
            ob.notifySlowFlush(dirs)
          case IoHang | ReadOrWriteFailure =>
            ob.notifyError(deviceInfo.name, dirs, deviceErrorType)
          case _ => // do nothing
        }
      }
    }

    def notifyObserversOnHealthy(dirs: ListBuffer[File]): Unit = this.synchronized {
      val tmpObservers = new util.HashSet[DeviceObserver](observers)
      tmpObservers.asScala.foreach { ob =>
        ob.notifyHealthy(dirs)
      }
    }

    def notifyObserversOnHighDiskUsage(dirs: ListBuffer[File]): Unit = this.synchronized {
      val tmpObservers = new util.HashSet[DeviceObserver](observers)
      tmpObservers.asScala.foreach { ob =>
        ob.notifyHighDiskUsage(dirs)
      }
    }

    /**
     * @return true if the device is hang
     */
    def checkIoHang(): Boolean = {
      var statsSource: Source = null
      var inFlightSource: Source = null

      try {
        statsSource = Source.fromFile(statFile)
        inFlightSource = Source.fromFile(inFlightFile)
        val stats = statsSource.getLines().next().trim.split("[ \t]+", -1)
        val inFlight = inFlightSource.getLines().next().trim.split("[ \t]+", -1)
        val readComplete = stats(0).toLong
        val writeComplete = stats(4).toLong
        val readInFlight = inFlight(0).toLong
        val writeInFlight = inFlight(1).toLong

        if (lastReadComplete == -1) {
          lastReadComplete = readComplete
          lastWriteComplete = writeComplete
          lastReadInFlight = readInFlight
          lastWriteInFlight = writeInFlight
          false
        } else {
          val isReadHang = lastReadComplete == readComplete &&
            readInFlight >= lastReadInFlight && lastReadInFlight > 0
          val isWriteHang = lastWriteComplete == writeComplete &&
            writeInFlight >= lastWriteInFlight && lastWriteInFlight > 0

          lastReadComplete = readComplete
          lastWriteComplete = writeComplete
          lastReadInFlight = readInFlight
          lastWriteInFlight = writeInFlight

          if (isReadHang || isWriteHang) {
            logger.info(s"Result of DeviceInfo.checkIoHang, DeviceName: ${deviceInfo.name}" +
              s"($readComplete,$writeComplete,$readInFlight,$writeInFlight)\t" +
              s"($lastReadComplete,$lastWriteComplete,$lastReadInFlight,$lastWriteInFlight)\t" +
              s"Observer cnt: ${observers.size()}"
            )
            logger.error(s"IO Hang! ReadHang: $isReadHang, WriteHang: $isWriteHang")
          }

          isReadHang || isWriteHang
        }
      } finally {
        statsSource.close()
        inFlightSource.close()
      }
    }

    override def toString: String = {
      s"DeviceName: ${deviceInfo.name}\tMount Information: ${mounts.mkString("\n")}"
    }
  }

  // (deviceName -> ObservedDevice)
  var observedDevices: util.HashMap[DeviceInfo, ObservedDevice] = _
  // (mount filesystem -> MountInfo)
  var mounts: util.HashMap[String, MountInfo] = _

  val diskCheckInterval = diskCheckIntervalMs(rssConf)
  private val diskChecker =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("worker-disk-checker")

  def init(): Unit = {
    val (devices, mounts) = DeviceInfo.getDeviceAndMountInformation(dirs)
    this.mounts = mounts
    this.observedDevices = new util.HashMap[DeviceInfo, ObservedDevice]()
    devices.asScala.foreach { entry =>
      val observedDevice = new ObservedDevice(entry._2)
      observedDevice.addObserver(observer)
      observedDevices.put(entry._2, observedDevice)
    }
  }

  override def startCheck(): Unit = {
    diskChecker.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = {
        logger.debug("Device check start")
        try {
          observedDevices.values().asScala.foreach { device =>
            val checkDirs = device.mounts.flatMap(mount => mount.mountedDirs)

            if (device.checkIoHang()) {
              logger.error(s"Encounter disk io hang error!" +
                s"${device.deviceInfo.name}, notify observers")
              device.notifyObserversOnError(checkDirs, IoHang)
            } else {
              device.mounts.foreach { entry =>
                if (DeviceMonitor.checkDiskUsage(rssConf, entry.mountPoint)) {
                  logger.error(s"${entry.mountPoint} high_disk_usage error, notify observers")
                  device.notifyObserversOnHighDiskUsage(entry.mountedDirs)
                } else if (DeviceMonitor.checkDiskReadAndWrite(rssConf, entry.mountedDirs)) {
                  logger.error(s"${entry.mountPoint} read-write error, notify observers")
                  // We think that if one dir in device has read-write problem, if possible all
                  // dirs in this device have the problem
                  device.notifyObserversOnError(entry.mountedDirs, ReadOrWriteFailure)
                } else {
                  device.notifyObserversOnHealthy(entry.mountedDirs)
                }
              }
            }
          }
        } catch {
          case t: Throwable =>
            logger.error("Device check failed.", t)
        }
      }
    }, diskCheckInterval, diskCheckInterval, TimeUnit.MILLISECONDS)
  }

  private def getObservedDevice(path: String): ObservedDevice = {
    val mountPoint = DeviceInfo.getMountPoint(path, mounts)
    observedDevices.get(mounts.get(mountPoint).deviceInfo)
  }

  override def registerFileWriter(fileWriter: FileWriter): Unit = {
    getObservedDevice(fileWriter.getFile.getAbsolutePath).addObserver(fileWriter)
  }

  override def unregisterFileWriter(fileWriter: FileWriter): Unit = {
    getObservedDevice(fileWriter.getFile.getAbsolutePath).removeObserver(fileWriter)
  }

  override def registerDiskFlusher(diskFlusher: DiskFlusher): Unit = {
    getObservedDevice(diskFlusher.workingDir.getAbsolutePath).addObserver(diskFlusher)
  }

  override def unregisterDiskFlusher(diskFlusher: DiskFlusher): Unit = {
    getObservedDevice(diskFlusher.workingDir.getAbsolutePath).removeObserver(diskFlusher)
  }

  override def reportDeviceError(
      workingDir: File,
      e: IOException,
      deviceErrorType: DeviceErrorType): Unit = {
    logger.error(s"Receive report exception, $workingDir, $e")
    val mountPoint = DeviceInfo.getMountPoint(workingDir.getAbsolutePath, mounts)
    if (mounts.containsKey(mountPoint)) {
      observedDevices.get(mounts.get(mountPoint).deviceInfo)
        .notifyObserversOnError(ListBuffer(workingDir), deviceErrorType)
    }
  }

  override def close(): Unit = {
    DeviceMonitor.deviceCheckThreadPool.shutdownNow()
  }
}

object DeviceMonitor {
  val logger = LoggerFactory.getLogger(classOf[DeviceMonitor])
  val deviceCheckThreadPool = ThreadUtils.newDaemonCachedThreadPool("device-check-thread", 5)

  def createDeviceMonitor(
      rssConf: RssConf,
      deviceObserver: DeviceObserver,
      dirs: util.ArrayList[File]): DeviceMonitor = {
    try {
      if (RssConf.deviceMonitorEnabled(rssConf)) {
        val monitor = new LocalDeviceMonitor(rssConf, deviceObserver, dirs)
        monitor.init()
        logger.info("Device monitor init success")
        monitor
      } else {
        EmptyDeviceMonitor
      }
    } catch {
      case t: Throwable =>
        logger.error("Device monitor init failed.", t)
        throw t
    }
  }

  /**
   * check if the disk is high usage
   * @param rssConf conf
   * @param diskRootPath disk root path
   * @return true if high disk usage
   */
  def checkDiskUsage(rssConf: RssConf, diskRootPath: String): Boolean = {
    tryWithTimeoutAndCallback {
      val usage = runCommand(s"df -B 1G $diskRootPath").trim.split("[ \t]+")
      val totalSpace = usage(usage.length - 1)
      val freeSpace = usage(usage.length - 3)
      val used_percent = usage(usage.length - 2)

      val status = freeSpace.toLong < RssConf.diskSpaceSafeWatermarkSizeInGb(rssConf)
      if (status) {
        logger.warn(s"$diskRootPath usage:{total:$totalSpace GB," +
          s" free:$freeSpace GB, used_percent:$used_percent}")
      }
      status
    }(true)(deviceCheckThreadPool, RssConf.workerStatusCheckTimeout(rssConf),
      s"Disk: $diskRootPath Usage Check Timeout")
  }

  /**
   * check if the data dir has read-write problem
   * @param rssConf conf
   * @param dataDirs shuffle data dirs in on mount disk
   * @return true if disk has read-write problem
   */
  def checkDiskReadAndWrite(rssConf: RssConf, dataDirs: ListBuffer[File]): Boolean = {
    if (null == dataDirs || dataDirs.isEmpty) {
      false
    } else {
      var diskHealthy = true
      for (i <- dataDirs.indices if diskHealthy) {
        val dir = dataDirs(i)
        diskHealthy = tryWithTimeoutAndCallback {
          try {
            dir.mkdirs()
            val file = new File(dir, s"_SUCCESS_${System.currentTimeMillis()}")
            if (!file.exists() && !file.createNewFile()) {
              false
            } else {
              FileUtils.write(file, "test", Charset.defaultCharset)
              var fileInputStream: FileInputStream = null
              var inputStreamReader: InputStreamReader = null
              var bufferReader: BufferedReader = null
              try {
                fileInputStream = FileUtils.openInputStream(file)
                inputStreamReader = new InputStreamReader(fileInputStream, Charset.defaultCharset())
                bufferReader = new BufferedReader(inputStreamReader)
                bufferReader.readLine()
              } finally {
                bufferReader.close()
                inputStreamReader.close()
                fileInputStream.close()
              }
              FileUtils.forceDelete(file)
              true
            }
          } catch {
            case t: Throwable =>
              logger.error(s"Disk $dir cannot read or write", t)
              false
          }
        }(false)(deviceCheckThreadPool, RssConf.workerStatusCheckTimeout(rssConf),
          s"Disk: $dir Read_Write Check Timeout")
      }

      !diskHealthy
    }
  }

  def EmptyMonitor(): DeviceMonitor = EmptyDeviceMonitor
}
