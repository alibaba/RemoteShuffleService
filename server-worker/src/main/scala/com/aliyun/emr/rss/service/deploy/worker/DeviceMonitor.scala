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
import java.util.{Set => jSet}
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.io.Source

import org.apache.commons.io.FileUtils
import org.slf4j.LoggerFactory

import com.aliyun.emr.rss.common.RssConf
import com.aliyun.emr.rss.common.RssConf.diskCheckIntervalMs
import com.aliyun.emr.rss.common.util.ThreadUtils
import com.aliyun.emr.rss.common.util.Utils._

trait DeviceMonitor {
  def startCheck() {}
  def registerFileWriter(fileWriter: FileWriter): Unit = {}
  def unregisterFileWriter(fileWriter: FileWriter): Unit = {}
  def registerDiskFlusher(diskFlusher: DiskFlusher): Unit = {}
  def unregisterDiskFlusher(diskFlusher: DiskFlusher): Unit = {}
  def reportDeviceError(workingDir: File, e: IOException,
    deviceErrorType: DeviceErrorType): Unit = {}
  def close() {}
}

object EmptyDeviceMonitor extends DeviceMonitor

class LocalDeviceMonitor(
  essConf: RssConf,
  observer: DeviceObserver,
  deviceInfos: util.HashMap[String, DeviceInfo],
  mountInfos: util.HashMap[String, MountInfo]) extends DeviceMonitor {
  val logger = LoggerFactory.getLogger(classOf[LocalDeviceMonitor])

  class ObservedDevice(val deviceInfo: DeviceInfo) {
    var mountInfos: ListBuffer[MountInfo] = deviceInfo.mountInfos
    val observers: jSet[DeviceObserver] = ConcurrentHashMap.newKeySet[DeviceObserver]()

    val sysBlockDir = RssConf.sysBlockDir(essConf)
    val statFile = new File(s"$sysBlockDir/${deviceInfo.name}/stat")
    val inFlightFile = new File(s"$sysBlockDir/${deviceInfo.name}/inflight")

    var lastReadComplete: Long = -1
    var lastWriteComplete: Long = -1
    var lastReadInflight: Long = -1
    var lastWriteInflight: Long = -1

    def addMountInfo(mountInfo: MountInfo): Unit = {
      mountInfos.append(mountInfo)
    }

    def addObserver(observer: DeviceObserver): Unit = {
      observers.add(observer)
    }

    def removeObserver(observer: DeviceObserver): Unit = {
      observers.remove(observer)
    }

    def notifyObserversOnError(dirs: ListBuffer[File],
      deviceErrorType: DeviceErrorType): Unit = this.synchronized {
      // observer.notifyDeviceError might remove itself from observers,
      // so we need to use tmpObservers
      val tmpObservers = new util.HashSet[DeviceObserver](observers)
      tmpObservers.asScala.foreach(ob => {
        if (deviceErrorType == DeviceErrorType.FlushTimeout) {
          ob.notifySlowFlush(dirs)
        } else if (DeviceErrorType.criticalError(deviceErrorType)) {
          ob.notifyError(deviceInfo.name, dirs, deviceErrorType)
        }
      })
    }

    def notifyObserversOnHealthy(dirs: ListBuffer[File]): Unit = this.synchronized {
      val tmpObservers = new util.HashSet[DeviceObserver](observers)
      tmpObservers.asScala.foreach(ob => {
        ob.notifyHealthy(dirs)
      })
    }

    def notifyObserversOnHighDiskUsage(dirs: ListBuffer[File]): Unit = this.synchronized {
      val tmpObservers = new util.HashSet[DeviceObserver](observers)
      tmpObservers.asScala.foreach(ob => {
        ob.notifyHighDiskUsage(dirs)
      })
    }

    /**
     * @return true if device is hang
     */
    def checkIoHang(): Boolean = {
      var statsSource: Source = null
      var infligtSource: Source = null

      try {
        statsSource = Source.fromFile(statFile)
        infligtSource = Source.fromFile(inFlightFile)
        val stats = statsSource.getLines().next().trim.split("[ \t]+", -1)
        val inflight = infligtSource.getLines().next().trim.split("[ \t]+", -1)
        val readComplete = stats(0).toLong
        val writeComplete = stats(4).toLong
        val readInflight = inflight(0).toLong
        val writeInflight = inflight(1).toLong

        if (lastReadComplete == -1) {
          lastReadComplete = readComplete
          lastWriteComplete = writeComplete
          lastReadInflight = readInflight
          lastWriteInflight = writeInflight
          false
        } else {
          val isReadHang = lastReadComplete == readComplete &&
            readInflight >= lastReadInflight && lastReadInflight > 0
          val isWriteHang = lastWriteComplete == writeComplete &&
            writeInflight >= lastWriteInflight && lastWriteInflight > 0

          lastReadComplete = readComplete
          lastWriteComplete = writeComplete
          lastReadInflight = readInflight
          lastWriteInflight = writeInflight

          if (isReadHang || isWriteHang) {
            logger.info(s"Result of DeviceInfo.checkIoHang, DeviceName: ${deviceInfo.name}" +
              s"($readComplete,$writeComplete,$readInflight,$writeInflight)\t" +
              s"($lastReadComplete,$lastWriteComplete,$lastReadInflight,$lastWriteInflight)\t" +
              s"Observer cnt: ${observers.size()}"
            )
            logger.error(s"IO Hang! ReadHang: $isReadHang, WriteHang: $isWriteHang")
          }

          isReadHang || isWriteHang
        }
      } finally {
        statsSource.close()
        infligtSource.close()
      }
    }

    override def toString: String = {
      s"DeviceName: ${deviceInfo.name}\tMount Infos: ${mountInfos.mkString("\n")}"
    }
  }

  // (deviceName -> ObservedDevice)
  var observedDevices: util.HashMap[DeviceInfo, ObservedDevice] = _

  val diskCheckInterval = diskCheckIntervalMs(essConf)
  private val diskChecker =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("worker-disk-checker")

  def init(): Unit = {
    this.observedDevices = new util.HashMap[DeviceInfo, ObservedDevice]()
    deviceInfos.asScala.filter(_._2.virtual).foreach { case (deviceName, _) =>
      logger.warn(s"device monitor may not worker properly " +
        s"because virtual device $deviceName exists.")
    }
    deviceInfos.asScala.foreach(entry => {
      val observedDevice = new ObservedDevice(entry._2)
      observedDevice.addObserver(observer)
      observedDevices.put(entry._2, observedDevice)
    })
  }

  override def startCheck(): Unit = {
    diskChecker.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = {
        logger.debug("Device check start")
        try {
          observedDevices.values().asScala.foreach(device => {
            val checkDirs = device.mountInfos.flatMap(mount => mount.dirInfos)

            if (device.checkIoHang()) {
              logger.error(s"Encounter disk io hang error!" +
                s"${device.deviceInfo.name}, notify observers")
              device.notifyObserversOnError(checkDirs, DeviceErrorType.IoHang)
            } else {
              device.mountInfos.foreach(entry => {
                if (DeviceMonitor.checkDiskUsage(essConf, entry.mountPoint)) {
                  logger.error(s"${entry.mountPoint} high_disk_usage error, notify observers")
                  device.notifyObserversOnHighDiskUsage(entry.dirInfos)
                } else if (DeviceMonitor.checkDiskReadAndWrite(essConf, entry.dirInfos)) {
                  logger.error(s"${entry.mountPoint} read-write error, notify observers")
                  // We think that if one dir in device has read-write problem, if possible all
                  // dirs in this device have the problem
                  device.notifyObserversOnError(entry.dirInfos, DeviceErrorType.ReadOrWriteFailure)
                } else {
                  device.notifyObserversOnHealthy(entry.dirInfos)
                }
              })
            }
          })
        } catch {
          case t: Throwable =>
            logger.error("Device check failed.", t)
        }
      }
    }, diskCheckInterval, diskCheckInterval, TimeUnit.MILLISECONDS)
  }

  override def registerFileWriter(fileWriter: FileWriter): Unit = {
    val mountPoint = DeviceInfo.getMountPoint(fileWriter.getFile.getAbsolutePath, mountInfos)
    observedDevices.get(mountInfos.get(mountPoint).deviceInfo).addObserver(fileWriter)
  }

  override def unregisterFileWriter(fileWriter: FileWriter): Unit = {
    val mountPoint = DeviceInfo.getMountPoint(fileWriter.getFile.getAbsolutePath, mountInfos)
    observedDevices.get(mountInfos.get(mountPoint).deviceInfo).removeObserver(fileWriter)
  }

  override def registerDiskFlusher(diskFlusher: DiskFlusher): Unit = {
    val mountPoint = DeviceInfo.getMountPoint(diskFlusher.workingDir.getAbsolutePath, mountInfos)
    observedDevices.get(mountInfos.get(mountPoint).deviceInfo).addObserver(diskFlusher)
  }

  override def unregisterDiskFlusher(diskFlusher: DiskFlusher): Unit = {
    val mountPoint = DeviceInfo.getMountPoint(diskFlusher.workingDir.getAbsolutePath, mountInfos)
    observedDevices.get(mountInfos.get(mountPoint).deviceInfo).removeObserver(diskFlusher)
  }

  override def reportDeviceError(workingDir: File, e: IOException,
    deviceErrorType: DeviceErrorType): Unit = {
    logger.error(s"Receive report exception, $workingDir, $e")
    val mountPoint = DeviceInfo.getMountPoint(workingDir.getAbsolutePath, mountInfos)
    if (mountInfos.containsKey(mountPoint)) {
      observedDevices.get(mountInfos.get(mountPoint).deviceInfo)
        .notifyObserversOnError(ListBuffer(workingDir), deviceErrorType)
    }
  }

  override def close(): Unit = {
    if (null != DeviceMonitor.deviceCheckThreadPool) {
      DeviceMonitor.deviceCheckThreadPool.shutdownNow()
    }
  }
}

object DeviceMonitor {
  val logger = LoggerFactory.getLogger(classOf[DeviceMonitor])
  val deviceCheckThreadPool = ThreadUtils.newDaemonCachedThreadPool("device-check-thread", 5)

  def createDeviceMonitor(
    rssConf: RssConf,
    deviceObserver: DeviceObserver,
    deviceInfos: util.HashMap[String, DeviceInfo],
    mountInfos: util.HashMap[String, MountInfo]): DeviceMonitor = {
    try {
      if (RssConf.deviceMonitorEnabled(rssConf)) {
        val monitor = new LocalDeviceMonitor(rssConf, deviceObserver, deviceInfos, mountInfos)
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
  def checkDiskUsage(essConf: RssConf, diskRootPath: String): Boolean = {
    tryWithTimeoutAndCallback({
      val usage = runCommand(s"df -B 1G $diskRootPath").trim.split("[ \t]+")
      val totalSpace = usage(usage.length - 1)
      val freeSpace = usage(usage.length - 3)
      val used_percent = usage(usage.length - 2)

      val status = freeSpace.toLong < RssConf.diskSpaceSafeWatermarkSizeInGb(essConf)
      if (status) {
        logger.warn(s"$diskRootPath usage:{total:$totalSpace GB," +
          s" free:$freeSpace GB, used_percent:$used_percent}")
      }
      status
    })(true)(deviceCheckThreadPool, RssConf.workerStatusCheckTimeout(essConf),
      s"Disk: $diskRootPath Usage Check Timeout")
  }

  /**
   * check if the data dir has read-write problem
   * @param rssConf conf
   * @param dataDirs shuffle data dirs in on mount disk
   * @return true if disk has read-write problem
   */
  def checkDiskReadAndWrite(essConf: RssConf, dataDirs: ListBuffer[File]): Boolean = {
    if (null == dataDirs || dataDirs.isEmpty) {
      return false
    }

    var diskHealthy = true
    for (i <- dataDirs.indices if diskHealthy) {
      val dir = dataDirs(i)
      diskHealthy = tryWithTimeoutAndCallback({
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
      })(false)(deviceCheckThreadPool, RssConf.workerStatusCheckTimeout(essConf),
        s"Disk: $dir Read_Write Check Timeout")
    }

    !diskHealthy
  }

  def EmptyMonitor(): DeviceMonitor = EmptyDeviceMonitor
}
