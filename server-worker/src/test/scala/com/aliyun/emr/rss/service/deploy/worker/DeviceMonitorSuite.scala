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
import java.util.{ArrayList => jArrayList}
import java.util.concurrent.atomic.AtomicBoolean

import org.junit.Assert.assertEquals
import org.mockito.ArgumentMatchers._
import org.mockito.MockitoSugar._
import org.scalatest.funsuite.AnyFunSuite

import com.aliyun.emr.rss.common.RssConf
import com.aliyun.emr.rss.common.util.Utils

class DeviceMonitorSuite extends AnyFunSuite {
  val dfCmd = "df -h"
  val dfOut =
    """
      |文件系统        容量  已用  可用 已用% 挂载点
      |devtmpfs         32G     0   32G    0% /dev
      |tmpfs            32G  108K   32G    1% /dev/shm
      |tmpfs            32G  672K   32G    1% /run
      |tmpfs            32G     0   32G    0% /sys/fs/cgroup
      |/dev/vda1       118G   43G   71G   38% /
      |tmpfs           6.3G     0  6.3G    0% /run/user/0
      |/dev/vda        1.8T   91G  1.7T    6% /mnt/disk1
      |/dev/vdb        1.8T   91G  1.7T    6% /mnt/disk2
      |tmpfs           6.3G     0  6.3G    0% /run/user/1001
      |""".stripMargin

  val lsCmd = "ls /sys/block/"
  val lsOut = "loop0  loop1  loop2  loop3  loop4  loop5  loop6  loop7  vda  vdb"

  val dirs = new jArrayList[File]()
  val workingDir1 = new File("/mnt/disk1/data1")
  val workingDir2 = new File("/mnt/disk1/data2")
  val workingDir3 = new File("/mnt/disk2/data3")
  val workingDir4 = new File("/mnt/disk2/data4")
  dirs.add(workingDir1)
  dirs.add(workingDir2)
  dirs.add(workingDir3)
  dirs.add(workingDir4)

  val rssConf = new RssConf()
  rssConf.set("rss.disk.check.interval", "3600s")

  val localStorageManager = mock[DeviceObserver]
  var (deviceInfos, mountInfos, workingDirMountInfos): (
    java.util.HashMap[String, DeviceInfo],
      java.util.HashMap[String, MountInfo],
      java.util.HashMap[String, MountInfo]
    ) = (null, null, null)

  withObjectMocked[com.aliyun.emr.rss.common.util.Utils.type] {
    when(Utils.runCommand(dfCmd)) thenReturn dfOut
    when(Utils.runCommand(lsCmd)) thenReturn lsOut
    val (tdeviceInfos, tmountInfos, tworkingDirMountInfos) = DeviceInfo.getDeviceAndMountInfos(dirs)
    deviceInfos = tdeviceInfos
    mountInfos = tmountInfos
    workingDirMountInfos = tworkingDirMountInfos
  }
  val deviceMonitor =
    new LocalDeviceMonitor(rssConf, localStorageManager, deviceInfos, mountInfos)

  val vdaDeviceInfo = new DeviceInfo("vda")
  val vdbDeviceInfo = new DeviceInfo("vdb")

  test("init") {
    withObjectMocked[com.aliyun.emr.rss.common.util.Utils.type] {
      when(Utils.runCommand(dfCmd)) thenReturn dfOut
      when(Utils.runCommand(lsCmd)) thenReturn lsOut

      deviceMonitor.init()

      assertEquals(2, deviceMonitor.observedDevices.size())

      assert(deviceMonitor.observedDevices.containsKey(vdaDeviceInfo))
      assert(deviceMonitor.observedDevices.containsKey(vdbDeviceInfo))

      assertEquals(deviceMonitor.observedDevices.get(vdaDeviceInfo).mountInfos.size, 1)
      assertEquals(deviceMonitor.observedDevices.get(vdbDeviceInfo).mountInfos.size, 1)

      assertEquals(
        deviceMonitor.observedDevices.get(vdaDeviceInfo).mountInfos(0).mountPoint,
        "/mnt/disk1"
      )
      assertEquals(
        deviceMonitor.observedDevices.get(vdbDeviceInfo).mountInfos(0).mountPoint,
        "/mnt/disk2"
      )

      assertEquals(
        deviceMonitor.observedDevices.get(vdaDeviceInfo).mountInfos(0).dirInfos(0),
        new File("/mnt/disk1/data1")
      )
      assertEquals(
        deviceMonitor.observedDevices.get(vdaDeviceInfo).mountInfos(0).dirInfos(1),
        new File("/mnt/disk1/data2")
      )
      assertEquals(
        deviceMonitor.observedDevices.get(vdbDeviceInfo).mountInfos(0).dirInfos(0),
        new File("/mnt/disk2/data3")
      )
      assertEquals(
        deviceMonitor.observedDevices.get(vdbDeviceInfo).mountInfos(0).dirInfos(1),
        new File("/mnt/disk2/data4")
      )

      assertEquals(deviceMonitor.observedDevices.get(vdaDeviceInfo).observers.size(), 1)
      assertEquals(deviceMonitor.observedDevices.get(vdbDeviceInfo).observers.size(), 1)
    }
  }

  test("register/unregister/notify/report") {
    withObjectMocked[com.aliyun.emr.rss.common.util.Utils.type] {
      when(Utils.runCommand(dfCmd)) thenReturn dfOut
      when(Utils.runCommand(lsCmd)) thenReturn lsOut

      deviceMonitor.init()

      val fw1 = mock[FileWriter]
      val fw2 = mock[FileWriter]
      val fw3 = mock[FileWriter]
      val fw4 = mock[FileWriter]

      val f1 = new File("/mnt/disk1/data1/f1")
      val f2 = new File("/mnt/disk1/data2/f2")
      val f3 = new File("/mnt/disk2/data3/f3")
      val f4 = new File("/mnt/disk2/data4/f4")
      when(fw1.getFile).thenReturn(f1)
      when(fw2.getFile).thenReturn(f2)
      when(fw3.getFile).thenReturn(f3)
      when(fw4.getFile).thenReturn(f4)

      deviceMonitor.registerFileWriter(fw1)
      deviceMonitor.registerFileWriter(fw2)
      deviceMonitor.registerFileWriter(fw3)
      deviceMonitor.registerFileWriter(fw4)

      assertEquals(deviceMonitor.observedDevices.get(vdaDeviceInfo).observers.size(), 3)
      assertEquals(deviceMonitor.observedDevices.get(vdbDeviceInfo).observers.size(), 3)
      assert(
        deviceMonitor.observedDevices.get(vdaDeviceInfo).observers.contains(localStorageManager)
      )
      assert(deviceMonitor.observedDevices.get(vdaDeviceInfo).observers.contains(fw1))
      assert(deviceMonitor.observedDevices.get(vdaDeviceInfo).observers.contains(fw2))
      assert(
        deviceMonitor.observedDevices.get(vdbDeviceInfo).observers.contains(localStorageManager)
      )
      assert(deviceMonitor.observedDevices.get(vdbDeviceInfo).observers.contains(fw3))
      assert(deviceMonitor.observedDevices.get(vdbDeviceInfo).observers.contains(fw4))

      deviceMonitor.unregisterFileWriter(fw1)
      deviceMonitor.unregisterFileWriter(fw3)
      assertEquals(deviceMonitor.observedDevices.get(vdaDeviceInfo).observers.size(), 2)
      assertEquals(deviceMonitor.observedDevices.get(vdbDeviceInfo).observers.size(), 2)
      assert(
        deviceMonitor.observedDevices.get(vdaDeviceInfo).observers.contains(localStorageManager)
      )
      assert(deviceMonitor.observedDevices.get(vdaDeviceInfo).observers.contains(fw2))
      assert(
        deviceMonitor.observedDevices.get(vdbDeviceInfo).observers.contains(localStorageManager)
      )
      assert(deviceMonitor.observedDevices.get(vdbDeviceInfo).observers.contains(fw4))

      val df1 = mock[DiskFlusher]
      val df2 = mock[DiskFlusher]
      val df3 = mock[DiskFlusher]
      val df4 = mock[DiskFlusher]

      when(df1.stopFlag).thenReturn(new AtomicBoolean(false))
      when(df2.stopFlag).thenReturn(new AtomicBoolean(false))
      when(df3.stopFlag).thenReturn(new AtomicBoolean(false))
      when(df4.stopFlag).thenReturn(new AtomicBoolean(false))

      when(df1.workingDir).thenReturn(workingDir1)
      when(df2.workingDir).thenReturn(workingDir2)
      when(df3.workingDir).thenReturn(workingDir3)
      when(df4.workingDir).thenReturn(workingDir4)

      deviceMonitor.registerDiskFlusher(df1)
      deviceMonitor.registerDiskFlusher(df2)
      deviceMonitor.registerDiskFlusher(df3)
      deviceMonitor.registerDiskFlusher(df4)
      assertEquals(deviceMonitor.observedDevices.get(vdaDeviceInfo).observers.size(), 4)
      assertEquals(deviceMonitor.observedDevices.get(vdbDeviceInfo).observers.size(), 4)
      assert(
        deviceMonitor.observedDevices.get(vdaDeviceInfo).observers.contains(localStorageManager)
      )
      assert(deviceMonitor.observedDevices.get(vdaDeviceInfo).observers.contains(df1))
      assert(deviceMonitor.observedDevices.get(vdaDeviceInfo).observers.contains(df2))
      assert(
        deviceMonitor.observedDevices.get(vdbDeviceInfo).observers.contains(localStorageManager)
      )
      assert(deviceMonitor.observedDevices.get(vdbDeviceInfo).observers.contains(df3))
      assert(deviceMonitor.observedDevices.get(vdbDeviceInfo).observers.contains(df4))

      deviceMonitor.unregisterDiskFlusher(df1)
      deviceMonitor.unregisterDiskFlusher(df3)
      assertEquals(deviceMonitor.observedDevices.get(vdaDeviceInfo).observers.size(), 3)
      assertEquals(deviceMonitor.observedDevices.get(vdbDeviceInfo).observers.size(), 3)
      assert(
        deviceMonitor.observedDevices.get(vdaDeviceInfo).observers.contains(localStorageManager)
      )
      assert(deviceMonitor.observedDevices.get(vdaDeviceInfo).observers.contains(df2))
      assert(
        deviceMonitor.observedDevices.get(vdbDeviceInfo).observers.contains(localStorageManager)
      )
      assert(deviceMonitor.observedDevices.get(vdbDeviceInfo).observers.contains(df4))

      when(fw2.notifyError("vda", null, DeviceErrorType.IoHang))
        .thenAnswer((a: String, b: List[File]) => {
          deviceMonitor.unregisterFileWriter(fw2)
        })
      when(fw4.notifyError("vdb", null, DeviceErrorType.IoHang))
        .thenAnswer((a: String, b: List[File]) => {
          deviceMonitor.unregisterFileWriter(fw4)
        })
      when(df2.notifyError("vda", null, DeviceErrorType.IoHang))
        .thenAnswer((a: String, b: List[File]) => {
          df2.stopFlag.set(true)
        })
      when(df4.notifyError("vdb", null, DeviceErrorType.IoHang))
        .thenAnswer((a: String, b: List[File]) => {
          df4.stopFlag.set(true)
        })

      deviceMonitor.observedDevices
        .get(vdaDeviceInfo)
        .notifyObserversOnError(null, DeviceErrorType.IoHang)
      deviceMonitor.observedDevices
        .get(vdbDeviceInfo)
        .notifyObserversOnError(null, DeviceErrorType.IoHang)
      assertEquals(deviceMonitor.observedDevices.get(vdaDeviceInfo).observers.size(), 2)
      assertEquals(deviceMonitor.observedDevices.get(vdbDeviceInfo).observers.size(), 2)
      assert(
        deviceMonitor.observedDevices.get(vdaDeviceInfo).observers.contains(localStorageManager)
      )
      assert(deviceMonitor.observedDevices.get(vdaDeviceInfo).observers.contains(df2))
      assert(
        deviceMonitor.observedDevices.get(vdbDeviceInfo).observers.contains(localStorageManager)
      )
      assert(deviceMonitor.observedDevices.get(vdbDeviceInfo).observers.contains(df4))

      deviceMonitor.registerFileWriter(fw1)
      deviceMonitor.registerFileWriter(fw2)
      deviceMonitor.registerFileWriter(fw3)
      deviceMonitor.registerFileWriter(fw4)
      assertEquals(deviceMonitor.observedDevices.get(vdaDeviceInfo).observers.size(), 4)
      assertEquals(deviceMonitor.observedDevices.get(vdbDeviceInfo).observers.size(), 4)
      when(fw1.reportError(workingDir1, null, DeviceErrorType.IoHang))
        .thenAnswer((workingDir: File, e: IOException) => {
          deviceMonitor.reportDeviceError(workingDir1, null, DeviceErrorType.IoHang)
        })
      val dirs = new jArrayList[File]()
      dirs.add(null)
      when(fw1.notifyError(any(), any(), any()))
        .thenAnswer((_: Any) => {
          deviceMonitor.unregisterFileWriter(fw1)
        })
      when(fw2.notifyError(any(), any(), any()))
        .thenAnswer((_: Any) => {
          deviceMonitor.unregisterFileWriter(fw2)
        })
      fw1.reportError(workingDir1, null, DeviceErrorType.IoHang)
      assertEquals(deviceMonitor.observedDevices.get(vdaDeviceInfo).observers.size(), 2)
      assert(
        deviceMonitor.observedDevices.get(vdaDeviceInfo).observers.contains(localStorageManager)
      )
      assert(deviceMonitor.observedDevices.get(vdaDeviceInfo).observers.contains(df2))

      when(df4.reportError(workingDir4, null, DeviceErrorType.IoHang))
        .thenAnswer((workingDir: File, e: IOException) => {
          deviceMonitor.reportDeviceError(workingDir4, null, DeviceErrorType.IoHang)
        })
      when(fw3.notifyError(any(), any(), any()))
        .thenAnswer((_: Any) => {
          deviceMonitor.unregisterFileWriter(fw3)
        })
      when(fw4.notifyError(any(), any(), any()))
        .thenAnswer((_: Any) => {
          deviceMonitor.unregisterFileWriter(fw4)
        })
      df4.reportError(workingDir4, null, DeviceErrorType.IoHang)
      assertEquals(deviceMonitor.observedDevices.get(vdbDeviceInfo).observers.size(), 2)
      assert(
        deviceMonitor.observedDevices.get(vdbDeviceInfo).observers.contains(localStorageManager)
      )
      assert(deviceMonitor.observedDevices.get(vdbDeviceInfo).observers.contains(df4))
    }
  }

  test("tryWithTimeoutAndCallback") {
    val fn = (i: Int) => {
      0 until 100 foreach (x => {
        // scalastyle:off println
        println(i + Thread.currentThread().getName)
        Thread.sleep(2000)
        // scalastyle:on println
      })
      true
    }
    0 until 3 foreach (i => {
      val result = Utils.tryWithTimeoutAndCallback({
        fn(i)
      })(false)(DeviceMonitor.deviceCheckThreadPool, 1)
      assert(!result)
    })
    DeviceMonitor.deviceCheckThreadPool.shutdownNow()
  }
}
