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

import java.io.File
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import org.slf4j.LoggerFactory

import com.aliyun.emr.rss.common.util.Utils.runCommand

class MountInfo(val mountPoint: String, val deviceInfo: DeviceInfo) extends Serializable {
  val mountedDirs: ListBuffer[File] = new ListBuffer[File]()

  def addDir(dir: File): Unit = {
    mountedDirs.append(dir)
  }

  override def toString: String = {
    s"\tMount Point: $mountPoint\tDirs: ${mountedDirs.mkString("[", "\t", "]")}"
  }

  override def hashCode(): Int = {
    mountPoint.hashCode
  }

  override def equals(obj: Any): Boolean = {
    obj.isInstanceOf[MountInfo] && mountPoint.equals(obj.asInstanceOf[MountInfo].mountPoint)
  }
}

class DeviceInfo(val name: String) extends Serializable {
  var mounts: ListBuffer[MountInfo] = new ListBuffer[MountInfo]()

  def addMountInfo(mountInfo: MountInfo): Unit = {
    mounts.append(mountInfo)
  }

  override def hashCode(): Int = {
    name.hashCode
  }

  override def equals(obj: Any): Boolean = {
    obj.isInstanceOf[DeviceInfo] && name.equals(obj.asInstanceOf[DeviceInfo].name)
  }

  override def toString: String = {
    s"Device Name: $name\tMount Information: ${mounts.mkString("[", "\n", "]")}"
  }
}

object DeviceInfo {
  val logger = LoggerFactory.getLogger(classOf[DeviceInfo])

  def getDeviceAndMountInformation(workingDirs: util.List[File]):
  (util.HashMap[String, DeviceInfo], util.HashMap[String, MountInfo]) = {
    val allDevices = new util.HashMap[String, DeviceInfo]()
    val allMounts = new util.HashMap[String, MountInfo]()

    // (/dev/vdb, /mnt/disk1)
    val fsMounts = runCommand("df -h").trim
      .split("[\n\r]")
      .tail
      .map { line =>
        val tokens = line.trim.split("[ \t]+")
        (tokens.head, tokens.last)
      }

    // (vda, vdb)
    val blocks = runCommand("ls /sys/block/").trim.split("[ \n\r\t]+")

    fsMounts.foreach { fsMount =>
      val baseName = fsMount._1.substring(fsMount._1.lastIndexOf('/') + 1)
      var index = -1
      var maxLength = -1
      blocks.zipWithIndex.foreach { block =>
        if (baseName.startsWith(block._1) && block._1.length > maxLength) {
          index = block._2
          maxLength = block._1.length
        }
      }

      val newDeviceInfoFunc = new util.function.Function[String, DeviceInfo]() {
        override def apply(s: String): DeviceInfo = {
          new DeviceInfo(s)
        }
      }

      if (index >= 0) {
        val deviceInfo = allDevices.computeIfAbsent(blocks(index), newDeviceInfoFunc)
        val mountInfo = new MountInfo(fsMount._2, deviceInfo)
        deviceInfo.addMountInfo(mountInfo)
        allMounts.putIfAbsent(fsMount._2, mountInfo)
      }
    }

    val devices = new util.HashMap[String, DeviceInfo]()
    val mounts = new util.HashMap[String, MountInfo]()

    workingDirs.asScala.foreach { dir =>
      val mount = getMountPoint(dir.getAbsolutePath, allMounts)
      val mountInfo = allMounts.get(mount)
      mountInfo.addDir(dir)
      mounts.putIfAbsent(mountInfo.mountPoint, mountInfo)
      devices.putIfAbsent(mountInfo.deviceInfo.name, mountInfo.deviceInfo)
    }

    devices.asScala.foreach { entry =>
      val mountInformation = entry._2.mounts.filter(_.mountedDirs.nonEmpty)
      entry._2.mounts = mountInformation
    }
    logger.info(s"Device Information:\n$devices")

    (devices, mounts)
  }

  def getMountPoint(
      absPath: String,
      mountInformation: util.HashMap[String, MountInfo]): String = {
    var currentMatchedMaxPathLength = -1
    var currentMountPoint = ""
    mountInformation.keySet.asScala.foreach { mount =>
      if (absPath.startsWith(mount) && mount.length > currentMatchedMaxPathLength) {
        currentMatchedMaxPathLength = mount.length
        currentMountPoint = mount
      }
    }
    currentMountPoint
  }
}
