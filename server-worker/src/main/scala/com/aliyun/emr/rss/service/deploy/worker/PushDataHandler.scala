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

import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentHashMap, ThreadPoolExecutor}
import java.util.concurrent.atomic.AtomicBoolean

import com.google.common.base.Throwables
import io.netty.buffer.ByteBuf

import com.aliyun.emr.rss.common.exception.AlreadyClosedException
import com.aliyun.emr.rss.common.internal.Logging
import com.aliyun.emr.rss.common.meta.{PartitionLocationInfo, WorkerInfo}
import com.aliyun.emr.rss.common.network.buffer.{NettyManagedBuffer, NioManagedBuffer}
import com.aliyun.emr.rss.common.network.client.{RpcResponseCallback, TransportClient, TransportClientFactory}
import com.aliyun.emr.rss.common.network.protocol.{PushData, PushMergedData, RequestMessage, RpcFailure, RpcResponse}
import com.aliyun.emr.rss.common.network.server.BaseMessageHandler
import com.aliyun.emr.rss.common.protocol.{PartitionLocation, PartitionSplitMode}
import com.aliyun.emr.rss.common.protocol.message.StatusCode
import com.aliyun.emr.rss.common.unsafe.Platform

class PushDataHandler extends BaseMessageHandler with Logging {

  var workerSource: WorkerSource = _
  var partitionLocationInfo: PartitionLocationInfo = _
  var shuffleMapperAttempts: ConcurrentHashMap[String, Array[Int]] = _
  var replicateThreadPool: ThreadPoolExecutor = _
  var unavailablePeers: ConcurrentHashMap[WorkerInfo, Long] = _
  var pushClientFactory: TransportClientFactory = _
  var registered: AtomicBoolean = _

  def init(worker: Worker): Unit = {
    workerSource = worker.workerSource
    partitionLocationInfo = worker.partitionLocationInfo
    shuffleMapperAttempts = worker.shuffleMapperAttempts
    replicateThreadPool = worker.replicateThreadPool
    unavailablePeers = worker.unavailablePeers
    pushClientFactory = worker.pushClientFactory
    registered = worker.registered
  }

  override def receive(client: TransportClient, msg: RequestMessage): Unit =
    msg match {
      case pushData: PushData =>
        try {
          handlePushData(pushData, new RpcResponseCallback {
            override def onSuccess(response: ByteBuffer): Unit = {
              client.getChannel.writeAndFlush(new RpcResponse(
                pushData.requestId, new NioManagedBuffer(response)))
            }

            override def onFailure(e: Throwable): Unit = {
              logError("[processPushData] Process pushData onFailure! ShuffleKey: "
                + pushData.shuffleKey + ", partitionUniqueId: " + pushData.partitionUniqueId, e)
              client.getChannel.writeAndFlush(new RpcFailure(pushData.requestId, e.getMessage))
            }
          })
        } catch {
          case e: Exception =>
            logError(s"Error while handlePushData $pushData", e)
            client.getChannel.writeAndFlush(new RpcFailure(pushData.requestId,
              Throwables.getStackTraceAsString(e)))
        } finally {
          pushData.body().release()
        }
      case pushMergedData: PushMergedData =>
        try {
          handlePushMergedData(pushMergedData, new RpcResponseCallback {
            override def onSuccess(response: ByteBuffer): Unit = {
              client.getChannel.writeAndFlush(new RpcResponse(
                pushMergedData.requestId, new NioManagedBuffer(response)))
            }

            override def onFailure(e: Throwable): Unit = {
              logError("[processPushMergedData] Process PushMergedData onFailure! ShuffleKey: " +
                pushMergedData.shuffleKey +
                ", partitionUniqueId: " + pushMergedData.partitionUniqueIds.mkString(","), e)
              client.getChannel.writeAndFlush(
                new RpcFailure(pushMergedData.requestId, e.getMessage()))
            }
          })
        } catch {
          case e: Exception =>
            logError(s"Error while handlePushMergedData $pushMergedData", e);
            client.getChannel.writeAndFlush(new RpcFailure(pushMergedData.requestId,
              Throwables.getStackTraceAsString(e)));
        } finally {
          pushMergedData.body().release()
        }
    }

  def handlePushData(pushData: PushData, callback: RpcResponseCallback): Unit = {
    val shuffleKey = pushData.shuffleKey
    val mode = PartitionLocation.getMode(pushData.mode)
    val body = pushData.body.asInstanceOf[NettyManagedBuffer].getBuf
    val isMaster = mode == PartitionLocation.Mode.Master

    val key = s"${pushData.requestId}"
    if (isMaster) {
      workerSource.startTimer(WorkerSource.MasterPushDataTime, key)
    } else {
      workerSource.startTimer(WorkerSource.SlavePushDataTime, key)
    }

    // find FileWriter responsible for the data
    val location = if (isMaster) {
      partitionLocationInfo.getMasterLocation(shuffleKey, pushData.partitionUniqueId)
    } else {
      partitionLocationInfo.getSlaveLocation(shuffleKey, pushData.partitionUniqueId)
    }

    val wrappedCallback = new RpcResponseCallback() {
      override def onSuccess(response: ByteBuffer): Unit = {
        if (isMaster) {
          workerSource.stopTimer(WorkerSource.MasterPushDataTime, key)
          if (response.remaining() > 0) {
            val resp = ByteBuffer.allocate(response.remaining())
            resp.put(response)
            resp.flip()
            callback.onSuccess(resp)
          } else {
            callback.onSuccess(response)
          }
        } else {
          workerSource.stopTimer(WorkerSource.SlavePushDataTime, key)
          callback.onSuccess(response)
        }
      }

      override def onFailure(e: Throwable): Unit = {
        logError(s"[handlePushData.onFailure] partitionLocation: $location")
        workerSource.incCounter(WorkerSource.PushDataFailCount)
        callback.onFailure(new Exception(StatusCode.PushDataFailSlave.getMessage(), e))
      }
    }

    if (location == null) {
      val (mapId, attemptId) = getMapAttempt(body)
      if (shuffleMapperAttempts.containsKey(shuffleKey) &&
        -1 != shuffleMapperAttempts.get(shuffleKey)(mapId)) {
        // partition data has already been committed
        logInfo(s"Receive push data from speculative task(shuffle $shuffleKey, map $mapId, " +
          s" attempt $attemptId), but this mapper has already been ended.")
        wrappedCallback.onSuccess(ByteBuffer.wrap(Array[Byte](StatusCode.StageEnded.getValue)))
      } else {
        val msg = s"Partition location wasn't found for task(shuffle $shuffleKey, map $mapId, " +
          s"attempt $attemptId, uniqueId ${pushData.partitionUniqueId})."
        logWarning(s"[handlePushData] $msg")
        callback.onFailure(new Exception(StatusCode.PushDataFailPartitionNotFound.getMessage()))
      }
      return
    }
    val fileWriter = location.asInstanceOf[WorkingPartition].getFileWriter
    val exception = fileWriter.getException
    if (exception != null) {
      logWarning(s"[handlePushData] fileWriter $fileWriter has Exception $exception")
      val message = if (isMaster) {
        StatusCode.PushDataFailMain.getMessage()
      } else {
        StatusCode.PushDataFailSlave.getMessage()
      }
      callback.onFailure(new Exception(message, exception))
      return
    }
    if (isMaster && fileWriter.getFileLength > fileWriter.getSplitThreshold()) {
      fileWriter.setSplitFlag()
      if (fileWriter.getSplitMode == PartitionSplitMode.soft) {
        callback.onSuccess(ByteBuffer.wrap(Array[Byte](StatusCode.SoftSplit.getValue)))
      } else {
        callback.onSuccess(ByteBuffer.wrap(Array[Byte](StatusCode.HardSplit.getValue)))
        return
      }
    }
    fileWriter.incrementPendingWrites()

    // for master, send data to slave
    if (location.getPeer != null && isMaster) {
      pushData.body().retain()
      replicateThreadPool.submit(new Runnable {
        override def run(): Unit = {
          val peer = location.getPeer
          val peerWorker = new WorkerInfo(
            peer.getHost,
            peer.getRpcPort,
            peer.getPushPort,
            peer.getFetchPort,
            peer.getReplicatePort
          )
          if (unavailablePeers.containsKey(peerWorker)) {
            pushData.body().release()
            wrappedCallback.onFailure(new Exception(s"Peer $peerWorker unavailable!"))
            return
          }
          try {
            val client = pushClientFactory.createClient(peer.getHost, peer.getReplicatePort,
              location.getId)
            val newPushData = new PushData(
              PartitionLocation.Mode.Slave.mode(),
              shuffleKey,
              pushData.partitionUniqueId,
              pushData.body)
            client.pushData(newPushData, wrappedCallback)
          } catch {
            case e: Exception =>
              pushData.body().release()
              unavailablePeers.put(peerWorker, System.currentTimeMillis())
              wrappedCallback.onFailure(e)
          }
        }
      })
    } else {
      wrappedCallback.onSuccess(ByteBuffer.wrap(Array[Byte]()))
    }

    try {
      fileWriter.write(body)
    } catch {
      case e: AlreadyClosedException =>
        fileWriter.decrementPendingWrites()
        val (mapId, attemptId) = getMapAttempt(body)
        val endedAttempt = if (shuffleMapperAttempts.containsKey(shuffleKey)) {
          shuffleMapperAttempts.get(shuffleKey)(mapId)
        } else -1
        logWarning(s"Append data failed for task(shuffle $shuffleKey, map $mapId, attempt" +
          s" $attemptId), caused by ${e.getMessage}")
      case e: Exception =>
        logError("Exception encountered when write.", e)
    }
  }

  def handlePushMergedData(
    pushMergedData: PushMergedData,
    callback: RpcResponseCallback): Unit = {
    val shuffleKey = pushMergedData.shuffleKey
    val mode = PartitionLocation.getMode(pushMergedData.mode)
    val batchOffsets = pushMergedData.batchOffsets
    val body = pushMergedData.body.asInstanceOf[NettyManagedBuffer].getBuf
    val isMaster = mode == PartitionLocation.Mode.Master

    val key = s"${pushMergedData.requestId}"
    if (isMaster) {
      workerSource.startTimer(WorkerSource.MasterPushDataTime, key)
    } else {
      workerSource.startTimer(WorkerSource.SlavePushDataTime, key)
    }

    val wrappedCallback = new RpcResponseCallback() {
      override def onSuccess(response: ByteBuffer): Unit = {
        if (isMaster) {
          workerSource.stopTimer(WorkerSource.MasterPushDataTime, key)
          if (response.remaining() > 0) {
            val resp = ByteBuffer.allocate(response.remaining())
            resp.put(response)
            resp.flip()
            callback.onSuccess(resp)
          } else {
            callback.onSuccess(response)
          }
        } else {
          workerSource.stopTimer(WorkerSource.SlavePushDataTime, key)
          callback.onSuccess(response)
        }
      }

      override def onFailure(e: Throwable): Unit = {
        workerSource.incCounter(WorkerSource.PushDataFailCount)
        callback.onFailure(new Exception(StatusCode.PushDataFailSlave.getMessage, e))
      }
    }

    // find FileWriters responsible for the data
    val locations = pushMergedData.partitionUniqueIds.map { id =>
      val loc = if (isMaster) {
        partitionLocationInfo.getMasterLocation(shuffleKey, id)
      } else {
        partitionLocationInfo.getSlaveLocation(shuffleKey, id)
      }
      if (loc == null) {
        val (mapId, attemptId) = getMapAttempt(body)
        if (shuffleMapperAttempts.containsKey(shuffleKey)
          && -1 != shuffleMapperAttempts.get(shuffleKey)(mapId)) {
          val msg = s"Receive push data from speculative task(shuffle $shuffleKey, map $mapId," +
            s" attempt $attemptId), but this mapper has already been ended."
          logInfo(msg)
          wrappedCallback.onSuccess(ByteBuffer.wrap(Array[Byte](StatusCode.StageEnded.getValue)))
        } else {
          val msg = s"Partition location wasn't found for task(shuffle $shuffleKey, map $mapId," +
            s" attempt $attemptId, uniqueId $id)."
          logWarning(s"[handlePushMergedData] $msg")
          wrappedCallback.onFailure(new Exception(msg))
        }
        return
      }
      loc
    }

    val fileWriters = locations.map(_.asInstanceOf[WorkingPartition].getFileWriter)
    val fileWriterWithException = fileWriters.find(_.getException != null)
    if (fileWriterWithException.nonEmpty) {
      val exception = fileWriterWithException.get.getException
      logDebug(s"[handlePushMergedData] fileWriter ${fileWriterWithException}" +
        s" has Exception $exception")
      val message = if (isMaster) {
        StatusCode.PushDataFailMain.getMessage()
      } else {
        StatusCode.PushDataFailSlave.getMessage()
      }
      callback.onFailure(new Exception(message, exception))
      return
    }
    fileWriters.foreach(_.incrementPendingWrites())

    // for master, send data to slave
    if (locations.head.getPeer != null && isMaster) {
      pushMergedData.body().retain()
      replicateThreadPool.submit(new Runnable {
        override def run(): Unit = {
          val location = locations.head
          val peer = location.getPeer
          val peerWorker = new WorkerInfo(
            peer.getHost,
            peer.getRpcPort,
            peer.getPushPort,
            peer.getFetchPort,
            peer.getReplicatePort
          )
          if (unavailablePeers.containsKey(peerWorker)) {
            pushMergedData.body().release()
            wrappedCallback.onFailure(new Exception(s"Peer $peerWorker unavailable!"))
            return
          }
          try {
            val client = pushClientFactory.createClient(
              peer.getHost, peer.getReplicatePort, location.getId)
            val newPushMergedData = new PushMergedData(
              PartitionLocation.Mode.Slave.mode(),
              shuffleKey,
              pushMergedData.partitionUniqueIds,
              batchOffsets,
              pushMergedData.body)
            client.pushMergedData(newPushMergedData, wrappedCallback)
          } catch {
            case e: Exception =>
              pushMergedData.body().release()
              unavailablePeers.put(peerWorker, System.currentTimeMillis())
              wrappedCallback.onFailure(e)
          }
        }
      })
    } else {
      wrappedCallback.onSuccess(ByteBuffer.wrap(Array[Byte]()))
    }

    var index = 0
    var fileWriter: FileWriter = null
    var alreadyClosed = false
    while (index < fileWriters.length) {
      fileWriter = fileWriters(index)
      val offset = body.readerIndex() + batchOffsets(index)
      val length = if (index == fileWriters.length - 1) {
        body.readableBytes() - batchOffsets(index)
      } else {
        batchOffsets(index + 1) - batchOffsets(index)
      }
      val batchBody = body.slice(offset, length)

      try {
        if (!alreadyClosed) {
          fileWriter.write(batchBody)
        } else {
          fileWriter.decrementPendingWrites()
        }
      } catch {
        case e: AlreadyClosedException =>
          fileWriter.decrementPendingWrites()
          alreadyClosed = true
          val (mapId, attemptId) = getMapAttempt(body)
          val endedAttempt = if (shuffleMapperAttempts.containsKey(shuffleKey)) {
            shuffleMapperAttempts.get(shuffleKey)(mapId)
          } else -1
          logWarning(s"Append data failed for task(shuffle $shuffleKey, map $mapId, attempt" +
            s" $attemptId), caused by ${e.getMessage}")
        case e: Exception =>
          logError("Exception encountered when write.", e)
      }
      index += 1
    }
  }

  private def getMapAttempt(body: ByteBuf): (Int, Int) = {
    // header: mapId attemptId batchId compressedTotalSize
    val header = new Array[Byte](8)
    body.getBytes(body.readerIndex(), header)
    val mapId = Platform.getInt(header, Platform.BYTE_ARRAY_OFFSET)
    val attemptId = Platform.getInt(header, Platform.BYTE_ARRAY_OFFSET + 4)
    (mapId, attemptId)
  }

  override def checkRegistered(): Boolean = registered.get()
}
