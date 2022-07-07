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

package com.aliyun.emr.rss.common.protocol.message

import java.util
import java.util.UUID

import com.aliyun.emr.rss.common.meta.WorkerInfo
import com.aliyun.emr.rss.common.protocol.{PartitionLocation, PartitionSplitMode, TransportMessages}

sealed trait Message extends Serializable
sealed trait MasterMessage extends Message
sealed abstract class MasterRequestMessage extends MasterMessage {
  var requestId: String

  def requestId_(id: String): Unit = {
    this.requestId = id
  }
}
sealed trait WorkerMessage extends Message
sealed trait ClientMessage extends Message

object ControlMessages {
  val ZERO_UUID = new UUID(0L, 0L).toString

  type WorkerResource = java.util.HashMap[WorkerInfo,
    (java.util.List[PartitionLocation], java.util.List[PartitionLocation])]

  /** ==========================================
   *         handled by master
   *  ==========================================
   */
  case object CheckForWorkerTimeOut extends Message

  case object CheckForApplicationTimeOut extends Message

  case object RemoveExpiredShuffle extends Message

  /**
   * The response message for one-way message. Due to the Master HA, we must know whether a Master
   * is the leader, and the one-way message does not care about the response, so there is no
   * corresponding response message. So add a response message to get the response.
   */
  case object OneWayMessageResponse extends Message

  case class RegisterWorker(
      host: String,
      rpcPort: Int,
      pushPort: Int,
      fetchPort: Int,
      replicatePort: Int,
      numSlots: Int,
      override var requestId: String = ZERO_UUID)
    extends MasterRequestMessage

  case class HeartbeatFromWorker(
      host: String,
      rpcPort: Int,
      pushPort: Int,
      fetchPort: Int,
      replicatePort : Int,
      numSlots: Int,
      shuffleKeys: util.HashSet[String],
    override var requestId: String = ZERO_UUID) extends MasterRequestMessage

  case class HeartbeatResponse(
      expiredShuffleKeys: util.HashSet[String],
      registered: Boolean) extends MasterMessage

  case class RegisterShuffle(
      applicationId: String,
      shuffleId: Int,
      numMappers: Int,
      numPartitions: Int)
    extends MasterMessage

  case class RegisterShuffleResponse(
    status: StatusCode,
    partitionLocations: util.List[PartitionLocation])
    extends MasterMessage

  case class RequestSlots(
    applicationId: String,
    shuffleId: Int,
    reduceIdList: util.ArrayList[Integer],
    hostname: String,
    shouldReplicate: Boolean,
    override var requestId: String = ZERO_UUID)
    extends MasterRequestMessage

  case class ReleaseSlots(
    applicationId: String,
    shuffleId: Int,
    workerIds: util.List[String],
    slots: util.List[Integer],
    override var requestId: String = ZERO_UUID)
    extends MasterRequestMessage

  case class ReleaseSlotsResponse(status: StatusCode)
    extends MasterMessage

  case class RequestSlotsResponse(
    status: StatusCode,
    workerResource: WorkerResource)
    extends MasterMessage

  trait ChangeLocationRequest

  case class Revive(
      applicationId: String,
      shuffleId: Int,
      mapId: Int,
      attemptId: Int,
      reduceId: Int,
      epoch: Int,
      oldPartition: PartitionLocation,
      cause: StatusCode)
    extends MasterMessage with ChangeLocationRequest

  case class PartitionSplit(
      applicationId : String,
      shuffleId: Int,
      reduceId: Int,
      epoch: Int,
      oldPartition: PartitionLocation)
    extends MasterMessage with ChangeLocationRequest

  case class ChangeLocationResponse(
      status: StatusCode,
      partition: PartitionLocation)
    extends MasterMessage

  case class MapperEnd(
      applicationId: String,
      shuffleId: Int,
      mapId: Int,
      attemptId: Int,
      numMappers: Int)
    extends MasterMessage

  case class MapperEndResponse(status: StatusCode) extends MasterMessage

  case class GetReducerFileGroup(applicationId: String, shuffleId: Int) extends MasterMessage

  // util.Set[String] -> util.Set[Path.toString]
  // Path can't be serialized
  case class GetReducerFileGroupResponse(
      status: StatusCode,
      fileGroup: Array[Array[PartitionLocation]],
      attempts: Array[Int])
    extends MasterMessage

  case class WorkerLost(host: String, rpcPort: Int, pushPort: Int, fetchPort: Int,
    replicatePort: Int, override var requestId: String = ZERO_UUID) extends MasterRequestMessage

  case class WorkerLostResponse(success: Boolean) extends MasterMessage

  case class StageEnd(applicationId: String, shuffleId: Int) extends MasterMessage

  case class StageEndResponse(status: StatusCode)
    extends MasterMessage

  case class UnregisterShuffle(
      appId: String,
      shuffleId: Int,
      override var requestId: String = ZERO_UUID) extends MasterRequestMessage

  case class UnregisterShuffleResponse(status: StatusCode) extends MasterMessage

  case class ApplicationLost(
      appId: String, override var requestId: String = ZERO_UUID) extends MasterRequestMessage

  case class ApplicationLostResponse(status: StatusCode) extends MasterMessage

  case class HeartBeatFromApplication(
      appId: String, override var requestId: String = ZERO_UUID) extends MasterRequestMessage

  case class GetBlacklist(localBlacklist: util.List[WorkerInfo]) extends MasterMessage

  case class GetBlacklistResponse(statusCode: StatusCode,
      blacklist: util.List[WorkerInfo], unknownWorkers: util.List[WorkerInfo]) extends Message

  case class GetClusterLoadStatus(numPartitions: Int) extends Message

  case class GetClusterLoadStatusResponse(isOverload: Boolean) extends Message

  case class ReportWorkerFailure(
      failed: util.List[WorkerInfo],
      override var requestId: String = ZERO_UUID) extends MasterRequestMessage

  /** ==========================================
   *         handled by worker
   *  ==========================================
   */
  case class RegisterWorkerResponse(success: Boolean, message: String) extends WorkerMessage

  case class ReregisterWorkerResponse(success: Boolean) extends WorkerMessage

  case class ReserveSlots(
      applicationId: String,
      shuffleId: Int,
      masterLocations: util.List[PartitionLocation],
      slaveLocations: util.List[PartitionLocation],
      splitThreshold: Long,
      splitMode: PartitionSplitMode,
      storageHint: PartitionLocation.StorageHint)
    extends WorkerMessage

  case class ReserveSlotsResponse(
      status: StatusCode, reason: String = "") extends WorkerMessage

  case class CommitFiles(
    applicationId: String,
    shuffleId: Int,
    masterIds: util.List[String],
    slaveIds: util.List[String],
    mapAttempts: Array[Int])
    extends WorkerMessage

  case class CommitFilesResponse(
      status: StatusCode,
      committedMasterIds: util.List[String],
      committedSlaveIds: util.List[String],
      failedMasterIds: util.List[String],
      failedSlaveIds: util.List[String])
    extends WorkerMessage

  case class Destroy(
      shuffleKey: String,
      masterLocations: util.List[String],
      slaveLocations: util.List[String])
    extends WorkerMessage

  case class DestroyResponse(
      status: StatusCode,
      failedMasters: util.List[String],
      failedSlaves: util.List[String])
    extends WorkerMessage

  /** ==========================================
   *              common
   *  ==========================================
   */
  case class SlaveLostResponse(status: StatusCode, slaveLocation: PartitionLocation) extends Message

  case object GetWorkerInfos extends Message

  case class GetWorkerInfosResponse(status: StatusCode, workerInfos: WorkerInfo*) extends Message

  case object ThreadDump extends Message

  case class ThreadDumpResponse(threadDump: String) extends Message
}
