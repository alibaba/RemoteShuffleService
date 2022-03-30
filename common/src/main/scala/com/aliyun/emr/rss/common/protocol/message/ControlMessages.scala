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

import scala.collection.JavaConverters._

import com.aliyun.emr.rss.common.internal.Logging
import com.aliyun.emr.rss.common.meta.WorkerInfo
import com.aliyun.emr.rss.common.network.protocol.TransportMessage
import com.aliyun.emr.rss.common.protocol.{PartitionLocation, TransportMessages}
import com.aliyun.emr.rss.common.protocol.TransportMessages._
import com.aliyun.emr.rss.common.protocol.TransportMessages.MessageType._
import com.aliyun.emr.rss.common.util.Utils

sealed trait Message extends Serializable{
  import com.aliyun.emr.rss.common.protocol.message.ControlMessages._
  def toTransportMessage(): TransportMessage = {
    this match {
      case CheckForWorkerTimeOut =>
        new TransportMessage(TransportMessages.MessageType.CHECK_FOR_WORKER_TIMEOUT, null)

      case CheckForApplicationTimeOut =>
        new TransportMessage(TransportMessages.MessageType.CHECK_FOR_APPLICATION_TIMEOUT, null)

      case RemoveExpiredShuffle =>
        new TransportMessage(TransportMessages.MessageType.REMOVE_EXPIRED_SHUFFLE, null)

      case RegisterWorker(host, rpcPort, pushPort, fetchPort, numSlots, requestId) =>
        val payload = TransportMessages.PbRegisterWorker.newBuilder()
          .setHost(host)
          .setRpcPort(rpcPort)
          .setPushPort(pushPort)
          .setFetchPort(fetchPort)
          .setNumSlots(numSlots)
          .setRequestId(requestId)
          .build().toByteArray
        new TransportMessage(TransportMessages.MessageType.REGISTER_WORKER, payload)

      case HeartbeatFromWorker(host, rpcPort, pushPort, fetchPort, numSlots,
      shuffleKeys, requestId) =>
        val payload = TransportMessages.PbHeartbeatFromWorker.newBuilder()
          .setHost(host)
          .setRpcPort(rpcPort)
          .setPushPort(pushPort)
          .setFetchPort(fetchPort)
          .setNumSlots(numSlots)
          .addAllShuffleKeys(shuffleKeys)
          .setRequestId(requestId)
          .build().toByteArray
        new TransportMessage(TransportMessages.MessageType.HEARTBEAT_FROM_WORKER, payload)

      case HeartbeatResponse(expiredShuffleKeys) =>
        val payload = TransportMessages.PbHeartbeatResponse.newBuilder()
          .addAllExpiredShuffleKeys(expiredShuffleKeys)
          .build().toByteArray
        new TransportMessage(TransportMessages.MessageType.HEARTBEAT_RESPONSE, payload)

      case RegisterShuffle(applicationId, shuffleId, numMappers, numPartitions) =>
        val payload = TransportMessages.PbRegisterShuffle.newBuilder()
          .setApplicationId(applicationId)
          .setShuffleId(shuffleId)
          .setNumMapppers(numMappers)
          .setNumPartitions(numPartitions)
          .build().toByteArray
        new TransportMessage(TransportMessages.MessageType.REGISTER_SHUFFLE, payload)

      case RegisterShuffleResponse(status, partitionLocations) =>
        val builder = TransportMessages.PbRegisterShuffleResponse.newBuilder()
          .setStatus(status.getValue)
        if (partitionLocations != null) {
          builder.addAllPartitionLocations(partitionLocations.iterator().asScala
            .map(PartitionLocation.toPbPartitionLocation(_)).toList.asJava)
        }
        val payload = builder.build().toByteArray
        new TransportMessage(TransportMessages.MessageType.REGISTER_SHUFFLE_RESPONSE, payload)

      case RequestSlots(applicationId, shuffleId, reduceIdList, hostname,
      shouldReplicate, requestId) =>
        val payload = TransportMessages.PbRequestSlots.newBuilder()
          .setApplicationId(applicationId)
          .setShuffleId(shuffleId)
          .addAllReduceIdList(reduceIdList)
          .setHostname(hostname)
          .setShouldReplicate(shouldReplicate)
          .setRequestId(requestId)
          .build().toByteArray
        new TransportMessage(TransportMessages.MessageType.REQUEST_SLOTS, payload)

      case ReleaseSlots(applicationId, shuffleId, workerIds, slots, requestId) =>
        val payload = TransportMessages.PbReleaseSlots.newBuilder()
          .setApplicationId(applicationId)
          .setShuffleId(shuffleId)
          .addAllSlots(slots)
          .setRequestId(requestId)
          .addAllWorkerIds(workerIds)
          .build().toByteArray
        new TransportMessage(TransportMessages.MessageType.RELEASE_SLOTS, payload)

      case ReleaseSlotsResponse(status) =>
        val payload = TransportMessages.PbReleaseSlotsResponse.newBuilder()
          .setStatus(status.getValue).build().toByteArray
        new TransportMessage(TransportMessages.MessageType.RELEASE_SLOTS_RESPONSE, payload)

      case RequestSlotsResponse(status, workerResource) =>
        val builder = TransportMessages.PbRequestSlotsResponse.newBuilder()
          .setStatus(status.getValue)
        if (workerResource != null) {
          builder.putAllWorkerResource(
            Utils.convertWorkerResourceToPbWorkerResource(workerResource))
        }
        val payload = builder.build().toByteArray
        new TransportMessage(TransportMessages.MessageType.REQUEST_SLOTS_RESPONSE, payload)

      case Revive(applicationId, shuffleId, mapId, attemptId, reduceId,
      epoch, oldPartition, cause) =>
        val builder = TransportMessages.PbRevive.newBuilder()
        builder.setApplicationId(applicationId)
          .setShuffleId(shuffleId)
          .setMapId(mapId)
          .setAttemptId(attemptId)
          .setReduceId(reduceId)
          .setEpoch(epoch)
          .setStatus(cause.getValue)
        if (oldPartition != null) {
          builder.setOldPartition(PartitionLocation.toPbPartitionLocation(oldPartition))
        }
        val payload = builder.build().toByteArray
        new TransportMessage(TransportMessages.MessageType.REVIVE, payload)

      case ReviveResponse(status, partitionLocation) =>
        val builder = TransportMessages.PbReviveResponse.newBuilder()
          .setStatus(status.getValue)
        if (partitionLocation != null) {
          builder.setPartitionLocation(PartitionLocation.toPbPartitionLocation(partitionLocation))
        }
        val payload = builder.build().toByteArray
        new TransportMessage(TransportMessages.MessageType.REVIVE_RESPONSE, payload)

      case MapperEnd(applicationId, shuffleId, mapId, attemptId, numMappers) =>
        val payload = TransportMessages.PbMapperEnd.newBuilder()
          .setApplicationId(applicationId)
          .setShuffleId(shuffleId)
          .setMapId(mapId)
          .setAttemptId(attemptId)
          .setNumMappers(numMappers)
          .build().toByteArray
        new TransportMessage(TransportMessages.MessageType.MAPPER_END, payload)

      case MapperEndResponse(status) =>
        val payload = TransportMessages.PbMapperEndResponse.newBuilder()
          .setStatus(status.getValue)
          .build().toByteArray
        new TransportMessage(TransportMessages.MessageType.MAPPER_END_RESPONSE, payload)

      case GetReducerFileGroup(applicationId, shuffleId) =>
        val payload = TransportMessages.PbGetReducerFileGroup.newBuilder()
          .setApplicationId(applicationId).setShuffleId(shuffleId)
          .build().toByteArray
        new TransportMessage(TransportMessages.MessageType.GET_REDUCER_FILE_GROUP, payload)

      case GetReducerFileGroupResponse(status, fileGroup, attempts) =>
        val builder = TransportMessages.PbGetReducerFileGroupResponse.newBuilder()
          .setStatus(status.getValue)
        if (fileGroup != null) {
          builder.addAllFileGroup(fileGroup.map(arr => PbFileGroup.newBuilder()
            .addAllLocaltions(arr.map(PartitionLocation.toPbPartitionLocation(_)).toIterable.asJava)
            .build()).toIterable.asJava)
        }
        if (attempts != null) {
          builder.addAllAttempts(attempts.map(new Integer(_)).toIterable.asJava)
        }
        val payload = builder.build().toByteArray
        new TransportMessage(TransportMessages.MessageType.GET_REDUCER_FILE_GROUP_RESPONSE, payload)

      case WorkerLost(host, rpcPort, pushPort, fetchPort, requestId) =>
        val payload = TransportMessages.PbWorkerLost.newBuilder()
          .setHost(host)
          .setRpcPort(rpcPort)
          .setPushPort(pushPort)
          .setFetchPort(fetchPort)
          .setRequestId(requestId)
          .build().toByteArray
        new TransportMessage(TransportMessages.MessageType.WORKER_LOST, payload)

      case WorkerLostResponse(success) =>
        val payload = TransportMessages.PbWorkerLostResponse.newBuilder()
          .setSuccess(success)
          .build().toByteArray
        new TransportMessage(TransportMessages.MessageType.WORKER_LOST_RESPONSE, payload)

      case StageEnd(applicationId, shuffleId) =>
        val payload = TransportMessages.PbStageEnd.newBuilder()
          .setApplicationId(applicationId)
          .setShuffleId(shuffleId)
          .build().toByteArray
        new TransportMessage(TransportMessages.MessageType.STAGE_END, payload)

      case StageEndResponse(status) =>
        val payload = TransportMessages.PbStageEndResponse.newBuilder()
          .setStatus(status.getValue)
          .build().toByteArray
        new TransportMessage(TransportMessages.MessageType.STAGE_END_RESPONSE, payload)

      case UnregisterShuffle(appId, shuffleId, requestId) =>
        val payload = TransportMessages.PbUnregisterShuffle.newBuilder()
          .setAppId(appId).setShuffleId(shuffleId).setRequestId(requestId)
          .build().toByteArray
        new TransportMessage(TransportMessages.MessageType.UNREGISTER_SHUFFLE, payload)

      case UnregisterShuffleResponse(status) =>
        val payload = TransportMessages.PbUnregisterShuffleResponse.newBuilder()
          .setStatus(status.getValue)
          .build().toByteArray
        new TransportMessage(TransportMessages.MessageType.UNREGISTER_SHUFFLE_RESPONSE, payload)

      case ApplicationLost(appId, requestId) =>
        val payload = TransportMessages.PbApplicationLost.newBuilder()
          .setAppId(appId).setRequestId(requestId)
          .build().toByteArray
        new TransportMessage(TransportMessages.MessageType.APPLICATION_LOST, payload)

      case ApplicationLostResponse(status) =>
        val payload = TransportMessages.PbApplicationLostResponse.newBuilder()
          .setStatus(status.getValue).build().toByteArray
        new TransportMessage(TransportMessages.MessageType.APPLICATION_LOST_RESPONSE, payload)

      case HeartBeatFromApplication(appId, requestId) =>
        val payload = TransportMessages.PbHeartBeatFromApplication.newBuilder()
          .setAppId(appId)
          .setRequestId(requestId)
          .build().toByteArray
        new TransportMessage(TransportMessages.MessageType.HEARTBEAT_FROM_APPLICATION, payload)

      case GetBlacklist(localBlacklist) =>
        val payload = TransportMessages.PbGetBlacklist.newBuilder()
          .addAllLocalBlackList(localBlacklist.asScala.map(WorkerInfo.toPbWorkerInfo(_))
            .toList.asJava)
          .build().toByteArray
        new TransportMessage(TransportMessages.MessageType.GET_BLACKLIST, payload)

      case GetBlacklistResponse(statusCode, blacklist, unknownWorkers) =>
        val builder = TransportMessages.PbGetBlacklistResponse.newBuilder()
          .setStatus(statusCode.getValue)
        if (blacklist != null) {
          builder.addAllBlacklist(blacklist.asScala.map(WorkerInfo.toPbWorkerInfo(_)).toList.asJava)
        }
        if (unknownWorkers != null) {
          builder.addAllUnknownWorkers(unknownWorkers.asScala
            .map(WorkerInfo.toPbWorkerInfo(_)).toList.asJava)
        }
        val payload = builder.build().toByteArray
        new TransportMessage(TransportMessages.MessageType.GET_BLACKLIST_RESPONSE, payload)

      case GetClusterLoadStatus(numPartitions) =>
        new TransportMessage(TransportMessages.MessageType.GET_CLUSTER_LOAD_STATUS,
          PbGetClusterLoadStatus.newBuilder().setNumPartitions(numPartitions).build().toByteArray)

      case GetClusterLoadStatusResponse(isOverload) =>
        val payload = TransportMessages.PbGetClusterLoadStatusResponse.newBuilder()
          .setIsOverload(isOverload)
          .build().toByteArray
        new TransportMessage(TransportMessages.MessageType.GET_CLUSTER_LOAD_STATUS_RESPONSE,
          payload)

      case ReportWorkerFailure(failed, requestId) =>
        val payload = TransportMessages.PbReportWorkerFailure.newBuilder()
          .addAllFailed(failed.asScala.map(WorkerInfo.toPbWorkerInfo(_)).toList.asJava)
          .setRequestId(requestId).build().toByteArray
        new TransportMessage(TransportMessages.MessageType.REPORT_WORKER_FAILURE, payload)

      case RegisterWorkerResponse(success, message) =>
        val payload = TransportMessages.PbRegisterWorkerResponse.newBuilder()
          .setSuccess(success)
          .setMessage(message)
          .build().toByteArray
        new TransportMessage(TransportMessages.MessageType.REGISTER_WORKER_RESPONSE, payload)

      case ReregisterWorkerResponse(success) =>
        val payload = TransportMessages.PbReregisterWorkerResponse.newBuilder()
          .setSuccess(success)
          .build().toByteArray
        new TransportMessage(TransportMessages.MessageType.REREGISTER_WORKER_RESPONSE, payload)

      case ReserveSlots(applicationId, shuffleId, masterLocations, slaveLocations) =>
        val payload = TransportMessages.PbReserveSlots.newBuilder()
          .setApplicationId(applicationId)
          .setShuffleId(shuffleId)
          .addAllMasterLocations(masterLocations.asScala
            .map(PartitionLocation.toPbPartitionLocation(_)).toList.asJava)
          .addAllSlaveLocations(slaveLocations.asScala
            .map(PartitionLocation.toPbPartitionLocation(_)).toList.asJava)
          .build().toByteArray
        new TransportMessage(TransportMessages.MessageType.RESERVE_SLOTS, payload)

      case ReserveSlotsResponse(status, reason) =>
        val payload = TransportMessages.PbReserveSlotsResponse.newBuilder()
          .setStatus(status.getValue).setReason(reason)
          .build().toByteArray
        new TransportMessage(TransportMessages.MessageType.RESERVE_SLOTS_RESPONSE, payload)

      case CommitFiles(applicationId, shuffleId, masterIds, slaveIds, mapAttempts) =>
        val payload = TransportMessages.PbCommitFiles.newBuilder()
          .setApplicationId(applicationId)
          .setShuffleId(shuffleId)
          .addAllMasterIds(masterIds)
          .addAllSlaveIds(slaveIds)
          .addAllMapAttempts(mapAttempts.map(new Integer(_)).toIterable.asJava)
          .build().toByteArray
        new TransportMessage(TransportMessages.MessageType.COMMIT_FILES, payload)

      case CommitFilesResponse(status, committedMasterIds, committedSlaveIds,
      failedMasterIds, failedSlaveIds) =>
        val builder = TransportMessages.PbCommitFilesResponse.newBuilder()
          .setStatus(status.getValue)
        if (committedMasterIds != null) {
          builder.addAllCommittedMasterIds(committedMasterIds)
        }
        if (committedSlaveIds != null) {
          builder.addAllCommittedSlaveIds(committedSlaveIds)
        }
        if (failedMasterIds != null) {
          builder.addAllFailedMasterIds(failedMasterIds)
        }
        if (failedSlaveIds != null) {
          builder.addAllFailedSlaveIds(failedSlaveIds)
        }
        val payload = builder.build().toByteArray
        new TransportMessage(TransportMessages.MessageType.COMMIT_FILES_RESPONSE, payload)

      case Destroy(shuffleKey, masterLocations, slaveLocations) =>
        val payload = TransportMessages.PbDestroy.newBuilder()
          .setShuffleKey(shuffleKey)
          .addAllMasterLocations(masterLocations)
          .addAllSlaveLocation(slaveLocations)
          .build().toByteArray
        new TransportMessage(TransportMessages.MessageType.DESTROY, payload)

      case DestroyResponse(status, failedMasters, failedSlaves) =>
        val builder = TransportMessages.PbDestroyResponse.newBuilder()
          .setStatus(status.getValue)
        if (failedMasters != null) {
          builder.addAllFailedMasters(failedMasters)
        }
        if (failedSlaves != null) {
          builder.addAllFailedSlaves(failedSlaves)
        }
        val payload = builder.build().toByteArray
        new TransportMessage(TransportMessages.MessageType.DESTROY_RESPONSE, payload)

      case SlaveLostResponse(status, slaveLocation) =>
        val payload = TransportMessages.PbSlaveLostResponse.newBuilder()
          .setStatus(status.getValue)
          .setSlaveLocation(PartitionLocation.toPbPartitionLocation(slaveLocation))
          .build().toByteArray
        new TransportMessage(TransportMessages.MessageType.SLAVE_LOST_RESPONSE, payload)

      case GetWorkerInfos =>
        new TransportMessage(TransportMessages.MessageType.GET_WORKER_INFO, null)

      case GetWorkerInfosResponse(status, workerInfos@_*) =>
        val payload = TransportMessages.PbGetWorkerInfosResponse.newBuilder()
          .setStatus(status.getValue)
          .addAllWorkerInfos(workerInfos.map(WorkerInfo.toPbWorkerInfo(_)).toList.asJava)
          .build().toByteArray
        new TransportMessage(TransportMessages.MessageType.GET_WORKER_INFO_RESPONSE, payload)

      case ThreadDump =>
        new TransportMessage(TransportMessages.MessageType.THREAD_DUMP, null)

      case ThreadDumpResponse(threadDump) =>
        val payload = TransportMessages.PbThreadDumpResponse.newBuilder()
          .setThreadDump(threadDump).build().toByteArray
        new TransportMessage(TransportMessages.MessageType.THREAD_DUMP_RESPONSE, payload)

      case OneWayMessageResponse =>
        new TransportMessage(TransportMessages.MessageType.ONE_WAY_MESSAGE_RESPONSE, null)
    }
  }
}
sealed trait MasterMessage extends Message
sealed abstract class MasterRequestMessage extends MasterMessage {
  var requestId: String

  def requestId_(id: String): Unit = {
    this.requestId = id
  }
}
sealed trait WorkerMessage extends Message
sealed trait ClientMessage extends Message

object ControlMessages extends Logging{
  val ZERO_UUID: String = new UUID(0L, 0L).toString

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
      numSlots: Int,
      override var requestId: String = ZERO_UUID)
    extends MasterRequestMessage

  case class HeartbeatFromWorker(
      host: String,
      rpcPort: Int,
      pushPort: Int,
      fetchPort: Int,
      numSlots: Int,
      shuffleKeys: util.HashSet[String],
    override var requestId: String = ZERO_UUID) extends MasterRequestMessage

  case class HeartbeatResponse(expiredShuffleKeys: util.HashSet[String]) extends MasterMessage

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

  case class Revive(
      applicationId: String,
      shuffleId: Int,
      mapId: Int,
      attemptId: Int,
      reduceId: Int,
      epoch: Int,
      oldPartition: PartitionLocation,
      cause: StatusCode)
    extends MasterMessage

  case class ReviveResponse(
      status: StatusCode,
      partitionLocation: PartitionLocation)
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
                        override var requestId: String = ZERO_UUID) extends MasterRequestMessage

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
      slaveLocations: util.List[PartitionLocation])
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

  def fromTransportMessage(message: TransportMessage): Message = {
    message.getType match {
      case UNKNOWN_MESSAGE =>
        val msg = s"received unknown message $message"
        logError(msg)
        throw new UnsupportedOperationException(msg)

      case REGISTER_WORKER =>
        val pbRegisterWorker = PbRegisterWorker.parseFrom(message.getPayload)
        RegisterWorker(pbRegisterWorker.getHost, pbRegisterWorker.getRpcPort,
          pbRegisterWorker.getPushPort, pbRegisterWorker.getFetchPort, pbRegisterWorker.getNumSlots,
          pbRegisterWorker.getRequestId)

      case HEARTBEAT_FROM_WORKER =>
        val pbHeartbeatFromWorker = PbHeartbeatFromWorker.parseFrom(message.getPayload)
        val shuffleKeys = new util.HashSet[String]()
        if (pbHeartbeatFromWorker.getShuffleKeysCount > 0) {
          shuffleKeys.addAll(pbHeartbeatFromWorker.getShuffleKeysList)
        }
        HeartbeatFromWorker(pbHeartbeatFromWorker.getHost, pbHeartbeatFromWorker.getRpcPort,
          pbHeartbeatFromWorker.getPushPort, pbHeartbeatFromWorker.getFetchPort,
          pbHeartbeatFromWorker.getNumSlots, shuffleKeys, pbHeartbeatFromWorker.getRequestId)

      case HEARTBEAT_RESPONSE =>
        val pbHeartBeatResponse = PbHeartbeatResponse.parseFrom(message.getPayload)
        val expiredShuffleKeys = new util.HashSet[String]()
        if (pbHeartBeatResponse.getExpiredShuffleKeysCount > 0) {
          expiredShuffleKeys.addAll(pbHeartBeatResponse.getExpiredShuffleKeysList)
        }
        HeartbeatResponse(expiredShuffleKeys)

      case REGISTER_SHUFFLE =>
        val pbRegisterShuffle = PbRegisterShuffle.parseFrom(message.getPayload)
        RegisterShuffle(pbRegisterShuffle.getApplicationId, pbRegisterShuffle.getShuffleId,
          pbRegisterShuffle.getNumMapppers, pbRegisterShuffle.getNumPartitions)

      case REGISTER_SHUFFLE_RESPONSE =>
        val pbRegisterShuffleResponse = PbRegisterShuffleResponse.parseFrom(message.getPayload)
        val partitionLocations = new util.ArrayList[PartitionLocation]()
        if (pbRegisterShuffleResponse.getPartitionLocationsCount > 0) {
          partitionLocations.addAll(pbRegisterShuffleResponse.getPartitionLocationsList
            .asScala.map(PartitionLocation.fromPbPartitionLocation(_)).toList.asJava)
        }
        RegisterShuffleResponse(Utils.toStatusCode(pbRegisterShuffleResponse.getStatus),
          partitionLocations)

      case REQUEST_SLOTS =>
        val pbRequestSlots = PbRequestSlots.parseFrom(message.getPayload)
        RequestSlots(pbRequestSlots.getApplicationId, pbRequestSlots.getShuffleId,
          new util.ArrayList[Integer](pbRequestSlots.getReduceIdListList),
          pbRequestSlots.getHostname, pbRequestSlots.getShouldReplicate,
          pbRequestSlots.getRequestId)

      case RELEASE_SLOTS =>
        val pbRequestSlots = PbReleaseSlots.parseFrom(message.getPayload)
        ReleaseSlots(pbRequestSlots.getApplicationId, pbRequestSlots.getShuffleId,
          new util.ArrayList[String](pbRequestSlots.getWorkerIdsList),
          new util.ArrayList[Integer](pbRequestSlots.getSlotsList), pbRequestSlots.getRequestId)

      case RELEASE_SLOTS_RESPONSE =>
        val pbReleaseSlotsResponse = PbReleaseSlotsResponse.parseFrom(message.getPayload)
        ReleaseSlotsResponse(Utils.toStatusCode(pbReleaseSlotsResponse.getStatus))

      case REQUEST_SLOTS_RESPONSE =>
        val pbRequestSlotsResponse = PbRequestSlotsResponse.parseFrom(message.getPayload)
        val workerResource = if (pbRequestSlotsResponse.getWorkerResourceCount > 0) {
          Utils.convertPbWorkerResourceToWorkerResource(pbRequestSlotsResponse.getWorkerResourceMap)
        } else {
          null
        }
        RequestSlotsResponse(Utils.toStatusCode(pbRequestSlotsResponse.getStatus), workerResource)

      case REVIVE =>
        val pbRevive = PbRevive.parseFrom(message.getPayload)
        val oldPartition = if (pbRevive.hasOldPartition) {
          PartitionLocation.fromPbPartitionLocation(pbRevive.getOldPartition)
        } else {
          null
        }
        Revive(pbRevive.getApplicationId, pbRevive.getShuffleId, pbRevive.getMapId,
          pbRevive.getAttemptId, pbRevive.getReduceId, pbRevive.getEpoch,
          oldPartition, Utils.toStatusCode(pbRevive.getStatus))

      case REVIVE_RESPONSE =>
        val pbReviveResponse = PbReviveResponse.parseFrom(message.getPayload)
        val loc = if (pbReviveResponse.hasPartitionLocation) {
          PartitionLocation.fromPbPartitionLocation(pbReviveResponse.getPartitionLocation)
        } else null
        ReviveResponse(Utils.toStatusCode(pbReviveResponse.getStatus), loc)

      case MAPPER_END =>
        val pbMapperEnd = PbMapperEnd.parseFrom(message.getPayload)
        MapperEnd(pbMapperEnd.getApplicationId, pbMapperEnd.getShuffleId, pbMapperEnd.getMapId,
          pbMapperEnd.getAttemptId, pbMapperEnd.getNumMappers)

      case MAPPER_END_RESPONSE =>
        val pbMapperEndResponse = PbMapperEndResponse.parseFrom(message.getPayload)
        MapperEndResponse(Utils.toStatusCode(pbMapperEndResponse.getStatus))

      case GET_REDUCER_FILE_GROUP =>
        val pbGetReducerFileGroup = PbGetReducerFileGroup.parseFrom(message.getPayload)
        GetReducerFileGroup(pbGetReducerFileGroup.getApplicationId,
          pbGetReducerFileGroup.getShuffleId)

      case GET_REDUCER_FILE_GROUP_RESPONSE =>
        val pbGetReducerFileGroupResponse = PbGetReducerFileGroupResponse
          .parseFrom(message.getPayload)
        val fileGroup = if (pbGetReducerFileGroupResponse.getFileGroupCount > 0) {
          pbGetReducerFileGroupResponse.getFileGroupList.asScala
            .map(fg => fg.getLocaltionsList.asScala
              .map(PartitionLocation.fromPbPartitionLocation(_)).toArray).toArray
        } else null
        val attempts = if (pbGetReducerFileGroupResponse.getAttemptsCount > 0) {
          pbGetReducerFileGroupResponse.getAttemptsList().asScala.map(Int.unbox(_)).toArray
        } else null
        GetReducerFileGroupResponse(Utils.toStatusCode(pbGetReducerFileGroupResponse.getStatus),
          fileGroup, attempts)

      case UNREGISTER_SHUFFLE =>
        val pbUnregisterShuffle = PbUnregisterShuffle.parseFrom(message.getPayload)
        UnregisterShuffle(pbUnregisterShuffle.getAppId, pbUnregisterShuffle.getShuffleId,
          pbUnregisterShuffle.getRequestId)

      case UNREGISTER_SHUFFLE_RESPONSE =>
        val pbUnregisterShuffleResponse = PbUnregisterShuffleResponse.parseFrom(message.getPayload)
        UnregisterShuffleResponse(Utils.toStatusCode(pbUnregisterShuffleResponse.getStatus))

      case APPLICATION_LOST =>
        val pbApplicationLost = PbApplicationLost.parseFrom(message.getPayload)
        ApplicationLost(pbApplicationLost.getAppId, pbApplicationLost.getRequestId)

      case APPLICATION_LOST_RESPONSE =>
        val pbApplicationLostResponse = PbApplicationLostResponse.parseFrom(message.getPayload)
        ApplicationLostResponse(Utils.toStatusCode(pbApplicationLostResponse.getStatus))

      case HEARTBEAT_FROM_APPLICATION =>
        val pbHeartBeatFromApplication = PbHeartBeatFromApplication.parseFrom(message.getPayload)
        HeartBeatFromApplication(pbHeartBeatFromApplication.getAppId,
          pbHeartBeatFromApplication.getRequestId)

      case GET_BLACKLIST =>
        val pbGetBlacklist = PbGetBlacklist.parseFrom(message.getPayload)
        GetBlacklist(new util.ArrayList[WorkerInfo](pbGetBlacklist.getLocalBlackListList.asScala
          .map(WorkerInfo.fromPbWorkerInfo(_)).toList.asJava))

      case GET_BLACKLIST_RESPONSE =>
        val pbGetBlacklistResponse = PbGetBlacklistResponse.parseFrom(message.getPayload)
        val blacklist = if (pbGetBlacklistResponse.getBlacklistCount > 0) {
          pbGetBlacklistResponse.getBlacklistList.asScala
            .map(WorkerInfo.fromPbWorkerInfo(_)).toList.asJava
        } else null
        val unkownList = if (pbGetBlacklistResponse.getUnknownWorkersCount > 0) {
          pbGetBlacklistResponse.getUnknownWorkersList.asScala
            .map(WorkerInfo.fromPbWorkerInfo(_)).toList.asJava
        } else null
        GetBlacklistResponse(Utils.toStatusCode(pbGetBlacklistResponse.getStatus),
          blacklist, unkownList)

      case GET_CLUSTER_LOAD_STATUS =>
        val pbGetClusterLoadStats = PbGetClusterLoadStatus.parseFrom(message.getPayload)
        GetClusterLoadStatus(pbGetClusterLoadStats.getNumPartitions)

      case GET_CLUSTER_LOAD_STATUS_RESPONSE =>
        val pbGetClusterLoadStatusResponse = PbGetClusterLoadStatusResponse
          .parseFrom(message.getPayload)
        GetClusterLoadStatusResponse(pbGetClusterLoadStatusResponse.getIsOverload)

      case REPORT_WORKER_FAILURE =>
        val pbReportWorkerFailure = PbReportWorkerFailure.parseFrom(message.getPayload)
        ReportWorkerFailure(new util.ArrayList[WorkerInfo](pbReportWorkerFailure.getFailedList
          .asScala.map(WorkerInfo.fromPbWorkerInfo(_)).toList.asJava),
          pbReportWorkerFailure.getRequestId)

      case REGISTER_WORKER_RESPONSE =>
        val pbRegisterWorkerResponse = PbRegisterWorkerResponse.parseFrom(message.getPayload)
        RegisterWorkerResponse(pbRegisterWorkerResponse.getSuccess,
          pbRegisterWorkerResponse.getMessage)

      case REREGISTER_WORKER_RESPONSE =>
        val pbReregisterWorkerResponse = PbReregisterWorkerResponse.parseFrom(message.getPayload)
        ReregisterWorkerResponse(pbReregisterWorkerResponse.getSuccess)

      case RESERVE_SLOTS =>
        val pbReserveSlots = PbReserveSlots.parseFrom(message.getPayload)
        ReserveSlots(pbReserveSlots.getApplicationId,
          pbReserveSlots.getShuffleId,
          new util.ArrayList[PartitionLocation](pbReserveSlots.getMasterLocationsList.asScala
            .map(PartitionLocation.fromPbPartitionLocation(_)).toList.asJava),
          new util.ArrayList[PartitionLocation](pbReserveSlots.getSlaveLocationsList.asScala
            .map(PartitionLocation.fromPbPartitionLocation(_)).toList.asJava))

      case RESERVE_SLOTS_RESPONSE =>
        val pbReserveSlotsResponse = PbReserveSlotsResponse.parseFrom(message.getPayload)
        ReserveSlotsResponse(Utils.toStatusCode(pbReserveSlotsResponse.getStatus),
          pbReserveSlotsResponse.getReason)

      case COMMIT_FILES =>
        val pbCommitFiles = PbCommitFiles.parseFrom(message.getPayload)
        CommitFiles(pbCommitFiles.getApplicationId, pbCommitFiles.getShuffleId,
          pbCommitFiles.getMasterIdsList, pbCommitFiles.getSlaveIdsList,
          pbCommitFiles.getMapAttemptsList.asScala.map(Int.unbox(_)).toArray)

      case COMMIT_FILES_RESPONSE =>
        val pbCommitFilesResponse = PbCommitFilesResponse.parseFrom(message.getPayload)
        CommitFilesResponse(Utils.toStatusCode(pbCommitFilesResponse.getStatus),
          pbCommitFilesResponse.getCommittedMasterIdsList,
          pbCommitFilesResponse.getCommittedSlaveIdsList,
          pbCommitFilesResponse.getFailedMasterIdsList,
          pbCommitFilesResponse.getFailedSlaveIdsList)

      case DESTROY =>
        val pbDestroy = PbDestroy.parseFrom(message.getPayload)
        Destroy(pbDestroy.getShuffleKey, pbDestroy.getMasterLocationsList,
          pbDestroy.getSlaveLocationList)

      case DESTROY_RESPONSE =>
        val pbDestroyResponse = PbDestroyResponse.parseFrom(message.getPayload)
        DestroyResponse(Utils.toStatusCode(pbDestroyResponse.getStatus),
          pbDestroyResponse.getFailedMastersList, pbDestroyResponse.getFailedSlavesList)

      case SLAVE_LOST_RESPONSE =>
        val pbSlaveLostResponse = PbSlaveLostResponse.parseFrom(message.getPayload)
        SlaveLostResponse(Utils.toStatusCode(pbSlaveLostResponse.getStatus),
          PartitionLocation.fromPbPartitionLocation(pbSlaveLostResponse.getSlaveLocation))

      case GET_WORKER_INFO =>
        GetWorkerInfos

      case GET_WORKER_INFO_RESPONSE =>
        val pbGetWorkerInfoResponse = PbGetWorkerInfosResponse.parseFrom(message.getPayload)
        GetWorkerInfosResponse(Utils.toStatusCode(pbGetWorkerInfoResponse.getStatus),
          pbGetWorkerInfoResponse.getWorkerInfosList.asScala
            .map(WorkerInfo.fromPbWorkerInfo(_)).toList: _* )

      case THREAD_DUMP =>
        ThreadDump

      case THREAD_DUMP_RESPONSE =>
        val pbThreadDumpResponse = PbThreadDumpResponse.parseFrom(message.getPayload)
        ThreadDumpResponse(pbThreadDumpResponse.getThreadDump)

      case REMOVE_EXPIRED_SHUFFLE =>
        RemoveExpiredShuffle

      case ONE_WAY_MESSAGE_RESPONSE =>
        OneWayMessageResponse

      case CHECK_FOR_WORKER_TIMEOUT =>
        CheckForWorkerTimeOut

      case CHECK_FOR_APPLICATION_TIMEOUT =>
        CheckForApplicationTimeOut

      case WORKER_LOST =>
        val pbWorkerLost = PbWorkerLost.parseFrom(message.getPayload)
        WorkerLost(pbWorkerLost.getHost, pbWorkerLost.getRpcPort, pbWorkerLost.getPushPort,
          pbWorkerLost.getFetchPort, pbWorkerLost.getRequestId)

      case WORKER_LOST_RESPONSE =>
        val pbWorkerLostResponse = PbWorkerLostResponse.parseFrom(message.getPayload)
        WorkerLostResponse(pbWorkerLostResponse.getSuccess)

      case STAGE_END =>
        val pbStageEnd = PbStageEnd.parseFrom(message.getPayload)
        StageEnd(pbStageEnd.getApplicationId, pbStageEnd.getShuffleId)

      case STAGE_END_RESPONSE =>
        val pbStageEndResponse = PbStageEndResponse.parseFrom(message.getPayload)
        StageEndResponse(Utils.toStatusCode(pbStageEndResponse.getStatus))

      case ONE_WAY_MESSAGE_RESPONSE =>
        OneWayMessageResponse
    }
  }
}
