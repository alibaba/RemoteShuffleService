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

package com.aliyun.emr.rss.service.deploy.master.clustermeta.ha;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.emr.rss.common.RssConf;
import com.aliyun.emr.rss.common.meta.DiskInfo;
import com.aliyun.emr.rss.common.meta.WorkerInfo;
import com.aliyun.emr.rss.service.deploy.master.clustermeta.MetaUtil;
import com.aliyun.emr.rss.service.deploy.master.clustermeta.ResourceProtos;
import com.aliyun.emr.rss.service.deploy.master.clustermeta.ResourceProtos.ResourceResponse;

public class MetaHandler {
  private static final Logger LOG = LoggerFactory.getLogger(MetaHandler.class);

  private final HAMasterMetaManager metaSystem;

  public MetaHandler(HAMasterMetaManager metaSystem) {
    this.metaSystem = metaSystem;
  }

  public void setUpMasterRatisServer(RssConf conf) throws IOException {
    HANodeDetails haNodeDetails = HANodeDetails.loadHAConfig(conf);
    metaSystem.setRatisServer(
        HARaftServer.newMasterRatisServer(
            this, conf,
            haNodeDetails.getLocalNodeDetails(),
            haNodeDetails.getPeerNodeDetails()));
    metaSystem.getRatisServer().start();
  }

  /**
   * Get an initial MasterMetaResponse.Builder with proper request cmdType.
   * @param request MasterMetaRequest.
   * @return MasterMetaResponse builder.
   */
  public static ResourceResponse.Builder getMasterMetaResponseBuilder(
    ResourceProtos.ResourceRequest request) {
    return ResourceResponse.newBuilder()
        .setCmdType(request.getCmdType())
        .setStatus(ResourceProtos.Status.OK)
        .setSuccess(true);
  }

  public ResourceResponse handleReadRequest(
    ResourceProtos.ResourceRequest request) {
    ResourceProtos.Type cmdType = request.getCmdType();
    ResourceResponse.Builder responseBuilder =
        getMasterMetaResponseBuilder(request);
    responseBuilder.setCmdType(cmdType);
    try {
      switch (cmdType) {
        default:
          throw new IOException("Can not parse this command!" + request);
      }
    } catch (IOException e) {
      LOG.warn("Handle meta read request " + cmdType + " failed!", e);
      responseBuilder.setSuccess(false);
      responseBuilder.setStatus(ResourceProtos.Status.INTERNAL_ERROR);
      if (e.getMessage() != null) {
        responseBuilder.setMessage(e.getMessage());
      }
    }
    return responseBuilder.build();
  }

  public ResourceResponse handleWriteRequest(
    ResourceProtos.ResourceRequest request) {
    ResourceProtos.Type cmdType = request.getCmdType();
    ResourceResponse.Builder responseBuilder =
        getMasterMetaResponseBuilder(request);
    responseBuilder.setCmdType(cmdType);
    try {
      String shuffleKey;
      String appId;
      String host;
      int rpcPort;
      int pushPort;
      int fetchPort;
      int replicatePort;
      Map<String, DiskInfo> disks;
      List<Map<String, Integer>> slots = new ArrayList<>();
      Map<String, Map<String, Integer>> workerAllocations= new HashMap<>();
      switch (cmdType) {
        case RequestSlots:
          shuffleKey = request.getRequestSlotsRequest().getShuffleKey();
          request.getRequestSlotsRequest().getWorkerAllocationsMap().forEach((k, v) -> {
            workerAllocations.put(k, new HashMap<>(v.getSlotMap()));
          });
          LOG.debug("Handle request slots for {}", shuffleKey);
          metaSystem.updateRequestSlotsMeta(shuffleKey,
              request.getRequestSlotsRequest().getHostName(), workerAllocations);
          break;

        case ReleaseSlots:
          for (ResourceProtos.SlotInfo
                   pbSlotInfo : request.getReleaseSlotsRequest().getSlotsList()) {
            slots.add(pbSlotInfo.getSlotMap());
          }

          shuffleKey = request.getReleaseSlotsRequest().getShuffleKey();
          LOG.debug("Handle release slots for {}", shuffleKey);
          metaSystem.updateReleaseSlotsMeta(shuffleKey,
              request.getReleaseSlotsRequest().getWorkerIdsList(), slots);
          break;

        case UnRegisterShuffle:
          shuffleKey = request.getUnregisterShuffleRequest().getShuffleKey();
          LOG.debug("Handle unregister shuffle for {}", shuffleKey);
          metaSystem.updateUnregisterShuffleMeta(shuffleKey);
          break;

        case AppHeartBeat:
          appId = request.getAppHeartbeatRequest().getAppId();
          LOG.debug("Handle app heartbeat for {}", appId);
          long time = request.getAppHeartbeatRequest().getTime();
          metaSystem.updateAppHeartBeatMeta(appId, time);
          break;

        case AppLost:
          appId = request.getAppLostRequest().getAppId();
          LOG.debug("Handle app lost for {}", appId);
          metaSystem.updateAppLostMeta(appId);
          break;

        case WorkerLost:
          host = request.getWorkerLostRequest().getHost();
          rpcPort = request.getWorkerLostRequest().getRpcPort();
          pushPort = request.getWorkerLostRequest().getPushPort();
          fetchPort = request.getWorkerLostRequest().getFetchPort();
          replicatePort = request.getWorkerLostRequest().getReplicatePort();
          LOG.debug("Handle worker lost for {} {}", host, pushPort);
          metaSystem.updateWorkerLostMeta(host, rpcPort, pushPort, fetchPort, replicatePort);
          break;

        case WorkerHeartBeat:
          host = request.getWorkerHeartBeatRequest().getHost();
          rpcPort = request.getWorkerHeartBeatRequest().getRpcPort();
          pushPort = request.getWorkerHeartBeatRequest().getPushPort();
          fetchPort = request.getWorkerHeartBeatRequest().getFetchPort();
          disks = MetaUtil.fromPbDiskInfos(request.getWorkerHeartBeatRequest().getDisksMap());
          replicatePort = request.getWorkerHeartBeatRequest().getReplicatePort();
          LOG.debug("Handle worker heartbeat for {} {} {} {} {} {}",
            host, rpcPort, pushPort, fetchPort, replicatePort, disks);
          time = request.getWorkerHeartBeatRequest().getTime();
          metaSystem.updateWorkerHeartBeatMeta(host, rpcPort, pushPort, fetchPort, replicatePort,
            disks, time);
          break;

        case RegisterWorker:
          host = request.getRegisterWorkerRequest().getHost();
          rpcPort = request.getRegisterWorkerRequest().getRpcPort();
          pushPort = request.getRegisterWorkerRequest().getPushPort();
          fetchPort = request.getRegisterWorkerRequest().getFetchPort();
          replicatePort = request.getRegisterWorkerRequest().getReplicatePort();
          disks = MetaUtil.fromPbDiskInfos(request.getRegisterWorkerRequest().getDisksMap());
          LOG.debug("Handle worker register for {} {} {} {} {} {}",
            host, rpcPort, pushPort, fetchPort, replicatePort, disks);
          metaSystem.updateRegisterWorkerMeta(host, rpcPort, pushPort, fetchPort, replicatePort,
            disks);
          break;

        case ReportWorkerFailure:
          List<ResourceProtos.WorkerAddress> failedAddress = request
                  .getReportWorkerFailureRequest().getFailedWorkerList();
          List<WorkerInfo> failedWorkers= failedAddress.stream()
                  .map(MetaUtil::addrToInfo).collect(Collectors.toList());
          metaSystem.updateBlacklistByReportWorkerFailure(failedWorkers);
          break;

        default:
          throw new IOException("Can not parse this command!" + request);
      }
      responseBuilder.setStatus(ResourceProtos.Status.OK);
    } catch (IOException e) {
      LOG.warn("Handle meta write request " + cmdType + " failed!", e);
      responseBuilder.setSuccess(false);
      responseBuilder.setStatus(ResourceProtos.Status.INTERNAL_ERROR);
      if (e.getMessage() != null) {
        responseBuilder.setMessage(e.getMessage());
      }
    }
    return responseBuilder.build();
  }

  public void writeToSnapShot(File file) throws IOException {
    try {
      metaSystem.writeMetaInfoToFile(file);
    } catch (RuntimeException e) {
      throw new IOException(e.getCause());
    }
  }

  public void loadSnapShot(File file) throws IOException {
    try {
      metaSystem.restoreMetaFromFile(file);
    } catch (RuntimeException e) {
      throw new IOException(e.getCause());
    }
  }
}
