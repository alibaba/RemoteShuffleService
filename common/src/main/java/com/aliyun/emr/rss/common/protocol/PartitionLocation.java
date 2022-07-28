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

package com.aliyun.emr.rss.common.protocol;

import java.io.Serializable;

import com.aliyun.emr.rss.common.meta.WorkerInfo;
import com.aliyun.emr.rss.common.protocol.TransportMessages.PbPartitionLocation;

public class PartitionLocation implements Serializable {
  public enum Mode {
    Master(0), Slave(1);

    private final byte mode;

    Mode(int id) {
      assert id < 128 : "Cannot have more than 128 message types";
      this.mode = (byte) id;
    }

    public byte mode() { return mode; }
  }

  public static PartitionLocation.Mode getMode(byte mode) {
    if (mode == 0) {
      return Mode.Master;
    } else {
      return Mode.Slave;
    }
  }

  private int id;
  private int epoch;
  private String host;
  private int rpcPort;
  private int pushPort;
  private int fetchPort;
  private int replicatePort;
  private Mode mode;
  private PartitionLocation peer;
  private StorageHint storageHint;

  public PartitionLocation(PartitionLocation loc) {
    this.id = loc.id;
    this.epoch = loc.epoch;
    this.host = loc.host;
    this.rpcPort = loc.rpcPort;
    this.pushPort = loc.pushPort;
    this.fetchPort = loc.fetchPort;
    this.replicatePort = loc.replicatePort;
    this.mode = loc.mode;
    this.peer = loc.peer;
    this.storageHint = loc.storageHint;
  }

  public PartitionLocation(
    int id,
    int epoch,
    String host,
    int rpcPort,
    int pushPort,
    int fetchPort,
    int replicatePort,
    Mode mode) {
    this(id, epoch, host, rpcPort, pushPort, fetchPort, replicatePort,
      mode, null, new StorageHint());
  }

  public PartitionLocation(
    int id,
    int epoch,
    String host,
    int rpcPort,
    int pushPort,
    int fetchPort,
    int replicatePort,
    Mode mode,
    PartitionLocation peer) {
    this(id, epoch, host, rpcPort, pushPort, fetchPort, replicatePort, mode, peer,
      new StorageHint());
  }

  public PartitionLocation(
    int id,
    int epoch,
    String host,
    int rpcPort,
    int pushPort,
    int fetchPort,
    int replicatePort,
    Mode mode,
    PartitionLocation peer,
    StorageHint hint) {
    this.id = id;
    this.epoch = epoch;
    this.host = host;
    this.rpcPort = rpcPort;
    this.pushPort = pushPort;
    this.fetchPort = fetchPort;
    this.replicatePort = replicatePort;
    this.mode = mode;
    this.peer = peer;
    this.storageHint = hint;
  }

  public int getId()
  {
    return id;
  }

  public void setId(int id)
  {
    this.id = id;
  }

  public int getEpoch() {
    return epoch;
  }

  public void setEpoch(int epoch) {
    this.epoch = epoch;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public int getPushPort() {
    return pushPort;
  }

  public void setPushPort(int pushPort) {
    this.pushPort = pushPort;
  }

  public int getFetchPort() {
    return fetchPort;
  }

  public void setFetchPort(int fetchPort) {
    this.fetchPort = fetchPort;
  }

  public String hostAndPorts() {
    return host + ":" + rpcPort + ":" + pushPort + ":" + fetchPort;
  }

  public String hostAndPushPort() {
    return host+":"+pushPort;
  }

  public Mode getMode() {
    return mode;
  }

  public void setMode(Mode mode) {
    this.mode = mode;
  }

  public PartitionLocation getPeer() {
    return peer;
  }

  public void setPeer(PartitionLocation peer) {
    this.peer = peer;
  }

  public String getUniqueId() {
    return id + "-" + epoch;
  }

  public String getFileName() {
    return id + "-" + epoch + "-" + mode.mode;
  }

  public int getRpcPort() {
    return rpcPort;
  }

  public void setRpcPort(int rpcPort) {
    this.rpcPort = rpcPort;
  }

  public int getReplicatePort() {
    return replicatePort;
  }

  public void setReplicatePort(int replicatePort) {
    this.replicatePort = replicatePort;
  }

  public StorageHint getStorageHint() {
    return storageHint;
  }

  public void setStorageHint(StorageHint storageHint) {
    this.storageHint = storageHint;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof PartitionLocation)) {
      return false;
    }
    PartitionLocation o = (PartitionLocation) other;
    return id == o.id
        && epoch == o.epoch
        && host.equals(o.host)
        && rpcPort == o.rpcPort
        && pushPort == o.pushPort
        && fetchPort == o.fetchPort;
  }

  @Override
  public int hashCode() {
    return (id + epoch + host + rpcPort + pushPort + fetchPort).hashCode();
  }

  @Override
  public String toString() {
    String peerAddr = "";
    if (peer != null) {
      peerAddr = peer.hostAndPorts();
    }
    return "PartitionLocation[" + id + "-" + epoch + " " + host + ":" + rpcPort + ":" +
             pushPort + ":" + fetchPort + ":" + replicatePort + " Mode: " + mode +
             " peer: " + peerAddr + " storage hint:" + storageHint + "]";
  }

  public WorkerInfo getWorker() {
    return new WorkerInfo(host, rpcPort, pushPort, fetchPort, replicatePort);
  }

  public static PartitionLocation fromPbPartitionLocation(PbPartitionLocation pbLoc) {
    Mode mode = Mode.Master;
    if (pbLoc.getMode() == PbPartitionLocation.Mode.Slave) {
      mode = Mode.Slave;
    }

    PartitionLocation partitionLocation =
        new PartitionLocation(
            pbLoc.getId(),
            pbLoc.getEpoch(),
            pbLoc.getHost(),
            pbLoc.getRpcPort(),
            pbLoc.getPushPort(),
            pbLoc.getFetchPort(),
            pbLoc.getReplicatePort(),
            mode,
            null,
            StorageHint.fromPb(pbLoc.getStorageHint()));
    if (pbLoc.hasPeer()) {
      PbPartitionLocation peerPb = pbLoc.getPeer();
      Mode peerMode = Mode.Master;
      if (peerPb.getMode() == PbPartitionLocation.Mode.Slave) {
        peerMode = Mode.Slave;
      }
      PartitionLocation peerLocation = new PartitionLocation(
          peerPb.getId(),
          peerPb.getEpoch(),
          peerPb.getHost(),
          peerPb.getRpcPort(),
          peerPb.getPushPort(),
          peerPb.getFetchPort(),
          peerPb.getReplicatePort(),
          peerMode,
          partitionLocation,
          StorageHint.fromPb(peerPb.getStorageHint()));
      partitionLocation.setPeer(peerLocation);
    }

    return partitionLocation;
  }

  public static PbPartitionLocation toPbPartitionLocation(PartitionLocation location) {
    PbPartitionLocation.Builder builder = TransportMessages
      .PbPartitionLocation.newBuilder();
    if (location.mode == Mode.Master) {
      builder.setMode(PbPartitionLocation.Mode.Master);
    } else {
      builder.setMode(PbPartitionLocation.Mode.Slave);
    }
    builder.setHost(location.getHost());
    builder.setEpoch(location.getEpoch());
    builder.setId(location.getId());
    builder.setRpcPort(location.getRpcPort());
    builder.setPushPort(location.getPushPort());
    builder.setFetchPort(location.getFetchPort());
    builder.setReplicatePort(location.getReplicatePort());
    builder.setStorageHint(StorageHint.toPb(location.storageHint));

    if (location.getPeer() != null) {
      PbPartitionLocation.Builder peerBuilder = TransportMessages
        .PbPartitionLocation.newBuilder();
      if (location.getPeer().mode == Mode.Master) {
        peerBuilder.setMode(PbPartitionLocation.Mode.Master);
      } else {
        peerBuilder.setMode(PbPartitionLocation.Mode.Slave);
      }
      peerBuilder.setHost(location.getPeer().getHost());
      peerBuilder.setEpoch(location.getPeer().getEpoch());
      peerBuilder.setId(location.getPeer().getId());
      peerBuilder.setRpcPort(location.getPeer().getRpcPort());
      peerBuilder.setPushPort(location.getPeer().getPushPort());
      peerBuilder.setFetchPort(location.getPeer().getFetchPort());
      peerBuilder.setReplicatePort(location.getPeer().getReplicatePort());
      peerBuilder.setStorageHint(StorageHint.toPb(location.getPeer().getStorageHint()));
      builder.setPeer(peerBuilder.build());
    }

    return builder.build();
  }
}
