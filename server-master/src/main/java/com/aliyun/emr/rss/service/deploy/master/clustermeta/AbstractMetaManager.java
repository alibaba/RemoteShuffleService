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

package com.aliyun.emr.rss.service.deploy.master.clustermeta;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import io.netty.util.internal.ConcurrentSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.emr.rss.common.RssConf;
import com.aliyun.emr.rss.common.meta.WorkerInfo;
import com.aliyun.emr.rss.common.rpc.RpcAddress;
import com.aliyun.emr.rss.common.rpc.RpcEnv;
import com.aliyun.emr.rss.common.util.Utils;
import static com.aliyun.emr.rss.common.protocol.RpcNameConstants.WORKER_EP;

public abstract class AbstractMetaManager implements IMetadataHandler {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractMetaManager.class);

  // Meta data for master service
  public final ConcurrentSet<String> registeredShuffle = new ConcurrentSet<>();
  public final ConcurrentSet<String> hostnameSet = new ConcurrentSet<>();
  public final ArrayList<WorkerInfo> workers = new ArrayList<>();
  public final ConcurrentHashMap<String, Long> appHeartbeatTime = new ConcurrentHashMap<>();
  // blacklist
  public final ConcurrentSet<WorkerInfo> blacklist = new ConcurrentSet<>();
  // workerLost events
  public final ConcurrentSet<WorkerInfo> workerLostEvents = new ConcurrentSet<>();

  protected RpcEnv rpcEnv;
  protected RssConf conf;

  public void updateRequestSlotsMeta(
      String shuffleKey, String hostName, List<String> workerInfos) {
    registeredShuffle.add(shuffleKey);

    String appId = Utils.splitShuffleKey(shuffleKey)._1;
    appHeartbeatTime.compute(appId, (applicationId, oldTimestamp) -> {
      long oldTime = System.currentTimeMillis();
      if (oldTimestamp != null) {
        oldTime = oldTimestamp;
      }
      return Math.max(System.currentTimeMillis(), oldTime);
    });

    if (hostName != null) {
      hostnameSet.add(hostName);
    }
    if (workerInfos != null) {
      synchronized (workers) {
        HashMap<WorkerInfo, Integer> allocatedMap = WorkerInfo.decodeFromPbMessage(workerInfos);
        workers.forEach(workerInfo -> {
          if (allocatedMap.containsKey(workerInfo)) {
            workerInfo.allocateSlots(shuffleKey, allocatedMap.get(workerInfo));
          }
        });
      }
    }
  }

  public void updateReleaseSlotsMeta(String shuffleKey) {
    updateReleaseSlotsMeta(shuffleKey, null, null);
  }

  public void updateReleaseSlotsMeta(String shuffleKey, List<String> workerIds,
                                     List<Integer> slots) {
    if (workerIds != null && !workerIds.isEmpty()) {
      for (int i = 0; i < workerIds.size(); i++) {
        WorkerInfo worker = WorkerInfo.fromUniqueId(workerIds.get(i));
        for (WorkerInfo w : workers) {
          if (w.equals(worker))  {
            LOG.info("release slots for worker " + w + ", to release: " + slots.get(i));
            w.releaseSlots(shuffleKey, slots.get(i));
          }
        }
      }
    } else {
      workers.forEach(workerInfo -> workerInfo.releaseSlots(shuffleKey));
    }
  }

  public void updateUnregisterShuffleMeta(String shuffleKey) {
    registeredShuffle.remove(shuffleKey);
  }

  public void updateAppHeartBeatMeta(String appId, long time) {
    appHeartbeatTime.put(appId, time);
  }

  public void updateAppLostMeta(String appId) {
    registeredShuffle.stream()
        .filter(shuffle -> shuffle.startsWith(appId))
        .forEach(this::updateReleaseSlotsMeta);
    registeredShuffle.removeIf(shuffleKey -> shuffleKey.startsWith(appId));
    appHeartbeatTime.remove(appId);
  }

  public void updateWorkerLostMeta(String host,int rpcPort, int pushPort,int fetchPort,
    int replicatePort) {
    WorkerInfo worker = new WorkerInfo(host, rpcPort, pushPort,
            fetchPort, replicatePort, -1, null);
    workerLostEvents.add(worker);
    // remove worker from workers
    synchronized (workers) {
      workers.remove(worker);
    }
    // delete from blacklist
    blacklist.remove(worker);
    workerLostEvents.remove(worker);
  }

  public void updateWorkerHeartBeatMeta(String host, int rpcPort, int pushPort, int fetchPort,
    int replicatePort, int numSlots, long time) {
    WorkerInfo worker = new WorkerInfo(host, rpcPort, pushPort, fetchPort, replicatePort, numSlots,
      null);
    synchronized (workers) {
      Optional<WorkerInfo> workerInfo = workers.stream().filter(w -> w.equals(worker)).findFirst();
      workerInfo.ifPresent(info -> {
        info.lastHeartbeat_$eq(time);
        info.setNumSlots(numSlots);
      });
    }
    if (numSlots == 0 && !blacklist.contains(worker)) {
      LOG.warn("Worker: {} num total slots is 0, add to blacklist", worker.toString());
      blacklist.add(worker);
    } else if (numSlots > 0) {
      // only unblock if numSlots larger than 0
      blacklist.remove(worker);
    }
  }

  public void updateRegisterWorkerMeta(
      String host, int rpcPort, int pushPort, int fetchPort, int replicatePort, int numSlots) {
    WorkerInfo workerInfo = new WorkerInfo(host, rpcPort, pushPort, fetchPort, replicatePort,
      numSlots, null);
    workerInfo.lastHeartbeat_$eq(System.currentTimeMillis());

    try {
      workerInfo.setupEndpoint(rpcEnv.setupEndpointRef(RpcAddress.apply(host, rpcPort), WORKER_EP));
    } catch (Exception e) {
      LOG.error("Worker register failed , {}", e);
      return;
    }

    synchronized (workers) {
      workers.add(workerInfo);
    }
  }

  /**
   * Used for ratis state machine to take snapshot
   * @param file
   * @throws IOException
   */
  public void writeMetaInfoToFile(File file) throws IOException, RuntimeException {
    ObjectOutputStream out = new ObjectOutputStream(
        new BufferedOutputStream(new FileOutputStream(file)));

    // write registeredShuffle
    writeSetMetaToFile(registeredShuffle, out);

    // write hostnameSet
    writeSetMetaToFile(hostnameSet, out);

    // write blacklist
    writeSetMetaToFile(blacklist, out);

    // write workerLost events
    writeSetMetaToFile(workerLostEvents, out);

    // write application heartbeat time
    out.writeInt(appHeartbeatTime.size());
    appHeartbeatTime.forEach((app, time) -> {
      try {
        out.writeObject(app);
        out.writeLong(time);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    // write workerInfo
    out.writeInt(workers.size());
    workers.forEach(workerInfo -> {
      try {
        out.writeObject(workerInfo);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    out.flush();
  }

  private <T> void writeSetMetaToFile(
      ConcurrentSet<T> metas, ObjectOutputStream out) throws IOException {
    out.writeInt(metas.size());
    metas.forEach(meta -> {
      try {
        out.writeObject(meta);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  /**
   * Used for ratis state machine to load snapshot
   * @param file
   * @throws IOException
   */
  public void restoreMetaFromFile(File file) throws IOException {
    try (ObjectInputStream in = new ObjectInputStream(
      new BufferedInputStream(new FileInputStream(file)))) {
      // read registeredShuffle
      readSetMetaFromFile(registeredShuffle, in.readInt(), in);

      // read hostnameSet
      readSetMetaFromFile(hostnameSet, in.readInt(), in);

      // read blacklist
      readSetMetaFromFile(blacklist, in.readInt(), in);

      // read workerLost events
      readSetMetaFromFile(workerLostEvents, in.readInt(), in);

      // read application heartbeat time
      int size = in.readInt();
      for (int i = 0 ; i < size ; ++i) {
        appHeartbeatTime.put((String) in.readObject(), in.readLong());
      }
      registeredShuffle.forEach(shuffleKey -> {
        String appId = shuffleKey.split("-")[0];
        if (!appHeartbeatTime.containsKey(appId)) {
          appHeartbeatTime.put(appId, System.currentTimeMillis());
        }
      });

      // read workerInfo
      int workersSize = in.readInt();
      for (int i = 0 ; i < workersSize ; ++i) {
        workers.add((WorkerInfo) in.readObject());
      }
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }
    LOG.info("Successfully restore meta info from snapshot " + file.getAbsolutePath());
    LOG.info("Worker size: {}, Registered shuffle size: {}, Worker blacklist size: {}.",
        workers.size(), registeredShuffle.size(), blacklist.size());
    workers.forEach(workerInfo -> LOG.info(workerInfo.toString()));
    registeredShuffle.forEach(shuffle -> LOG.info("RegisteredShuffle " + shuffle));
  }

  private <T> void readSetMetaFromFile(
      ConcurrentSet<T> metas,
      int size,
      ObjectInputStream in) throws ClassNotFoundException, IOException {
    for (int i = 0; i < size; ++i) {
      metas.add((T) in.readObject());
    }
  }

  public void updateBlacklistByReportWorkerFailure(List<WorkerInfo> failedWorkers) {
    synchronized (this.workers) {
      failedWorkers.retainAll(this.workers);
      this.blacklist.addAll(failedWorkers);
    }
  }
}
