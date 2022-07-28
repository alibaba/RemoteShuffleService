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

package com.aliyun.emr.rss.service.deploy.master;

import java.util.*;

import scala.Tuple2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.emr.rss.common.meta.DiskInfo;
import com.aliyun.emr.rss.common.meta.WorkerInfo;
import com.aliyun.emr.rss.common.protocol.PartitionLocation;
import com.aliyun.emr.rss.common.protocol.StorageHint;

public class MasterUtil {

  private static Logger logger = LoggerFactory.getLogger(MasterUtil.class);
  private static final Random rand = new Random();
  private static double top30Ratio = 0.39;
  private static double mid30Ratio = 0.32;
  private static double last40Ratio = 0.29;

  public static Map<WorkerInfo, Map<String, Integer>> workerToAllocatedSlots(
    Map<WorkerInfo, Tuple2<List<PartitionLocation>, List<PartitionLocation>>> slots) {
    Iterator<WorkerInfo> workers = slots.keySet().iterator();
    Map<WorkerInfo, Map<String, Integer>> workerToSlots = new HashMap<>();
    while (workers.hasNext()) {
      WorkerInfo worker = workers.next();
      workerToSlots.compute(worker, (k, v) -> {
        if (v == null) {
          v = new HashMap<>();
        }
        for (PartitionLocation location : slots.get(worker)._1) {
          v.compute(location.getStorageHint().getMountPoint(), (hint, slot) -> {
            if (slot == null) {
              slot = 0;
            }
            slot = slot + 1;
            return slot;
          });
        }
        for (PartitionLocation location : slots.get(worker)._2) {
          v.compute(location.getStorageHint().getMountPoint(), (hint, slot) -> {
            if (slot == null) {
              slot = 0;
            }
            slot = slot + 1;
            return slot;
          });
        }
        return v;
      });
    }
    return workerToSlots;
  }

  public static Map<WorkerInfo, Tuple2<List<PartitionLocation>, List<PartitionLocation>>>
    offerSlotsV1(
      List<WorkerInfo> workers,
      List<Integer> partitionIds,
      boolean shouldReplicate) {
    int[] oldEpochs = new int[partitionIds.size()];
    Arrays.fill(oldEpochs, -1);
    if (workers.size() < 2 && shouldReplicate) {
      return null;
    }

    int masterInd = rand.nextInt(workers.size());
    Map<WorkerInfo, Tuple2<List<PartitionLocation>, List<PartitionLocation>>> slots =
        new HashMap<>();
    // foreach iteration, allocate both master and slave partitions
    for(int idx = 0; idx < partitionIds.size(); idx++) {
      int nextMasterInd = masterInd;
      // try to find slot for master partition
      while (!workers.get(nextMasterInd).slotAvailable()) {
        nextMasterInd = (nextMasterInd + 1) % workers.size();
        if (nextMasterInd == masterInd) {
          return null;
        }
      }
      int nextSlaveInd = 0;
      if (shouldReplicate) {
        // try to find slot for slave partition
        nextSlaveInd = (nextMasterInd + 1) % workers.size();
        while (!workers.get(nextSlaveInd).slotAvailable()) {
          nextSlaveInd = (nextSlaveInd + 1) % workers.size();
          if (nextSlaveInd == nextMasterInd) {
            return null;
          }
        }
        if (nextSlaveInd == nextMasterInd) {
          return null;
        }
      }
      // now nextMasterInd/nextSlaveInd point to
      // available master/slave partition respectively

      int newEpoch = oldEpochs[idx] + 1;
      // new slave and master locations
      slots.putIfAbsent(workers.get(nextMasterInd),
          new Tuple2<>(new ArrayList<>(), new ArrayList<>()));
      Tuple2<List<PartitionLocation>, List<PartitionLocation>> locations =
          slots.get(workers.get(nextMasterInd));
      PartitionLocation slaveLocation = null;
      PartitionLocation masterLocation = null;
      if (shouldReplicate) {
        slaveLocation = new PartitionLocation(
            partitionIds.get(idx),
            newEpoch,
            workers.get(nextSlaveInd).host(),
            workers.get(nextSlaveInd).rpcPort(),
            workers.get(nextSlaveInd).pushPort(),
            workers.get(nextSlaveInd).fetchPort(),
            workers.get(nextSlaveInd).replicatePort(),
            PartitionLocation.Mode.Slave
        );
      }
      masterLocation = new PartitionLocation(
          partitionIds.get(idx),
          newEpoch,
          workers.get(nextMasterInd).host(),
          workers.get(nextMasterInd).rpcPort(),
          workers.get(nextMasterInd).pushPort(),
          workers.get(nextMasterInd).fetchPort(),
          workers.get(nextMasterInd).replicatePort(),
          PartitionLocation.Mode.Master,
          slaveLocation
      );
      if (shouldReplicate) {
        slaveLocation.setPeer(masterLocation);
      }

      // add master location to WorkerInfo
      locations._1.add(masterLocation);

      if (shouldReplicate) {
        // add slave location to WorkerInfo
        slots.putIfAbsent(workers.get(nextSlaveInd),
            new Tuple2<>(new ArrayList<>(), new ArrayList<>()));
        locations = slots.get(workers.get(nextSlaveInd));
        locations._2.add(slaveLocation);
      }

      // update index
      masterInd = (nextMasterInd + 1) % workers.size();
    }
    return slots;
  }

  /**
   * It assumes that all disks whose available space is greater than the minimum space are divided
   * into three groups(top30,mid30,last40) by their average flush time.
   * Slots will be distributed to faster groups than the slower group.
   * A faster group will distribute 20 percent more workloads than a slower one.
   * disk group workload distribution ratio:
   * top30: mid30: last40
   * 1.44: 1.2: 1
   *
   *
   * @param workers
   * @param partitionIds
   * @param shouldReplicate
   * @param minimumUsableSize
   * @return
   */
  public static Map<WorkerInfo, Tuple2<List<PartitionLocation>, List<PartitionLocation>>>
    offerSlotsV2(
      List<WorkerInfo> workers,
      List<Integer> partitionIds,
      boolean shouldReplicate,
      long minimumUsableSize) {
    if (partitionIds.isEmpty()) {
      return new HashMap<>();
    }
    int[] oldEpochs = new int[partitionIds.size()];
    Arrays.fill(oldEpochs, -1);

    if (workers.size() < 2 && shouldReplicate) {
      return null;
    }

    Map<DiskInfo, WorkerInfo> diskToWorkerMap = new HashMap<>();
    Map<DiskInfo, Long> activeDiskSlots = new HashMap<>();
    List<DiskInfo> disks = new ArrayList<>();
    workers.forEach(i -> i.disks().entrySet().forEach(entry -> {
      DiskInfo diskInfo = entry.getValue();
      diskToWorkerMap.put(diskInfo, i);
      disks.add(diskInfo);
      activeDiskSlots.put(diskInfo, diskInfo.activeSlots());
    }));

    List<DiskInfo> usableDisks = new ArrayList<>();
    for (DiskInfo disk : disks) {
      if (disk.usableSpace() > minimumUsableSize) {
        usableDisks.add(disk);
      }
    }

    Collections.sort(usableDisks, (o1, o2) -> {
      if (o1.avgFlushTime() <= o2.avgFlushTime()) {
        return -1;
      } else {
        return 1;
      }
    });

    List<PartitionLocation> masterLocations = new ArrayList<>();
    Map<WorkerInfo, List<PartitionLocation>> masterAggregatedLocations = new HashMap<>();

    List<PartitionLocation> slaveLocations = new ArrayList<>();
    Map<WorkerInfo, List<PartitionLocation>> slaveAggregatedLocations = new HashMap<>();

    allocatePartitionByDiskSpeed(
        usableDisks,
        partitionIds,
        masterLocations,
        masterAggregatedLocations,
        oldEpochs,
        diskToWorkerMap,
        workers,
        activeDiskSlots,
        true);

    if (shouldReplicate) {
      for (Map.Entry<WorkerInfo, List<PartitionLocation>>
               entry : masterAggregatedLocations.entrySet()) {
        WorkerInfo workerToExclude = entry.getKey();
        List<PartitionLocation> locations = entry.getValue();
        Map<Integer, Integer> reducerIdToEpoch = new HashMap<>();
        for (PartitionLocation location : locations) {
          reducerIdToEpoch.put(location.getId(), location.getEpoch());
        }

        List<DiskInfo> remainDisks = new ArrayList<>(disks);
        remainDisks.removeAll(workerToExclude.disks().values());
        List<WorkerInfo> remainWorkers = new ArrayList<>(workers);
        remainWorkers.remove(workerToExclude);

        allocatePartitionByDiskSpeed(
            remainDisks,
            new ArrayList<>(reducerIdToEpoch.keySet()),
            slaveLocations,
            slaveAggregatedLocations,
            oldEpochs,
            diskToWorkerMap,
            remainWorkers,
            activeDiskSlots,
            false);
      }
    }

    Map<Integer, PartitionLocation> reduceIdToMasterLocations = new HashMap<>();
    for (PartitionLocation location : masterLocations) {
      reduceIdToMasterLocations.put(location.getId(), location);
    }

    if (shouldReplicate) {
      Map<Integer, PartitionLocation> reduceIdToSlaveLocations = new HashMap<>();
      for (PartitionLocation location : slaveLocations) {
        reduceIdToSlaveLocations.put(location.getId(), location);
      }
      for (Map.Entry<Integer, PartitionLocation> entry : reduceIdToMasterLocations.entrySet()) {
        Integer currentReduceId = entry.getKey();
        PartitionLocation masterLocation = reduceIdToMasterLocations.get(currentReduceId);
        PartitionLocation slaveLocation = reduceIdToSlaveLocations.get(currentReduceId);
        masterLocation.setPeer(slaveLocation);
        slaveLocation.setPeer(masterLocation);
      }
    }

    Map<WorkerInfo, Tuple2<List<PartitionLocation>, List<PartitionLocation>>> slots =
        new HashMap<>();

    Set<WorkerInfo> masterOnlySet = new HashSet<>(masterAggregatedLocations.keySet());
    Set<WorkerInfo> slaveOnlySet = new HashSet<>(slaveAggregatedLocations.keySet());
    Set<WorkerInfo> commonSet = new HashSet<>(masterAggregatedLocations.keySet());
    commonSet.retainAll(slaveOnlySet);
    masterOnlySet.removeAll(commonSet);
    slaveOnlySet.removeAll(commonSet);
    for (WorkerInfo workerInfo : commonSet) {
      slots.put(workerInfo, new Tuple2<>(masterAggregatedLocations.get(workerInfo),
          slaveAggregatedLocations.get(workerInfo)));
    }
    for (WorkerInfo workerInfo : masterOnlySet) {
      slots.put(workerInfo, new Tuple2<>(masterAggregatedLocations.get(workerInfo),
          new ArrayList<>()));
    }
    for (WorkerInfo workerInfo : slaveOnlySet) {
      slots.put(workerInfo, new Tuple2<>(new ArrayList<>(),
          slaveAggregatedLocations.get(workerInfo)));
    }

    return slots;
  }

  private static void allocatePartitionByDiskSpeed(
    List<DiskInfo> disks,
    List<Integer> reduceIds,
    List<PartitionLocation> locations,
    Map<WorkerInfo, List<PartitionLocation>> workerAggregatedLocations,
    int[] oldEpochs,
    Map<DiskInfo, WorkerInfo> diskToWorkerMap,
    List<WorkerInfo> workers,
    Map<DiskInfo, Long> activeDiskSlots,
    boolean isMaster) {
    List<Integer> nReduceIds = new ArrayList<>(reduceIds);
    int thirtyPerDiskCount = (int) Math.ceil(disks.size() * 0.3);
    List<DiskInfo> top30 = disks.subList(0, thirtyPerDiskCount);
    List<DiskInfo> mid30 = disks.subList(thirtyPerDiskCount, thirtyPerDiskCount * 2);
    List<DiskInfo> last40 = disks.subList(thirtyPerDiskCount * 2, disks.size());

    long top30Total = top30.stream()
                      .map(disk -> disk.availableSlots() - activeDiskSlots.get(disk))
                      .mapToLong(l -> l).sum();
    long mid30Total = mid30.stream()
                      .map(disk -> disk.availableSlots() - activeDiskSlots.get(disk))
                      .mapToLong(l -> l).sum();
    long last40Total = last40.stream()
                       .map(disk -> disk.availableSlots() - activeDiskSlots.get(disk))
                       .mapToLong(l -> l).sum();

    int requestTotalSlots = reduceIds.size();
    int top30Required = (int) Math.ceil(requestTotalSlots * top30Ratio);
    int mid30Required = (int) Math.ceil(requestTotalSlots * mid30Ratio);
    int last40Required = (int) Math.ceil(requestTotalSlots * last40Ratio);

    int topToMid = 0;
    int midToLast = 0;

    int top30ToAllocate = 0;
    int mid30ToAllocate = 0;
    int last40ToAllocate = 0;
    int roundRobinToAllocate = 0;

    if (top30Required > top30Total) {
      topToMid = (int) (top30Required - top30Total);
    }
    if (mid30Required + topToMid > mid30Total) {
      midToLast = (int) (mid30Required + topToMid - mid30Total);
    }
    if (last40Required + midToLast > last40Total) {
      roundRobinToAllocate = (int) (last40Required + midToLast - last40Total);
    }

    top30ToAllocate = top30Required - topToMid;
    if (requestTotalSlots > top30ToAllocate) {
      requestTotalSlots -= top30ToAllocate;
    } else {
      top30ToAllocate = requestTotalSlots;
      requestTotalSlots = 0;
    }
    if (requestTotalSlots > 0) {
      mid30ToAllocate = mid30Required + topToMid - midToLast;
      if (requestTotalSlots > mid30ToAllocate) {
        requestTotalSlots -= mid30ToAllocate;
      } else {
        mid30ToAllocate = requestTotalSlots;
        requestTotalSlots = 0;
      }
    }
    if (requestTotalSlots > 0) {
      last40ToAllocate = last40Required + midToLast - roundRobinToAllocate;
      if (requestTotalSlots > last40ToAllocate) {
        requestTotalSlots -= last40ToAllocate;
      } else {
        last40ToAllocate = requestTotalSlots;
        requestTotalSlots = 0;
      }
    }
    if (requestTotalSlots > 0) {
      roundRobinToAllocate = requestTotalSlots;
      requestTotalSlots = 0;
    } else {
      roundRobinToAllocate = 0;
    }

    logger.info("allocation total {} top30 {} mid30 {} last40 {} roundrobin {}",
      reduceIds.size(), top30ToAllocate, mid30ToAllocate, last40ToAllocate, roundRobinToAllocate);

    if (top30ToAllocate > 0) {
      int[] top30Allocations = getAllocationsBySlots(top30ToAllocate, top30, activeDiskSlots);
      allocateLocations(
        top30Allocations,
        top30,
        locations,
        isMaster,
        diskToWorkerMap,
        oldEpochs,
        nReduceIds,
        workerAggregatedLocations,
        activeDiskSlots);
    }

    if (mid30ToAllocate > 0) {
      int[] mid30Allocations = getAllocationsBySlots(mid30ToAllocate, mid30, activeDiskSlots);
      allocateLocations(
        mid30Allocations,
        mid30,
        locations,
        isMaster,
        diskToWorkerMap,
        oldEpochs,
        nReduceIds,
        workerAggregatedLocations,
        activeDiskSlots);
    }

    if (last40ToAllocate > 0) {
      int[] last40Allocations = getAllocationsBySlots(last40ToAllocate, last40, activeDiskSlots);
      allocateLocations(
        last40Allocations,
        last40,
        locations,
        isMaster,
        diskToWorkerMap,
        oldEpochs,
        nReduceIds,
        workerAggregatedLocations,
        activeDiskSlots);
    }

    if (roundRobinToAllocate > 0) {
      allocateEvenly(
        locations,
        isMaster,
        workers,
        roundRobinToAllocate,
        oldEpochs,
        nReduceIds,
        workerAggregatedLocations);
    }
  }

  private static void allocateEvenly(List<PartitionLocation> locations,
    boolean isMaster,
    List<WorkerInfo> workers,
    int required,
    int[] oldEpochs,
    List<Integer> reduceIds,
    Map<WorkerInfo, List<PartitionLocation>> workerAggregatedLocations) {
    int index = rand.nextInt(workers.size());
    for (int i = 0; i < required; i++) {
      int reduceId = reduceIds.remove(0);
      PartitionLocation.Mode mode = PartitionLocation.Mode.Master;
      if (!isMaster) {
        mode = PartitionLocation.Mode.Slave;
      }
      WorkerInfo workerInfo = workers.get(index % workers.size());
      int newEpoch = 0;
      if (oldEpochs != null) {
        newEpoch = oldEpochs[reduceId] + 1;
      }
      PartitionLocation location = new PartitionLocation(
        reduceId,
        newEpoch,
        workerInfo.host(),
        workerInfo.rpcPort(),
        workerInfo.pushPort(),
        workerInfo.fetchPort(),
        workerInfo.replicatePort(),
        mode,
        null,
        new StorageHint());
      locations.add(location);
      workerAggregatedLocations.compute(workerInfo, (k, v) -> {
        if (v == null) {
          v = new ArrayList<>();
        }
        v.add(location);
        return v;
      });
    }
  }

  private static void allocateLocations(
    int[] allocations,
    List<DiskInfo> disks,
    List<PartitionLocation> locations,
    boolean isMaster,
    Map<DiskInfo, WorkerInfo> diskToWorkerMap,
    int[] oldEpochs,
    List<Integer> reduceIds,
    Map<WorkerInfo, List<PartitionLocation>> workerAggregatedLocations,
    Map<DiskInfo, Long> activeDiskSlots) {
    for (int i = 0; i < allocations.length; i++) {
      int allocatedSlots = allocations[i];
      if (allocatedSlots == 0) {
        continue;
      }
      DiskInfo diskInfo = disks.get(i);
      for (int j = 0; j < allocatedSlots; j++) {
        int reduceId = reduceIds.remove(0);
        WorkerInfo workerInfo = diskToWorkerMap.get(diskInfo);
        int newEpoch = 0;
        if (oldEpochs != null) {
          newEpoch = oldEpochs[reduceId] + 1;
        }
        PartitionLocation.Mode mode = PartitionLocation.Mode.Master;
        if (!isMaster) {
          mode = PartitionLocation.Mode.Slave;
        }
        PartitionLocation location = new PartitionLocation(
          reduceId,
          newEpoch,
          workerInfo.host(),
          workerInfo.rpcPort(),
          workerInfo.pushPort(),
          workerInfo.fetchPort(),
          workerInfo.replicatePort(),
          mode,
          null,
          new StorageHint(StorageHint.Type.HDD,diskInfo.mountPoint()));
        locations.add(location);
        workerAggregatedLocations.compute(workerInfo, (k, v) -> {
          if (v == null) {
            v = new ArrayList<>();
          }
          v.add(location);
          return v;
        });
        activeDiskSlots.put(diskInfo, 1 + activeDiskSlots.get(diskInfo));
      }
    }
  }

  private static int[] getAllocationsBySlots(
    int required,
    List<DiskInfo> disks,
    Map<DiskInfo, Long> activeDiskSlots) {
    int diskCount = disks.size();
    long[] availableSlots = new long[diskCount];
    for (int i = 0; i < diskCount; i++) {
      availableSlots[i] = disks.get(i).availableSlots();
    }
    long total = disks.stream()
                 .map(disk -> disk.availableSlots() - activeDiskSlots.get(disk))
                 .mapToLong(l -> l).sum();
    int[] allocateArray = new int[diskCount];
    for (int i = 0; i < diskCount; i++) {
      DiskInfo disk = disks.get(i);
      int allocation = (int) Math.ceil((disk.availableSlots() - activeDiskSlots.get(disk)) * 1.0 /
                                       total * required);
      if (allocation > disk.availableSlots() - activeDiskSlots.get(disk)) {
        allocation = (int) disk.availableSlots() - activeDiskSlots.get(disk).intValue();
      }
      allocateArray[i] = allocation;
      availableSlots[i] -= allocation;
    }
    int allocatedTotal = Arrays.stream(allocateArray).sum();
    if (allocatedTotal > required) {
      int extraAllocations = allocatedTotal - required;
      int i = rand.nextInt(diskCount);
      while (extraAllocations != 0) {
        int idx = i % diskCount;
        if (allocateArray[idx] >= 0) {
          allocateArray[idx] = allocateArray[idx] - 1;
          extraAllocations--;
        }
        i++;
      }
    } else if (allocatedTotal < required) {
      int underAllocations = required - allocatedTotal;
      int i = rand.nextInt(diskCount);
      while (underAllocations != 0) {
        int idx = i % diskCount;
        if (availableSlots[idx] > 0) {
          allocateArray[idx] = allocateArray[idx] + 1;
        }
        i++;
      }
    }
    return allocateArray;
  }
}
