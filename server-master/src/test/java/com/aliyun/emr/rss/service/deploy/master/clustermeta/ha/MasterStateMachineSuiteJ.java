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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;

import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.server.storage.RaftStorageImpl;
import org.apache.ratis.statemachine.SnapshotRetentionPolicy;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.junit.Assert;
import org.junit.Test;

import com.aliyun.emr.rss.common.RssConf;
import com.aliyun.emr.rss.common.meta.DiskInfo;
import com.aliyun.emr.rss.common.meta.WorkerInfo;
import com.aliyun.emr.rss.common.util.Utils;
import com.aliyun.emr.rss.service.deploy.master.clustermeta.ResourceProtos;
import com.aliyun.emr.rss.service.deploy.master.clustermeta.ResourceProtos.RequestSlotsRequest;
import com.aliyun.emr.rss.service.deploy.master.clustermeta.ResourceProtos.ResourceRequest;
import com.aliyun.emr.rss.service.deploy.master.clustermeta.ResourceProtos.ResourceResponse;
import com.aliyun.emr.rss.service.deploy.master.clustermeta.ResourceProtos.Type;

public class MasterStateMachineSuiteJ extends RatisBaseSuiteJ {

  @Test
  public void testRunCommand() {
    StateMachine stateMachine = ratisServer.getMasterStateMachine();

    Map<String, Integer> allocations = new HashMap<>();
    allocations.put("disk1", 15);
    allocations.put("disk2", 20);

    Map<String, ResourceProtos.SlotInfo> workerAllocations = new HashMap<>();
    workerAllocations.put(
        new WorkerInfo("host1", 1, 2, 3, 10).toUniqueId(),
        ResourceProtos.SlotInfo.newBuilder().putAllSlot(allocations).build());
    workerAllocations.put(
        new WorkerInfo("host2", 2, 3, 4, 11).toUniqueId(),
        ResourceProtos.SlotInfo.newBuilder().putAllSlot(allocations).build());
    workerAllocations.put(
        new WorkerInfo("host3", 3, 4, 5, 12).toUniqueId(),
        ResourceProtos.SlotInfo.newBuilder().putAllSlot(allocations).build());

    RequestSlotsRequest requestSlots =
        RequestSlotsRequest.newBuilder()
            .setShuffleKey("appId-1-1")
            .setHostName("hostname")
            .putAllWorkerAllocations(workerAllocations)
            .build();

    ResourceRequest request = ResourceRequest.newBuilder()
      .setRequestSlotsRequest(requestSlots)
      .setCmdType(Type.RequestSlots)
      .setRequestId(UUID.randomUUID().toString())
      .build();

    ResourceResponse response = stateMachine.runCommand(request, -1);
    Assert.assertEquals(response.getSuccess(), true);
  }

  @Test
  public void testSnapshotCleanup() throws IOException {
    StateMachine stateMachine = ratisServer.getMasterStateMachine();
    SnapshotRetentionPolicy snapshotRetentionPolicy = new SnapshotRetentionPolicy() {
      @Override
      public int getNumSnapshotsRetained() {
        return 3;
      }
    };

    File storageDir = Utils.createTempDir("./", "snapshot");

    final RaftStorage storage = new RaftStorageImpl(storageDir, null, 100);
    SimpleStateMachineStorage simpleStateMachineStorage =
      (SimpleStateMachineStorage)stateMachine.getStateMachineStorage();
    simpleStateMachineStorage.init(storage);

    List<Long> indices = new ArrayList<>();

    //Create 5 snapshot files in storage dir.
    for (int i = 0; i < 5; i++) {
      final long term = ThreadLocalRandom.current().nextLong(3L, 10L);
      final long index = ThreadLocalRandom.current().nextLong(100L, 1000L);
      indices.add(index);
      File snapshotFile = simpleStateMachineStorage.getSnapshotFile(term, index);
      snapshotFile.createNewFile();
      File md5File = new File(snapshotFile.getAbsolutePath()+".md5");
      md5File.createNewFile();
    }

    // following 2 md5 files will be deleted
    File snapshotFile1 = simpleStateMachineStorage.getSnapshotFile(1, 1);
    File md5File1 = new File(snapshotFile1.getAbsolutePath()+".md5");
    md5File1.createNewFile();
    File snapshotFile2 = simpleStateMachineStorage.getSnapshotFile(5, 2);
    File md5File2 = new File(snapshotFile2.getAbsolutePath()+".md5");
    md5File2.createNewFile();
    // this md5 file will not be deleted
    File snapshotFile3 = simpleStateMachineStorage.getSnapshotFile(11, 1001);
    File md5File3 = new File(snapshotFile3.getAbsolutePath()+".md5");
    md5File3.createNewFile();

    File stateMachineDir = simpleStateMachineStorage.getSmDir();
    Assert.assertTrue(stateMachineDir.listFiles().length == 13);
    simpleStateMachineStorage.cleanupOldSnapshots(snapshotRetentionPolicy);
    File[] remainingFiles = stateMachineDir.listFiles();
    Assert.assertTrue(remainingFiles.length == 7);

    Collections.sort(indices);
    Collections.reverse(indices);
    List<Long> remainingIndices = indices.subList(0, 3);
    // check snapshot file and its md5 file management
    for (File file : remainingFiles) {
      System.out.println(file.getName());
      Matcher matcher = SimpleStateMachineStorage.SNAPSHOT_REGEX.matcher(file.getName());
      if (matcher.matches()) {
        Assert.assertTrue(remainingIndices.contains(Long.parseLong(matcher.group(2))));
        Assert.assertTrue(new File(file.getAbsolutePath() + ".md5").exists());
      }
    }

    // Attempt to clean up again should not delete any more files.
    simpleStateMachineStorage.cleanupOldSnapshots(snapshotRetentionPolicy);
    remainingFiles = stateMachineDir.listFiles();
    Assert.assertTrue(remainingFiles.length == 7);

    //Test with Retention disabled.
    //Create 2 snapshot files in storage dir.
    for (int i = 0; i < 2; i++) {
      final long term = ThreadLocalRandom.current().nextLong(10L);
      final long index = ThreadLocalRandom.current().nextLong(1000L);
      indices.add(index);
      File snapshotFile= simpleStateMachineStorage.getSnapshotFile(term, index);
      snapshotFile.createNewFile();
      File md5File = new File(snapshotFile.getAbsolutePath()+".md5");
      md5File.createNewFile();
    }

    simpleStateMachineStorage.cleanupOldSnapshots(new SnapshotRetentionPolicy() {
    });

    Assert.assertTrue(stateMachineDir.listFiles().length == 11);
  }

  @Test
  public void testObjSerde() throws IOException {
    HAMasterMetaManager masterStatusSystem = new HAMasterMetaManager(null, new RssConf());
    File tmpFile = File.createTempFile("tef", "test" + System.currentTimeMillis());

    Map<String, DiskInfo> disks1 = new HashMap<>();
    disks1.put("disk1", new DiskInfo("disk1", 64 * 1024 * 1024 * 1024, 100, 0));
    disks1.put("disk2", new DiskInfo("disk2", 64 * 1024 * 1024 * 1024, 100, 0));
    disks1.put("disk3", new DiskInfo("disk3", 64 * 1024 * 1024 * 1024, 100, 0));

    Map<String, DiskInfo> disks2 = new HashMap<>();
    disks2.put("disk1", new DiskInfo("disk1", 64 * 1024 * 1024 * 1024, 100, 0));
    disks2.put("disk2", new DiskInfo("disk2", 64 * 1024 * 1024 * 1024, 100, 0));
    disks2.put("disk3", new DiskInfo("disk3", 64 * 1024 * 1024 * 1024, 100, 0));

    Map<String, DiskInfo> disks3 = new HashMap<>();
    disks3.put("disk1", new DiskInfo("disk1", 64 * 1024 * 1024 * 1024, 100, 0));
    disks3.put("disk2", new DiskInfo("disk2", 64 * 1024 * 1024 * 1024, 100, 0));
    disks3.put("disk3", new DiskInfo("disk3", 64 * 1024 * 1024 * 1024, 100, 0));

    WorkerInfo info1 = new WorkerInfo("host1", 1, 2, 3, 10, disks1, null);
    WorkerInfo info2 = new WorkerInfo("host2", 4, 5, 6, 11, disks2, null);
    WorkerInfo info3 = new WorkerInfo("host3", 7, 8, 9, 12, disks3, null);

    String host1 = "host1";
    String host2 = "host2";
    String host3 = "host3";

    masterStatusSystem.blacklist.add(info1);
    masterStatusSystem.blacklist.add(info2);
    masterStatusSystem.blacklist.add(info3);

    masterStatusSystem.hostnameSet.add(host1);
    masterStatusSystem.hostnameSet.add(host2);
    masterStatusSystem.hostnameSet.add(host3);

    masterStatusSystem.writeMetaInfoToFile(tmpFile);

    masterStatusSystem.hostnameSet.clear();
    masterStatusSystem.blacklist.clear();

    masterStatusSystem.restoreMetaFromFile(tmpFile);

    Assert.assertEquals(3, masterStatusSystem.blacklist.size());
    Assert.assertEquals(3, masterStatusSystem.hostnameSet.size());
  }
}
