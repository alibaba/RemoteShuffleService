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
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import com.aliyun.emr.rss.common.RssConf;
import com.aliyun.emr.rss.common.haclient.RssHARetryClient;
import com.aliyun.emr.rss.common.meta.DiskInfo;
import com.aliyun.emr.rss.common.meta.WorkerInfo;
import com.aliyun.emr.rss.common.rpc.RpcEndpointAddress;
import com.aliyun.emr.rss.common.rpc.RpcEndpointRef;
import com.aliyun.emr.rss.common.rpc.RpcEnv;
import com.aliyun.emr.rss.common.rpc.netty.NettyRpcEndpointRef;
import com.aliyun.emr.rss.common.util.Utils;
import com.aliyun.emr.rss.service.deploy.master.clustermeta.AbstractMetaManager;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class RatisMasterStatusSystemSuiteJ {
  protected static HARaftServer RATISSERVER1 = null;
  protected static HARaftServer RATISSERVER2 = null;
  protected static HARaftServer RATISSERVER3 = null;
  protected static HAMasterMetaManager STATUSSYSTEM1 = null;
  protected static HAMasterMetaManager STATUSSYSTEM2 = null;
  protected static HAMasterMetaManager STATUSSYSTEM3 = null;

  private static RpcEndpointRef dummyRef = new NettyRpcEndpointRef(new RssConf(),
    RpcEndpointAddress.apply("localhost", 111, "dummy"), null);

  protected static RpcEnv mockRpcEnv = Mockito.mock(RpcEnv.class);
  protected static RpcEndpointRef mockRpcEndpoint = Mockito.mock(RpcEndpointRef.class);

  private static String DEFAULT_SERVICE_ID = "RSS_DEFAULT_SERVICE_ID";

  @BeforeClass
  public static void init() throws IOException, InterruptedException {
    Mockito.when(mockRpcEnv.setupEndpointRef(Mockito.any(), Mockito.any()))
      .thenReturn(mockRpcEndpoint);
    when(mockRpcEnv.setupEndpointRef(any(), any())).thenReturn(dummyRef);

    STATUSSYSTEM1 = new HAMasterMetaManager(mockRpcEnv, new RssConf());
    STATUSSYSTEM2 = new HAMasterMetaManager(mockRpcEnv, new RssConf());
    STATUSSYSTEM3 = new HAMasterMetaManager(mockRpcEnv, new RssConf());

    MetaHandler handler1 = new MetaHandler(STATUSSYSTEM1);
    MetaHandler handler2 = new MetaHandler(STATUSSYSTEM2);
    MetaHandler handler3 = new MetaHandler(STATUSSYSTEM3);

    RssConf conf1 = new RssConf();
    File tmpDir1 = File.createTempFile("rss-ratis1", "for-test-only");
    tmpDir1.delete();
    tmpDir1.mkdirs();
    conf1.set("rss.ha.storage.dir", tmpDir1.getAbsolutePath());

    RssConf conf2 = new RssConf();
    File tmpDir2 = File.createTempFile("rss-ratis2", "for-test-only");
    tmpDir2.delete();
    tmpDir2.mkdirs();
    conf2.set("rss.ha.storage.dir", tmpDir2.getAbsolutePath());

    RssConf conf3 = new RssConf();
    File tmpDir3 = File.createTempFile("rss-ratis3", "for-test-only");
    tmpDir3.delete();
    tmpDir3.mkdirs();
    conf3.set("rss.ha.storage.dir", tmpDir3.getAbsolutePath());

    String id1 = UUID.randomUUID().toString();
    String id2 = UUID.randomUUID().toString();
    String id3 = UUID.randomUUID().toString();
    int ratisPort1 = 9872;
    int ratisPort2 = 9873;
    int ratisPort3 = 9874;

    String localHost = Utils.localHostName();

    InetSocketAddress rpcAddress1 = new InetSocketAddress(
      localHost, 9872);
    InetSocketAddress rpcAddress2 = new InetSocketAddress(
      localHost, 9873);
    InetSocketAddress rpcAddress3 = new InetSocketAddress(
      localHost, 9874);
    NodeDetails nodeDetails1 =
        new NodeDetails.Builder()
            .setRpcAddress(rpcAddress1)
            .setRatisPort(ratisPort1)
            .setNodeId(id1)
            .setServiceId(DEFAULT_SERVICE_ID)
            .build();
    NodeDetails nodeDetails2 =
        new NodeDetails.Builder()
            .setRpcAddress(rpcAddress2)
            .setRatisPort(ratisPort2)
            .setNodeId(id2)
            .setServiceId(DEFAULT_SERVICE_ID)
            .build();
    NodeDetails nodeDetails3 =
        new NodeDetails.Builder()
            .setRpcAddress(rpcAddress3)
            .setRatisPort(ratisPort3)
            .setNodeId(id3)
            .setServiceId(DEFAULT_SERVICE_ID)
            .build();

    List<NodeDetails> peersForNode1 = new ArrayList() {{
        add(nodeDetails2);
        add(nodeDetails3);
      }};
    List<NodeDetails> peersForNode2 = new ArrayList() {{
        add(nodeDetails1);
        add(nodeDetails3);
      }};
    List<NodeDetails> peersForNode3 = new ArrayList() {{
        add(nodeDetails1);
        add(nodeDetails2);
      }};

    RATISSERVER1 = HARaftServer.newMasterRatisServer(handler1,
      conf1, nodeDetails1, peersForNode1);
    RATISSERVER2 = HARaftServer.newMasterRatisServer(handler2,
      conf2, nodeDetails2, peersForNode2);
    RATISSERVER3 = HARaftServer.newMasterRatisServer(handler3,
      conf3, nodeDetails3, peersForNode3);

    STATUSSYSTEM1.setRatisServer(RATISSERVER1);
    STATUSSYSTEM2.setRatisServer(RATISSERVER2);
    STATUSSYSTEM3.setRatisServer(RATISSERVER3);

    RATISSERVER1.start();
    RATISSERVER2.start();
    RATISSERVER3.start();

    Thread.sleep(15 * 1000);
  }

  @Test
  public void testLeaderAvaiable() throws InterruptedException {
    boolean hasLeader = false;
    if (RATISSERVER1.isLeader() || RATISSERVER2.isLeader() || RATISSERVER3.isLeader()) {
      hasLeader = true;
    }
    Assert.assertEquals(hasLeader, true);
  }

  private static String HOSTNAME1 = "host1";
  private static int RPCPORT1 = 1111;
  private static int PUSHPORT1 = 1112;
  private static int FETCHPORT1 = 1113;
  private static int REPLICATEPORT1 = 1114;
  private static Map<String, DiskInfo> disks1 = new HashMap(){};

  private static String HOSTNAME2 = "host2";
  private static int RPCPORT2 = 2111;
  private static int PUSHPORT2 = 2112;
  private static int FETCHPORT2 = 2113;
  private static int REPLICATEPORT2 = 2114;
  private static Map<String,DiskInfo> disks2 = new HashMap<>();

  private static String HOSTNAME3 = "host3";
  private static int RPCPORT3 = 3111;
  private static int PUSHPORT3 = 3112;
  private static int FETCHPORT3 = 3113;
  private static int REPLICATEPORT3 = 3114;
  private static Map<String,DiskInfo> disks3 = new HashMap<>();

  private AtomicLong callerId = new AtomicLong();
  private static String APPID1 = "appId1";
  private static int SHUFFLEID1 = 1;
  private static String SHUFFLEKEY1 = APPID1 + "-" + SHUFFLEID1;

  private String getNewReqeustId() {
    return RssHARetryClient.encodeRequestId(UUID.randomUUID().toString(),
      callerId.incrementAndGet());
  }

  public HAMasterMetaManager pickLeaderStatusSystem() {
    if (RATISSERVER1.isLeader()) {
      return STATUSSYSTEM1;
    }
    if (RATISSERVER2.isLeader()) {
      return STATUSSYSTEM2;
    }
    if (RATISSERVER3.isLeader()) {
      return STATUSSYSTEM3;
    }
    return null;
  }

  @Test
  public void testHandleRegisterWorker() throws InterruptedException {
    AbstractMetaManager statusSystem = pickLeaderStatusSystem();
    Assert.assertNotNull(statusSystem);

    statusSystem.handleRegisterWorker(HOSTNAME1, RPCPORT1, PUSHPORT1, FETCHPORT1, REPLICATEPORT1,
      disks1, getNewReqeustId());
    statusSystem.handleRegisterWorker(HOSTNAME2, RPCPORT2, PUSHPORT2, FETCHPORT2, REPLICATEPORT2,
      disks2, getNewReqeustId());
    statusSystem.handleRegisterWorker(HOSTNAME3, RPCPORT3, PUSHPORT3, FETCHPORT3, REPLICATEPORT3,
      disks3, getNewReqeustId());
    Thread.sleep(3000L);

    Assert.assertEquals(STATUSSYSTEM1.workers.size(), 3);
    Assert.assertEquals(3, STATUSSYSTEM1.workers.size());
    Assert.assertEquals(3, STATUSSYSTEM2.workers.size());
    Assert.assertEquals(3, STATUSSYSTEM3.workers.size());
  }

  @Test
  public void testHandleWorkerLost() throws InterruptedException {
    AbstractMetaManager statusSystem = pickLeaderStatusSystem();
    Assert.assertNotNull(statusSystem);

    statusSystem.handleRegisterWorker(HOSTNAME1, RPCPORT1, PUSHPORT1, FETCHPORT1, REPLICATEPORT1,
      disks1, getNewReqeustId());
    statusSystem.handleRegisterWorker(HOSTNAME2, RPCPORT2, PUSHPORT2, FETCHPORT2, REPLICATEPORT2,
      disks2, getNewReqeustId());
    statusSystem.handleRegisterWorker(HOSTNAME3, RPCPORT3, PUSHPORT3, FETCHPORT3, REPLICATEPORT3,
      disks3, getNewReqeustId());

    statusSystem.handleWorkerLost(HOSTNAME1, RPCPORT1, PUSHPORT1, FETCHPORT1, REPLICATEPORT1,
      getNewReqeustId());
    Thread.sleep(3000L);

    Assert.assertEquals(2, STATUSSYSTEM1.workers.size());
    Assert.assertEquals(2, STATUSSYSTEM2.workers.size());
    Assert.assertEquals(2, STATUSSYSTEM3.workers.size());
  }

  @Test
  public void testHandleRequestSlots() {
    AbstractMetaManager statusSystem = pickLeaderStatusSystem();
    Assert.assertNotNull(statusSystem);

    statusSystem.handleRegisterWorker(HOSTNAME1, RPCPORT1, PUSHPORT1, FETCHPORT1, REPLICATEPORT1,
      disks1, getNewReqeustId());
    statusSystem.handleRegisterWorker(HOSTNAME2, RPCPORT2, PUSHPORT2, FETCHPORT2, REPLICATEPORT2,
      disks2, getNewReqeustId());
    statusSystem.handleRegisterWorker(HOSTNAME3, RPCPORT3, PUSHPORT3, FETCHPORT3, REPLICATEPORT3,
      disks3, getNewReqeustId());

    WorkerInfo workerInfo1 = new WorkerInfo(HOSTNAME1, RPCPORT1, PUSHPORT1, FETCHPORT1,
      REPLICATEPORT1, disks1, dummyRef);
    WorkerInfo workerInfo2 = new WorkerInfo(HOSTNAME2, RPCPORT2, PUSHPORT2, FETCHPORT2,
      REPLICATEPORT2, disks2, dummyRef);
    WorkerInfo workerInfo3 = new WorkerInfo(HOSTNAME3, RPCPORT3, PUSHPORT3, FETCHPORT3,
      REPLICATEPORT3, disks3, dummyRef);

    Map<String, Map<String, Integer>> workersToAllocate = new HashMap<>();
    Map<String, Integer> allocation1 = new HashMap<>();
    allocation1.put("disk1", 15);
    Map<String, Integer> allocation2 = new HashMap<>();
    allocation2.put("disk2", 25);
    Map<String, Integer> allocation3 = new HashMap<>();
    allocation3.put("disk3", 35);
    workersToAllocate.put(workerInfo1.toUniqueId(), allocation1);
    workersToAllocate.put(workerInfo2.toUniqueId(), allocation2);
    workersToAllocate.put(workerInfo3.toUniqueId(), allocation3);

    statusSystem.handleRequestSlots(SHUFFLEKEY1, HOSTNAME1, workersToAllocate, getNewReqeustId());

    Assert.assertEquals(15,
      statusSystem.workers.stream().filter(w -> w.host().equals(HOSTNAME1)).findFirst()
        .get().usedSlots());
    Assert.assertEquals(25,
      statusSystem.workers.stream().filter(w -> w.host().equals(HOSTNAME2)).findFirst()
        .get().usedSlots());
    Assert.assertEquals(35,
      statusSystem.workers.stream().filter(w -> w.host().equals(HOSTNAME3)).findFirst()
        .get().usedSlots());
  }

  @Test
  public void testHandleReleaseSlots() throws InterruptedException {
    AbstractMetaManager statusSystem = pickLeaderStatusSystem();
    Assert.assertNotNull(statusSystem);

    statusSystem.handleRegisterWorker(HOSTNAME1, RPCPORT1, PUSHPORT1, FETCHPORT1, REPLICATEPORT1,
      disks1, getNewReqeustId());
    statusSystem.handleRegisterWorker(HOSTNAME2, RPCPORT2, PUSHPORT2, FETCHPORT2, REPLICATEPORT2,
      disks2, getNewReqeustId());
    statusSystem.handleRegisterWorker(HOSTNAME3, RPCPORT3, PUSHPORT3, FETCHPORT3, REPLICATEPORT3,
      disks3, getNewReqeustId());
    Thread.sleep(3000L);

    Assert.assertEquals(3, STATUSSYSTEM1.workers.size());
    Assert.assertEquals(3, STATUSSYSTEM2.workers.size());
    Assert.assertEquals(3, STATUSSYSTEM3.workers.size());

    Map<String, Map<String, Integer>> workersToAllocate = new HashMap<>();
    Map<String, Integer> allocations = new HashMap<>();
    allocations.put("disk1", 5);
    workersToAllocate.put(statusSystem.workers.stream().filter(w -> w.host().equals(HOSTNAME1))
                              .findFirst().get().toUniqueId(), allocations);
    workersToAllocate.put(statusSystem.workers.stream().filter(w -> w.host().equals(HOSTNAME2))
                              .findFirst().get().toUniqueId(), allocations);

    statusSystem.handleRequestSlots(SHUFFLEKEY1, HOSTNAME1, workersToAllocate, getNewReqeustId());
    Thread.sleep(3000L);

    List<String> workerIds = new ArrayList<>();
    workerIds.add(HOSTNAME1 + ":" + RPCPORT1 + ":" + PUSHPORT1 + ":" + FETCHPORT1 +
                    ":" + REPLICATEPORT1);

    List<Map<String, Integer>> workerSlots = new ArrayList<>();
    workerSlots.add(new HashMap() {{
        put("disk1", 3);
      }});

    statusSystem.handleReleaseSlots(SHUFFLEKEY1, workerIds, workerSlots, getNewReqeustId());
    Thread.sleep(3000L);

    Assert.assertEquals(2, STATUSSYSTEM1.workers.stream().filter(w -> w.host().equals(HOSTNAME1))
                             .findFirst().get().usedSlots());
    Assert.assertEquals(2, STATUSSYSTEM2.workers.stream().filter(w -> w.host().equals(HOSTNAME1))
                             .findFirst().get().usedSlots());
    Assert.assertEquals(2, STATUSSYSTEM3.workers.stream().filter(w -> w.host().equals(HOSTNAME1))
                             .findFirst().get().usedSlots());
  }

  @Test
  public void testHandleAppLost() throws InterruptedException {
    AbstractMetaManager statusSystem = pickLeaderStatusSystem();
    Assert.assertNotNull(statusSystem);

    statusSystem.handleRegisterWorker(HOSTNAME1, RPCPORT1, PUSHPORT1, FETCHPORT1, REPLICATEPORT1,
      disks1, getNewReqeustId());
    statusSystem.handleRegisterWorker(HOSTNAME2, RPCPORT2, PUSHPORT2, FETCHPORT2, REPLICATEPORT2,
      disks2, getNewReqeustId());
    statusSystem.handleRegisterWorker(HOSTNAME3, RPCPORT3, PUSHPORT3, FETCHPORT3, REPLICATEPORT3,
      disks3, getNewReqeustId());

    Thread.sleep(3000L);
    WorkerInfo workerInfo1 = new WorkerInfo(HOSTNAME1, RPCPORT1, PUSHPORT1, FETCHPORT1,
      REPLICATEPORT1, disks1, dummyRef);
    WorkerInfo workerInfo2 = new WorkerInfo(HOSTNAME2, RPCPORT2, PUSHPORT2, FETCHPORT2,
      REPLICATEPORT2, disks2, dummyRef);
    Map<String, Map<String, Integer>> workersToAllocate = new HashMap<>();
    Map<String, Integer> allocations = new HashMap<>();
    allocations.put("disk1", 5);
    workersToAllocate.put(workerInfo1.toUniqueId(), allocations);
    workersToAllocate.put(workerInfo2.toUniqueId(), allocations);

    statusSystem.handleRequestSlots(SHUFFLEKEY1, HOSTNAME1, workersToAllocate, getNewReqeustId());
    Thread.sleep(3000L);

    Assert.assertEquals(1, STATUSSYSTEM1.registeredShuffle.size());
    Assert.assertEquals(1, STATUSSYSTEM2.registeredShuffle.size());
    Assert.assertEquals(1, STATUSSYSTEM3.registeredShuffle.size());

    statusSystem.handleAppLost(APPID1, getNewReqeustId());
    Thread.sleep(3000L);

    Assert.assertEquals(true, STATUSSYSTEM1.registeredShuffle.isEmpty());
    Assert.assertEquals(true, STATUSSYSTEM2.registeredShuffle.isEmpty());
    Assert.assertEquals(true, STATUSSYSTEM3.registeredShuffle.isEmpty());
  }

  @Test
  public void testHandleUnRegisterShuffle() throws InterruptedException {
    AbstractMetaManager statusSystem = pickLeaderStatusSystem();
    Assert.assertNotNull(statusSystem);

    statusSystem.handleRegisterWorker(HOSTNAME1, RPCPORT1, PUSHPORT1, FETCHPORT1, REPLICATEPORT1,
      disks1, getNewReqeustId());
    statusSystem.handleRegisterWorker(HOSTNAME2, RPCPORT2, PUSHPORT2, FETCHPORT2, REPLICATEPORT2,
      disks2, getNewReqeustId());
    statusSystem.handleRegisterWorker(HOSTNAME3, RPCPORT3, PUSHPORT3, FETCHPORT3, REPLICATEPORT3,
      disks3, getNewReqeustId());

    WorkerInfo workerInfo1 = new WorkerInfo(HOSTNAME1, RPCPORT1, PUSHPORT1, FETCHPORT1,
      REPLICATEPORT1, disks1, dummyRef);
    WorkerInfo workerInfo2 = new WorkerInfo(HOSTNAME2, RPCPORT2, PUSHPORT2, FETCHPORT2,
      REPLICATEPORT2, disks2, dummyRef);

    Map<String, Map<String, Integer>> workersToAllocate = new HashMap<>();
    Map<String, Integer> allocations = new HashMap<>();
    allocations.put("disk1", 5);
    workersToAllocate.put(workerInfo1.toUniqueId(), allocations);
    workersToAllocate.put(workerInfo2.toUniqueId(), allocations);

    statusSystem.handleRequestSlots(SHUFFLEKEY1, HOSTNAME1, workersToAllocate, getNewReqeustId());
    Thread.sleep(3000L);

    Assert.assertEquals(1, STATUSSYSTEM1.registeredShuffle.size());
    Assert.assertEquals(1, STATUSSYSTEM2.registeredShuffle.size());
    Assert.assertEquals(1, STATUSSYSTEM3.registeredShuffle.size());

    statusSystem.handleUnRegisterShuffle(SHUFFLEKEY1, getNewReqeustId());
    Thread.sleep(3000L);

    Assert.assertEquals(true, STATUSSYSTEM1.registeredShuffle.isEmpty());
    Assert.assertEquals(true, STATUSSYSTEM2.registeredShuffle.isEmpty());
    Assert.assertEquals(true, STATUSSYSTEM3.registeredShuffle.isEmpty());
  }

  @Test
  public void testHandleAppHeartbeat() throws InterruptedException {
    AbstractMetaManager statusSystem = pickLeaderStatusSystem();
    Assert.assertNotNull(statusSystem);

    long dummy = 1235L;
    statusSystem.handleAppHeartbeat(APPID1, 1, 1, dummy, getNewReqeustId());
    Thread.sleep(3000L);
    Assert.assertEquals(new Long(dummy), STATUSSYSTEM1.appHeartbeatTime.get(APPID1));
    Assert.assertEquals(new Long(dummy), STATUSSYSTEM2.appHeartbeatTime.get(APPID1));
    Assert.assertEquals(new Long(dummy), STATUSSYSTEM3.appHeartbeatTime.get(APPID1));

    String appId2 = "app02";
    statusSystem.handleAppHeartbeat(appId2, 1, 1, dummy, getNewReqeustId());
    Thread.sleep(3000L);

    Assert.assertEquals(new Long(dummy), STATUSSYSTEM1.appHeartbeatTime.get(appId2));
    Assert.assertEquals(new Long(dummy), STATUSSYSTEM2.appHeartbeatTime.get(appId2));
    Assert.assertEquals(new Long(dummy), STATUSSYSTEM3.appHeartbeatTime.get(appId2));

    Assert.assertEquals(2, STATUSSYSTEM1.appHeartbeatTime.size());
    Assert.assertEquals(2, STATUSSYSTEM2.appHeartbeatTime.size());
    Assert.assertEquals(2, STATUSSYSTEM3.appHeartbeatTime.size());
  }

  @Test
  public void testHandleWorkerHeartBeat() throws InterruptedException {
    AbstractMetaManager statusSystem = pickLeaderStatusSystem();
    Assert.assertNotNull(statusSystem);

    statusSystem.handleRegisterWorker(HOSTNAME1, RPCPORT1, PUSHPORT1, FETCHPORT1, REPLICATEPORT1,
      disks1, getNewReqeustId());
    statusSystem.handleRegisterWorker(HOSTNAME2, RPCPORT2, PUSHPORT2, FETCHPORT2, REPLICATEPORT2,
      disks2, getNewReqeustId());
    statusSystem.handleRegisterWorker(HOSTNAME3, RPCPORT3, PUSHPORT3, FETCHPORT3, REPLICATEPORT3,
      disks3, getNewReqeustId());

    statusSystem.handleWorkerHeartBeat(HOSTNAME1, RPCPORT1, PUSHPORT1, FETCHPORT1, REPLICATEPORT1,
      new HashMap<>(), 1, getNewReqeustId());
    Thread.sleep(3000L);

    Assert.assertEquals(1, STATUSSYSTEM1.blacklist.size());
    Assert.assertEquals(1, STATUSSYSTEM2.blacklist.size());
    Assert.assertEquals(1, STATUSSYSTEM3.blacklist.size());

    statusSystem.handleWorkerHeartBeat(HOSTNAME2, RPCPORT2, PUSHPORT2, FETCHPORT2, REPLICATEPORT2,
      new HashMap<>(), 1, getNewReqeustId());
    Thread.sleep(3000L);

    Assert.assertEquals(2, statusSystem.blacklist.size());
    Assert.assertEquals(2, STATUSSYSTEM1.blacklist.size());
    Assert.assertEquals(2, STATUSSYSTEM2.blacklist.size());
    Assert.assertEquals(2, STATUSSYSTEM3.blacklist.size());

    statusSystem.handleWorkerHeartBeat(HOSTNAME1, RPCPORT1, PUSHPORT1, FETCHPORT1, REPLICATEPORT1,
      disks1, 1, getNewReqeustId());
    Thread.sleep(3000L);

    Assert.assertEquals(1, statusSystem.blacklist.size());
    Assert.assertEquals(1, STATUSSYSTEM1.blacklist.size());
    Assert.assertEquals(1, STATUSSYSTEM2.blacklist.size());
    Assert.assertEquals(1, STATUSSYSTEM3.blacklist.size());
  }

  @Before
  public void resetStatus() {
    STATUSSYSTEM1.registeredShuffle.clear();
    STATUSSYSTEM1.hostnameSet.clear();
    STATUSSYSTEM1.workers.clear();
    STATUSSYSTEM1.appHeartbeatTime.clear();
    STATUSSYSTEM1.blacklist.clear();
    STATUSSYSTEM1.workerLostEvents.clear();

    STATUSSYSTEM2.registeredShuffle.clear();
    STATUSSYSTEM2.hostnameSet.clear();
    STATUSSYSTEM2.workers.clear();
    STATUSSYSTEM2.appHeartbeatTime.clear();
    STATUSSYSTEM2.blacklist.clear();
    STATUSSYSTEM2.workerLostEvents.clear();

    STATUSSYSTEM3.registeredShuffle.clear();
    STATUSSYSTEM3.hostnameSet.clear();
    STATUSSYSTEM3.workers.clear();
    STATUSSYSTEM3.appHeartbeatTime.clear();
    STATUSSYSTEM3.blacklist.clear();
    STATUSSYSTEM3.workerLostEvents.clear();

    disks1.clear();
    disks1.put("disk1", new DiskInfo("disk1", 64 * 1024 * 1024 * 1024L, 100, 0));
    disks1.put("disk2", new DiskInfo("disk2", 64 * 1024 * 1024 * 1024L, 100, 0));
    disks1.put("disk3", new DiskInfo("disk3", 64 * 1024 * 1024 * 1024L, 100, 0));
    disks1.put("disk4", new DiskInfo("disk4", 64 * 1024 * 1024 * 1024L, 100, 0));

    disks2.clear();
    disks2.put("disk1", new DiskInfo("disk1", 64 * 1024 * 1024 * 1024L, 100, 0));
    disks2.put("disk2", new DiskInfo("disk2", 64 * 1024 * 1024 * 1024L, 100, 0));
    disks2.put("disk3", new DiskInfo("disk3", 64 * 1024 * 1024 * 1024L, 100, 0));
    disks2.put("disk4", new DiskInfo("disk4", 64 * 1024 * 1024 * 1024L, 100, 0));

    disks3.clear();
    disks3.put("disk1", new DiskInfo("disk1", 64 * 1024 * 1024 * 1024L, 100, 0));
    disks3.put("disk2", new DiskInfo("disk2", 64 * 1024 * 1024 * 1024L, 100, 0));
    disks3.put("disk3", new DiskInfo("disk3", 64 * 1024 * 1024 * 1024L, 100, 0));
    disks3.put("disk4", new DiskInfo("disk4", 64 * 1024 * 1024 * 1024L, 100, 0));
  }

  @Test
  public void testHandleReportWorkerFailure() throws InterruptedException {
    AbstractMetaManager statusSystem = pickLeaderStatusSystem();
    Assert.assertNotNull(statusSystem);

    statusSystem.handleRegisterWorker(HOSTNAME1, RPCPORT1, PUSHPORT1, FETCHPORT1, REPLICATEPORT1,
      disks1, getNewReqeustId());
    statusSystem.handleRegisterWorker(HOSTNAME2, RPCPORT2, PUSHPORT2, FETCHPORT2, REPLICATEPORT2,
      disks2, getNewReqeustId());
    statusSystem.handleRegisterWorker(HOSTNAME3, RPCPORT3, PUSHPORT3, FETCHPORT3, REPLICATEPORT3,
      disks3, getNewReqeustId());

    WorkerInfo workerInfo1 = new WorkerInfo(HOSTNAME1, RPCPORT1, PUSHPORT1, FETCHPORT1,
      REPLICATEPORT1, disks1, dummyRef);
    WorkerInfo workerInfo2 = new WorkerInfo(HOSTNAME2, RPCPORT2, PUSHPORT2, FETCHPORT2,
      REPLICATEPORT2, disks2, dummyRef);

    List<WorkerInfo> failedWorkers = new ArrayList<>();
    failedWorkers.add(workerInfo1);

    statusSystem.handleReportWorkerFailure(failedWorkers, getNewReqeustId());
    Thread.sleep(3000L);
    Assert.assertEquals(1, STATUSSYSTEM1.blacklist.size());
    Assert.assertEquals(1, STATUSSYSTEM2.blacklist.size());
    Assert.assertEquals(1, STATUSSYSTEM3.blacklist.size());
  }
}
