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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;

import com.aliyun.emr.rss.common.RssConf;

public class RatisBaseSuiteJ {
  HARaftServer ratisServer;

  protected final String DEFAULT_SERVICE_ID = "ServiceIdDefault";

  @Before
  public void init() throws Exception {
    RssConf conf = new RssConf();
    HAMasterMetaManager metaSystem = new HAMasterMetaManager(null, conf);
    MetaHandler handler = new MetaHandler(metaSystem);
    File tmpDir1 = File.createTempFile("rss-ratis-tmp", "for-test-only");
    tmpDir1.delete();
    tmpDir1.mkdirs();
    conf.set("rss.ha.storage.dir", tmpDir1.getAbsolutePath());
    String id = UUID.randomUUID().toString();
    int ratisPort = 9999;
    InetSocketAddress rpcAddress = new InetSocketAddress(
        InetAddress.getLocalHost(), 0);
    NodeDetails nodeDetails = new NodeDetails.Builder()
        .setRpcAddress(rpcAddress)
        .setRatisPort(ratisPort)
        .setNodeId(id)
        .setServiceId(DEFAULT_SERVICE_ID)
        .build();
    ratisServer = HARaftServer.newMasterRatisServer(handler,
        conf, nodeDetails, Collections.emptyList());
    ratisServer.start();
  }

  @After
  public void shutdown() {
    if (ratisServer != null) {
      ratisServer.stop();
    }
  }
}
