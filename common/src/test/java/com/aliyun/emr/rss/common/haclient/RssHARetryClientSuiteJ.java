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

package com.aliyun.emr.rss.common.haclient;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import scala.concurrent.Future;
import scala.concurrent.Future$;

import com.google.common.util.concurrent.SettableFuture;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.emr.rss.common.RssConf;
import com.aliyun.emr.rss.common.exception.RssException;
import com.aliyun.emr.rss.common.protocol.message.ControlMessages.HeartBeatFromApplication;
import com.aliyun.emr.rss.common.protocol.message.ControlMessages.HeartbeatFromWorker;
import com.aliyun.emr.rss.common.protocol.message.ControlMessages.HeartbeatResponse;
import com.aliyun.emr.rss.common.protocol.message.ControlMessages.OneWayMessageResponse$;
import com.aliyun.emr.rss.common.rpc.RpcAddress;
import com.aliyun.emr.rss.common.rpc.RpcEndpointRef;
import com.aliyun.emr.rss.common.rpc.RpcEnv;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RssHARetryClientSuiteJ {
  private static final Logger LOG = LoggerFactory.getLogger(RssHARetryClientSuiteJ.class);

  private final String masterHost = "localhost";
  private final int masterPort = 9097;
  private final RssConf rssConf = new RssConf(false);
  private final OneWayMessageResponse$ response = OneWayMessageResponse$.MODULE$;
  private final HeartbeatResponse mockResponse = Mockito.mock(HeartbeatResponse.class);

  private RpcEnv rpcEnv = null;
  private RpcEndpointRef endpointRef = null;

  @Before
  public void beforeEach() {
    rssConf.set("rss.rpc.askTimeout", "5s")
      .set("rss.network.timeout", "5s")
      .set("rss.master.port", String.valueOf(masterPort));
    rpcEnv = Mockito.mock(RpcEnv.class);
    endpointRef = Mockito.mock(RpcEndpointRef.class);
  }

  @Test
  public void testSendOneWayMessageWithoutHA() throws Exception {
    final SettableFuture<Boolean> success = SettableFuture.create();
    final RssConf conf = prepareForRssConfWithoutHA();

    prepareForEndpointRefWithoutRetry(() -> {
      success.set(true);
      return Future$.MODULE$.successful(response);
    });
    prepareForRpcEnvWithoutHA();

    RssHARetryClient client = new RssHARetryClient(rpcEnv, conf);
    HeartBeatFromApplication message = Mockito.mock(HeartBeatFromApplication.class);

    try {
      client.send(message);
    } catch (Throwable t) {
      LOG.error("It should be no exceptions when sending one-way message.", t);
      fail("It should be no exceptions when sending one-way message.");
    }

    assertTrue(success.get(5, TimeUnit.SECONDS));
  }

  @Test
  public void testSendOneWayMessageWithoutHAWithRetry() throws Exception {
    final AtomicInteger numTries = new AtomicInteger(0);
    final SettableFuture<Boolean> success = SettableFuture.create();
    final RssConf conf = prepareForRssConfWithoutHA();

    prepareForEndpointRefWithRetry(numTries, () -> {
      success.set(true);
      return Future$.MODULE$.successful(response);
    });
    prepareForRpcEnvWithoutHA();

    RssHARetryClient client = new RssHARetryClient(rpcEnv, conf);
    HeartBeatFromApplication message = Mockito.mock(HeartBeatFromApplication.class);

    try {
      client.send(message);
    } catch (Throwable t) {
      LOG.error("It should be no exceptions when sending one-way message.", t);
      fail("It should be no exceptions when sending one-way message.");
    }

    assertTrue(success.get(5, TimeUnit.SECONDS));
    assertEquals(3, numTries.get());
  }

  @Test
  public void testSendOneWayMessageWithHA() throws Exception {
    final RssConf conf = prepareForRssConfWithHA();

    final SettableFuture<Boolean> success = SettableFuture.create();

    prepareForRpcEnvWithHA(() -> {
      success.set(true);
      return Future$.MODULE$.successful(response);
    });

    RssHARetryClient client = new RssHARetryClient(rpcEnv, conf);
    HeartBeatFromApplication message = Mockito.mock(HeartBeatFromApplication.class);

    try {
      client.send(message);
    } catch (Throwable t) {
      LOG.error("It should be no exceptions when sending one-way message.", t);
      fail("It should be no exceptions when sending one-way message.");
    }

    assertTrue(success.get(5, TimeUnit.SECONDS));
  }

  @Test
  public void testSendMessageWithoutHA() {
    final RssConf conf = prepareForRssConfWithoutHA();

    prepareForEndpointRefWithoutRetry(() -> Future$.MODULE$.successful(mockResponse));
    prepareForRpcEnvWithoutHA();

    RssHARetryClient client = new RssHARetryClient(rpcEnv, conf);
    HeartbeatFromWorker message = Mockito.mock(HeartbeatFromWorker.class);

    HeartbeatResponse response = null;
    try {
      response = client.askSync(message, HeartbeatResponse.class);
    } catch (Throwable t) {
      LOG.error("It should be no exceptions when sending one-way message.", t);
      fail("It should be no exceptions when sending one-way message.");
    }

    assertEquals(mockResponse, response);
  }

  @Test
  public void testSendMessageWithoutHAWithRetry() {
    final AtomicInteger numTries = new AtomicInteger(0);
    final RssConf conf = prepareForRssConfWithoutHA();

    prepareForEndpointRefWithRetry(numTries, () -> Future$.MODULE$.successful(mockResponse));
    prepareForRpcEnvWithoutHA();

    RssHARetryClient client = new RssHARetryClient(rpcEnv, conf);
    HeartbeatFromWorker message = Mockito.mock(HeartbeatFromWorker.class);

    HeartbeatResponse response = null;
    try {
      response = client.askSync(message, HeartbeatResponse.class);
    } catch (Throwable t) {
      t.printStackTrace();
      LOG.error("It should be no exceptions when sending one-way message.", t);
      fail("It should be no exceptions when sending one-way message.");
    }

    assertEquals(mockResponse, response);
  }

  @Test
  public void testSendMessageWithHA() {
    final RssConf conf = prepareForRssConfWithHA();

    prepareForRpcEnvWithHA(() -> Future$.MODULE$.successful(mockResponse));

    RssHARetryClient client = new RssHARetryClient(rpcEnv, conf);
    HeartbeatFromWorker message = Mockito.mock(HeartbeatFromWorker.class);

    HeartbeatResponse response = null;
    try {
      response = client.askSync(message, HeartbeatResponse.class);
    } catch (Throwable t) {
      LOG.error("It should be no exceptions when sending one-way message.", t);
      fail("It should be no exceptions when sending one-way message.");
    }

    assertEquals(mockResponse, response);
  }

  @Test
  public void testOneMasterDownCausedByIOExceptionInHA() {
    checkOneMasterDownInHA(new IOException("test"));
  }

  @Test
  public void testOneMasterDownCausedByRuntimeExceptionInHA() {
    checkOneMasterDownInHA(new RuntimeException("test"));
  }

  private void checkOneMasterDownInHA(Exception causedByException) {
    final RssConf conf = prepareForRssConfWithHA();

    final RpcEndpointRef master1 = Mockito.mock(RpcEndpointRef.class);
    final RpcEndpointRef master3 = Mockito.mock(RpcEndpointRef.class);

    // master leader switch to host2
    Mockito.doReturn(Future$.MODULE$.failed(new MasterNotLeaderException("host1", "host2")))
        .when(master1).ask(Mockito.anyObject(), Mockito.anyObject(), Mockito.anyObject());

    Mockito.doReturn(Future$.MODULE$.successful(mockResponse))
        .when(master3).ask(Mockito.anyObject(), Mockito.anyObject(), Mockito.anyObject());

    Mockito.doAnswer((invocation) -> {
      RpcAddress address = invocation.getArgumentAt(0, RpcAddress.class);
      switch (address.host()) {
        case "host1": return master1;
        case "host2": throw new RssException("test", causedByException);
        case "host3": return master3;
        default:
          fail("Should use master host1/host2/host3:" + masterPort + ", but use " + address);
      }
      return null;
    }).when(rpcEnv).setupEndpointRef(Mockito.any(RpcAddress.class), Mockito.anyString());

    RssHARetryClient client = new RssHARetryClient(rpcEnv, conf);
    HeartbeatFromWorker message = Mockito.mock(HeartbeatFromWorker.class);

    HeartbeatResponse response = null;
    try {
      response = client.askSync(message, HeartbeatResponse.class);
    } catch (Throwable t) {
      LOG.error("It should be no exceptions when sending one-way message.", t);
      fail("It should be no exceptions when sending one-way message.");
    }

    assertEquals(mockResponse, response);
  }

  private void prepareForRpcEnvWithHA(final Supplier<Future<?>> supplier) {
    final RpcEndpointRef ref1 = Mockito.mock(RpcEndpointRef.class);
    final RpcEndpointRef ref2 = Mockito.mock(RpcEndpointRef.class);
    final RpcEndpointRef ref3 = Mockito.mock(RpcEndpointRef.class);

    // master leader switch to host2
    Mockito.doReturn(Future$.MODULE$.failed(new MasterNotLeaderException("host1", "host2")))
      .when(ref1).ask(Mockito.anyObject(), Mockito.anyObject(), Mockito.anyObject());
    // Assume host2 down.
    Mockito.doReturn(Future$.MODULE$.failed(new IOException("Test IOException")))
      .when(ref2).ask(Mockito.anyObject(), Mockito.anyObject(), Mockito.anyObject());
    // master leader switch to host3 after host2 down.
    Mockito.doReturn(supplier.get())
      .when(ref3).ask(Mockito.anyObject(), Mockito.anyObject(), Mockito.anyObject());

    Mockito.doAnswer(invocation -> {
      RpcAddress address = invocation.getArgumentAt(0, RpcAddress.class);
      if (address.port() == masterPort) {
        switch (address.host()) {
          case "host1": return ref1;
          case "host2": return ref2;
          case "host3": return ref3;
          default:
            fail("Should use master host1/host2/host3:" + masterPort + ", but use " + address);
        }
      } else {
        fail("Should use master host1/host2/host3:" + masterPort + ", but use " + address);
      }
      return null;
    }).when(rpcEnv).setupEndpointRef(Mockito.any(RpcAddress.class), Mockito.anyString());
  }

  private void prepareForRpcEnvWithoutHA() {
    Mockito.doAnswer((invocationOnMock) -> {
      RpcAddress address = invocationOnMock.getArgumentAt(0, RpcAddress.class);
      if (address.host().equals(masterHost) && address.port() == masterPort) {
        return endpointRef;
      } else {
        fail("Should only use master + " + masterHost + ":" + masterPort +  ", but use " + address);
        return null;
      }
    }).when(rpcEnv).setupEndpointRef(Mockito.any(RpcAddress.class), Mockito.anyString());
  }

  private void prepareForEndpointRefWithRetry(
      final AtomicInteger numTries, Supplier<Future<?>> supplier) {
    Mockito.doAnswer(invocation -> {
      switch (numTries.getAndIncrement()) {
        case 0: return Future$.MODULE$.failed(new IOException("Test 1"));
        case 1: return Future$.MODULE$.failed(new IOException("Test 2"));
        case 2: return supplier.get();
        default:
          return Future$.MODULE$.failed(new IllegalStateException("too many tries."));
      }
    }).when(endpointRef).ask(Mockito.anyObject(), Mockito.anyObject(), Mockito.anyObject());
  }

  private void prepareForEndpointRefWithoutRetry(Supplier<Future<?>> supplier) {
    Mockito.doAnswer(invocation -> supplier.get())
      .when(endpointRef)
      .ask(Mockito.anyObject(), Mockito.anyObject(), Mockito.anyObject());
  }

  private RssConf prepareForRssConfWithoutHA() {
    return rssConf.clone()
      .set("rss.ha.enabled", "false")
      .set("rss.master.host", masterHost);
  }

  private RssConf prepareForRssConfWithHA() {
    String masterHosts = "host1,host2,host3";
    return rssConf.clone()
      .set("rss.ha.enabled", "true")
      .set("rss.ha.master.hosts", masterHosts)
      .set("rss.ha.client.maxTries", "5");
  }
}
