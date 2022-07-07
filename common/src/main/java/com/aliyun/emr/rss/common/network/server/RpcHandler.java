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

package com.aliyun.emr.rss.common.network.server;

import java.nio.ByteBuffer;

import com.aliyun.emr.rss.common.network.client.RpcResponseCallback;
import com.aliyun.emr.rss.common.network.client.TransportClient;
import com.aliyun.emr.rss.common.network.protocol.PushData;
import com.aliyun.emr.rss.common.network.protocol.PushMergedData;

/**
 * Handler for sendRPC() messages sent by {@link TransportClient}s.
 */
public abstract class RpcHandler {
  public abstract void receive(
      TransportClient client,
      ByteBuffer message,
      RpcResponseCallback callback);

  public void receivePushData(
      TransportClient client,
      PushData pushData,
      RpcResponseCallback callback) {
    throw new UnsupportedOperationException();
  }

  public void receivePushMergedData(
      TransportClient client,
      PushMergedData pushMergedData,
      RpcResponseCallback callback) {
    throw new UnsupportedOperationException();
  }

  public void receive(TransportClient client, ByteBuffer message) {
  }

  public void channelActive(TransportClient client) { }

  public void channelInactive(TransportClient client) { }

  public void exceptionCaught(Throwable cause, TransportClient client) { }
}
