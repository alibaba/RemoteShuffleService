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

package com.aliyun.emr.rss.common.network;

import java.util.List;

import io.netty.channel.ChannelHandler;

import com.aliyun.emr.rss.common.network.client.TransportClientBootstrap;
import com.aliyun.emr.rss.common.network.client.TransportClientFactory;
import com.aliyun.emr.rss.common.network.server.*;
import com.aliyun.emr.rss.common.network.util.TransportConf;

public class TransportContext {
  public TransportContext(
      TransportConf conf,
      RpcHandler rpcHandler,
      boolean closeIdleConnections,
      ChannelHandler channelHandler) {
  }

  public TransportContext(
      TransportConf conf,
      RpcHandler rpcHandler,
      boolean closeIdleConnections) {
  }

  public TransportContext(TransportConf conf, RpcHandler rpcHandler) {
  }

  public TransportClientFactory createClientFactory(List<TransportClientBootstrap> bootstraps) {
    return null;
  }

  public TransportClientFactory createClientFactory() {
    return null;
  }

  /** Create a server which will attempt to bind to a specific port. */
  public TransportServer createServer(int port, List<TransportServerBootstrap> bootstraps) {
    return null;
  }

  /** Create a server which will attempt to bind to a specific host and port. */
  public TransportServer createServer(
      String host, int port, List<TransportServerBootstrap> bootstraps) {
    return null;
  }

  public TransportServer createServer() {
    return null;
  }
}
