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

import io.netty.channel.Channel;

import com.aliyun.emr.rss.common.network.client.TransportClient;
import com.aliyun.emr.rss.common.network.protocol.*;

public class TransportRequestHandler extends MessageHandler<RequestMessage> {

  public TransportRequestHandler(
      Channel channel,
      TransportClient reverseClient,
      RpcHandler rpcHandler,
      Long maxChunksBeingTransferred) {
  }

  @Override
  public void exceptionCaught(Throwable cause) {
  }

  @Override
  public void channelActive() {
  }

  @Override
  public void channelInactive() {
  }

  @Override
  public void handle(RequestMessage request) {
    if (request instanceof ChunkFetchRequest) {
    } else if (request instanceof RpcRequest) {
    } else if (request instanceof OneWayMessage) {
    } else if (request instanceof PushData) {
    } else if (request instanceof PushMergedData) {
    } else {
    }
  }
}
