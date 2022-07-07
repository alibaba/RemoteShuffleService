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

package com.aliyun.emr.rss.common.network.client;

import java.io.Closeable;
import java.net.SocketAddress;
import java.nio.ByteBuffer;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

import com.aliyun.emr.rss.common.network.protocol.*;
public class TransportClient implements Closeable {
  public TransportClient(Channel channel, TransportResponseHandler handler) {
  }

  public Channel getChannel() {
    return null;
  }

  public boolean isActive() {
    return true;
  }

  public SocketAddress getSocketAddress() {
    return null;
  }

  public String getClientId() {
    return null;
  }

  public void setClientId(String id) {
  }

  public void fetchChunk(
      long streamId,
      int chunkIndex,
      ChunkReceivedCallback callback) {
  }

  public long sendRpc(ByteBuffer message, RpcResponseCallback callback) {
    return 0;
  }

  public ChannelFuture pushData(PushData pushData, RpcResponseCallback callback) {
    return null;
  }

  public ChannelFuture pushMergedData(PushMergedData pushMergedData, RpcResponseCallback callback) {
    return null;
  }

  public ByteBuffer sendRpcSync(ByteBuffer message, long timeoutMs) {
    return null;
  }

  public void send(ByteBuffer message) {
  }

  public void removeRpcRequest(long requestId) {
  }

  public void timeOut() {
  }

  public TransportResponseHandler getHandler() {
    return null;
  }

  @Override
  public void close() {
  }

  @Override
  public String toString() {
    return null;
  }

  public static long requestId() {
    return 0;
  }
}
