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

package com.aliyun.emr.rss.service.deploy.worker;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.emr.rss.common.exception.RssException;
import com.aliyun.emr.rss.common.metrics.source.AbstractSource;
import com.aliyun.emr.rss.common.network.client.RpcResponseCallback;
import com.aliyun.emr.rss.common.network.client.TransportClient;
import com.aliyun.emr.rss.common.network.server.FileInfo;
import com.aliyun.emr.rss.common.network.server.FileManagedBuffers;
import com.aliyun.emr.rss.common.network.server.OneForOneStreamManager;
import com.aliyun.emr.rss.common.network.server.RpcHandler;
import com.aliyun.emr.rss.common.network.server.StreamManager;
import com.aliyun.emr.rss.common.network.util.TransportConf;

public final class ChunkFetchRpcHandler extends RpcHandler {

  private static final Logger logger = LoggerFactory.getLogger(ChunkFetchRpcHandler.class);

  private final TransportConf conf;
  private final OpenStreamHandler handler;
  private final OneForOneStreamManager streamManager;
  private final AbstractSource source; // metrics

  public ChunkFetchRpcHandler(TransportConf conf, AbstractSource source, OpenStreamHandler handler
  ) {
    this.conf = conf;
    this.handler = handler;
    this.streamManager = new OneForOneStreamManager();
    this.source = source;
  }

  private String readString(ByteBuffer buffer) {
    int length = buffer.getInt();
    byte[] bytes = new byte[length];
    buffer.get(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  @Override
  public void receive(TransportClient client, ByteBuffer message, RpcResponseCallback callback) {
    String shuffleKey = readString(message);
    String fileName = readString(message);
    int startMapIndex = message.getInt();
    int endMapIndex = message.getInt();

    // metrics start
    source.startTimer(WorkerSource.OPEN_STREAM_TIME(), shuffleKey);
    FileInfo fileInfo = handler.handleOpenStream(shuffleKey, fileName, startMapIndex, endMapIndex);

    if (fileInfo != null) {
      logger.debug("Received chunk fetch request {} {} {} {} get file info {}", shuffleKey,
        fileName, startMapIndex, endMapIndex, fileInfo);
      try {
        FileManagedBuffers buffers = new FileManagedBuffers(fileInfo, conf);
        long streamId = streamManager.registerStream(
            client.getClientId(), buffers, client.getChannel());

        ByteBuffer response = ByteBuffer.allocate(8 + 4);
        response.putLong(streamId);
        response.putInt(fileInfo.numChunks);
        if (fileInfo.numChunks == 0) {
          logger.debug("StreamId {} fileName {} startMapIndex {} endMapIndex {} is empty.",
            streamId, fileName, startMapIndex, endMapIndex);
        }
        response.flip();
        callback.onSuccess(response);
      } catch (IOException e) {
        callback.onFailure(
            new RssException("Chunk offsets meta exception ", e));
      } finally {
        // metrics end
        source.stopTimer(WorkerSource.OPEN_STREAM_TIME(), shuffleKey);
      }
    } else {
      // metrics end
      source.stopTimer(WorkerSource.OPEN_STREAM_TIME(), shuffleKey);

      callback.onFailure(new FileNotFoundException());
    }
  }

  @Override
  public boolean checkRegistered() {
    return ((Registerable) handler).isRegistered();
  }

  @Override
  public void channelInactive(TransportClient client) {
    logger.debug("channel Inactive " + client.getSocketAddress());
  }

  @Override
  public void exceptionCaught(Throwable cause, TransportClient client) {
    logger.debug("exception caught " + cause + " " + client.getSocketAddress());
  }

  @Override
  public StreamManager getStreamManager() {
    return streamManager;
  }
}
