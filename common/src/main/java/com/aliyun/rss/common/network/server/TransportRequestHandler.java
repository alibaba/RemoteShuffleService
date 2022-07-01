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

package com.aliyun.rss.common.network.server;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;

import com.google.common.base.Throwables;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.rss.common.metrics.source.AbstractSource;
import com.aliyun.rss.common.metrics.source.NetWorkSource;
import com.aliyun.rss.common.network.buffer.ManagedBuffer;
import com.aliyun.rss.common.network.buffer.NioManagedBuffer;
import com.aliyun.rss.common.network.client.RpcResponseCallback;
import com.aliyun.rss.common.network.client.TransportClient;
import com.aliyun.rss.common.network.protocol.*;
import com.aliyun.rss.common.network.util.NettyUtils;
import com.aliyun.rss.common.util.JavaUtils;

/**
 * A handler that processes requests from clients and writes chunk data back. Each handler is
 * attached to a single Netty channel, and keeps track of which streams have been fetched via this
 * channel, in order to clean them up if the channel is terminated (see #channelUnregistered).
 *
 * The messages should have been processed by the pipeline setup by {@link TransportServer}.
 */
public class TransportRequestHandler extends MessageHandler<RequestMessage> {

  private static final Logger logger = LoggerFactory.getLogger(TransportRequestHandler.class);

  /** The Netty channel that this handler is associated with. */
  private final Channel channel;

  /** Client on the same channel allowing us to talk back to the requester. */
  private final TransportClient reverseClient;

  /** Handles all RPC messages. */
  private final RpcHandler rpcHandler;

  /** Returns each chunk part of a stream. */
  private final StreamManager streamManager;

  /** The max number of chunks being transferred and not finished yet. */
  private final long maxChunksBeingTransferred;

  private AbstractSource source = null;

  public TransportRequestHandler(
      Channel channel,
      TransportClient reverseClient,
      RpcHandler rpcHandler,
      Long maxChunksBeingTransferred,
      AbstractSource source){
    this(channel, reverseClient, rpcHandler, maxChunksBeingTransferred);
    this.source = source;
  }

  public TransportRequestHandler(
      Channel channel,
      TransportClient reverseClient,
      RpcHandler rpcHandler,
      Long maxChunksBeingTransferred) {
    this.channel = channel;
    this.reverseClient = reverseClient;
    this.rpcHandler = rpcHandler;
    this.streamManager = rpcHandler.getStreamManager();
    this.maxChunksBeingTransferred = maxChunksBeingTransferred;
  }

  @Override
  public void exceptionCaught(Throwable cause) {
    rpcHandler.exceptionCaught(cause, reverseClient);
  }

  @Override
  public void channelActive() {
    rpcHandler.channelActive(reverseClient);
  }

  @Override
  public void channelInactive() {
    if (streamManager != null) {
      try {
        streamManager.connectionTerminated(channel);
      } catch (RuntimeException e) {
        logger.error("StreamManager connectionTerminated() callback failed.", e);
      }
    }
    rpcHandler.channelInactive(reverseClient);
  }

  @Override
  public void handle(RequestMessage request) {
    if (request instanceof ChunkFetchRequest) {
      if (checkRegistered(request)) {
        processFetchRequest((ChunkFetchRequest) request);
      }
    } else if (request instanceof RpcRequest) {
      if (checkRegistered(request)) {
        processRpcRequest((RpcRequest) request);
      }
    } else if (request instanceof OneWayMessage) {
      if (checkRegistered(request)) {
        processOneWayMessage((OneWayMessage) request);
      }
    } else if (request instanceof PushData) {
      if (checkRegistered(request)) {
        processPushData((PushData) request);
      }
    } else if (request instanceof PushMergedData) {
      if (checkRegistered(request)) {
        processPushMergedData((PushMergedData) request);
      }
    } else {
      throw new IllegalArgumentException("Unknown request type: " + request);
    }
  }

  private boolean checkRegistered(RequestMessage req) {
    if (!rpcHandler.checkRegistered()) {
      IOException e = new IOException("Worker Not Registered!");
      if (req instanceof RpcRequest) {
        respond(new RpcFailure(((RpcRequest)req).requestId, Throwables.getStackTraceAsString(e)));
      } else if (req instanceof ChunkFetchRequest) {
        respond(new ChunkFetchFailure(((ChunkFetchRequest)req).streamChunkId,
            Throwables.getStackTraceAsString(e)));
      } else if (req instanceof OneWayMessage) {
        logger.warn("Ignore OneWayMessage since worker is not registered!");
      }
      return false;
    }
    return true;
  }

  private void processFetchRequest(final ChunkFetchRequest req) {
    if (source != null) {
      source.startTimer(NetWorkSource.FetchChunkTime(), req.toString());
    }
    if (logger.isTraceEnabled()) {
      logger.trace("Received req from {} to fetch block {}", NettyUtils.getRemoteAddress(channel),
        req.streamChunkId);
    }
    long chunksBeingTransferred = streamManager.chunksBeingTransferred();
    if (chunksBeingTransferred >= maxChunksBeingTransferred) {
      logger.warn("The number of chunks being transferred {} is above {}, close the connection.",
        chunksBeingTransferred, maxChunksBeingTransferred);
      channel.close();
      if (source != null) {
        source.stopTimer(NetWorkSource.FetchChunkTime(), req.toString());
      }
      return;
    }
    ManagedBuffer buf;
    try {
      streamManager.checkAuthorization(reverseClient, req.streamChunkId.streamId);
      buf = streamManager.getChunk(req.streamChunkId.streamId, req.streamChunkId.chunkIndex);
    } catch (Exception e) {
      logger.error(String.format("Error opening block %s for request from %s",
        req.streamChunkId, NettyUtils.getRemoteAddress(channel)), e);
      respond(new ChunkFetchFailure(req.streamChunkId, Throwables.getStackTraceAsString(e)));
      if (source != null) {
        source.stopTimer(NetWorkSource.FetchChunkTime(), req.toString());
      }
      return;
    }

    streamManager.chunkBeingSent(req.streamChunkId.streamId);
    respond(new ChunkFetchSuccess(req.streamChunkId, buf)).addListener(future -> {
      streamManager.chunkSent(req.streamChunkId.streamId);
      if (source != null) {
        source.stopTimer(NetWorkSource.FetchChunkTime(), req.toString());
      }
    });
  }

  private void processRpcRequest(final RpcRequest req) {
    try {
      rpcHandler.receive(reverseClient, req.body().nioByteBuffer(), new RpcResponseCallback() {
        @Override
        public void onSuccess(ByteBuffer response) {
          respond(new RpcResponse(req.requestId, new NioManagedBuffer(response)));
        }

        @Override
        public void onFailure(Throwable e) {
          respond(new RpcFailure(req.requestId, Throwables.getStackTraceAsString(e)));
        }
      });
    } catch (Exception e) {
      logger.error("Error while invoking RpcHandler#receive() on RPC id " + req.requestId, e);
      respond(new RpcFailure(req.requestId, Throwables.getStackTraceAsString(e)));
    } finally {
      req.body().release();
    }
  }

  private void processPushData(PushData req) {
    try {
      rpcHandler.receivePushData(reverseClient, req, new RpcResponseCallback() {
        @Override
        public void onSuccess(ByteBuffer response) {
          respond(new RpcResponse(req.requestId, new NioManagedBuffer(response)));
        }

        @Override
        public void onFailure(Throwable e) {
          logger.error("[processPushData] Process pushData onFailure! ShuffleKey: "
                  + req.shuffleKey + ", partitionUniqueId: " + req.partitionUniqueId, e);
          respond(new RpcFailure(req.requestId, e.getMessage()));
        }
      });
    } catch (Exception e) {
      logger.error("Error while invoking RpcHandler#receive() on PushData " + req, e);
      channel.writeAndFlush(new RpcFailure(req.requestId, Throwables.getStackTraceAsString(e)));
    } finally {
      req.body().release();
    }
  }

  private void processPushMergedData(PushMergedData req) {
    try {
      rpcHandler.receivePushMergedData(reverseClient, req, new RpcResponseCallback() {
        @Override
        public void onSuccess(ByteBuffer response) {
          respond(new RpcResponse(req.requestId, new NioManagedBuffer(response)));
        }

        @Override
        public void onFailure(Throwable e) {
          logger.error("[processPushMergedData] Process PushMergedData onFailure! ShuffleKey: " +
                  req.shuffleKey +
                  ", partitionUniqueId: " + JavaUtils.mkString(req.partitionUniqueIds, ","), e);
          respond(new RpcFailure(req.requestId, e.getMessage()));
        }
      });
    } catch (Exception e) {
      logger.error("Error while invoking RpcHandler#receive() on PushMergedData " + req, e);
      channel.writeAndFlush(new RpcFailure(req.requestId, Throwables.getStackTraceAsString(e)));
    } finally {
      req.body().release();
    }
  }

  private void processOneWayMessage(OneWayMessage req) {
    try {
      rpcHandler.receive(reverseClient, req.body().nioByteBuffer());
    } catch (Exception e) {
      logger.error("Error while invoking RpcHandler#receive() for one-way message.", e);
    } finally {
      req.body().release();
    }
  }

  /**
   * Responds to a single message with some Encodable object. If a failure occurs while sending,
   * it will be logged and the channel closed.
   */
  private ChannelFuture respond(Encodable result) {
    SocketAddress remoteAddress = channel.remoteAddress();
    return channel.writeAndFlush(result).addListener(future -> {
      if (future.isSuccess()) {
        logger.trace("Sent result {} to client {}", result, remoteAddress);
      } else {
        logger.warn(String.format("Fail to sending result %s to %s; closing connection",
          result, remoteAddress), future.cause());
        channel.close();
      }
    });
  }
}
