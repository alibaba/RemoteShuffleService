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

package com.aliyun.rss.common.network;

import java.util.ArrayList;
import java.util.List;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.mockito.Mockito;

import com.aliyun.rss.common.network.buffer.ManagedBuffer;
import com.aliyun.rss.common.network.client.TransportClient;
import com.aliyun.rss.common.network.protocol.*;
import com.aliyun.rss.common.network.server.*;

public class TransportRequestHandlerSuiteJ {

  @Test
  public void handleFetchRequest() throws Exception {
    RpcHandler rpcHandler = new NoOpRpcHandler();
    OneForOneStreamManager streamManager = (OneForOneStreamManager) (rpcHandler.getStreamManager());
    Channel channel = Mockito.mock(Channel.class);
    List<Pair<Object, ExtendedChannelPromise>> responseAndPromisePairs =
      new ArrayList<>();
    Mockito.when(channel.writeAndFlush(Mockito.any()))
      .thenAnswer(invocationOnMock0 -> {
        Object response = invocationOnMock0.getArguments()[0];
        ExtendedChannelPromise channelFuture = new ExtendedChannelPromise(channel);
        responseAndPromisePairs.add(ImmutablePair.of(response, channelFuture));
        return channelFuture;
      });

    // Prepare the stream.
    List<ManagedBuffer> managedBuffers = new ArrayList<>();
    managedBuffers.add(new TestManagedBuffer(10));
    managedBuffers.add(new TestManagedBuffer(20));
    managedBuffers.add(new TestManagedBuffer(30));
    managedBuffers.add(new TestManagedBuffer(40));

    ManagedBufferIterator iterator = Mockito.mock(ManagedBufferIterator.class);
    Mockito.when(iterator.chunk(Mockito.anyInt()))
      .thenReturn(managedBuffers.get(0))
      .thenReturn(managedBuffers.get(1))
      .thenReturn(managedBuffers.get(2))
      .thenReturn(managedBuffers.get(3));
    Mockito.when(iterator.hasNext())
      .thenReturn(true).thenReturn(true)
      .thenReturn(true).thenReturn(true)
      .thenReturn(true).thenReturn(true)
      .thenReturn(true).thenReturn(true)
      .thenReturn(false);

    long streamId = streamManager.registerStream("test-app", iterator, channel);

    assert streamManager.numStreamStates() == 1;

    TransportClient reverseClient = Mockito.mock(TransportClient.class);
    TransportRequestHandler requestHandler = new TransportRequestHandler(channel, reverseClient,
      rpcHandler, 2L);

    RequestMessage request0 = new ChunkFetchRequest(new StreamChunkId(streamId, 0));
    requestHandler.handle(request0);
    assert responseAndPromisePairs.size() == 1;
    assert responseAndPromisePairs.get(0).getLeft() instanceof ChunkFetchSuccess;
    assert ((ChunkFetchSuccess) (responseAndPromisePairs.get(0).getLeft())).body() ==
      managedBuffers.get(0);

    RequestMessage request1 = new ChunkFetchRequest(new StreamChunkId(streamId, 1));
    requestHandler.handle(request1);
    assert responseAndPromisePairs.size() == 2;
    assert responseAndPromisePairs.get(1).getLeft() instanceof ChunkFetchSuccess;
    assert ((ChunkFetchSuccess) (responseAndPromisePairs.get(1).getLeft())).body() ==
      managedBuffers.get(1);

    // Finish flushing the response for request0.
    responseAndPromisePairs.get(0).getRight().finish(true);

    assert responseAndPromisePairs.size() == 2;
  }

  private class ExtendedChannelPromise extends DefaultChannelPromise {

    private List<GenericFutureListener<Future<Void>>> listeners = new ArrayList<>();
    private boolean success;

    ExtendedChannelPromise(Channel channel) {
      super(channel);
      success = false;
    }

    @Override
    public ChannelPromise addListener(
        GenericFutureListener<? extends Future<? super Void>> listener) {
      @SuppressWarnings("unchecked")
      GenericFutureListener<Future<Void>> gfListener =
          (GenericFutureListener<Future<Void>>) listener;
      listeners.add(gfListener);
      return super.addListener(listener);
    }

    @Override
    public boolean isSuccess() {
      return success;
    }

    public void finish(boolean success) {
      this.success = success;
      listeners.forEach(listener -> {
        try {
          listener.operationComplete(this);
        } catch (Exception e) {
          // do nothing
        }
      });
    }
  }
}
