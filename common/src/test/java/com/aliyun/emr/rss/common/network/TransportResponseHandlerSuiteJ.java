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

import java.nio.ByteBuffer;

import io.netty.channel.local.LocalChannel;
import org.junit.Test;

import com.aliyun.emr.rss.common.network.buffer.NioManagedBuffer;
import com.aliyun.emr.rss.common.network.client.ChunkReceivedCallback;
import com.aliyun.emr.rss.common.network.client.RpcResponseCallback;
import com.aliyun.emr.rss.common.network.client.TransportResponseHandler;
import com.aliyun.emr.rss.common.network.protocol.*;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class TransportResponseHandlerSuiteJ {
  @Test
  public void handleSuccessfulFetch() throws Exception {
    StreamChunkSlice streamChunkSlice = new StreamChunkSlice(1, 0);

    TransportResponseHandler handler = new TransportResponseHandler(new LocalChannel());
    ChunkReceivedCallback callback = mock(ChunkReceivedCallback.class);
    handler.addFetchRequest(streamChunkSlice, callback);
    assertEquals(1, handler.numOutstandingRequests());

    handler.handle(new ChunkFetchSuccess(streamChunkSlice, new TestManagedBuffer(123)));
    verify(callback, times(1)).onSuccess(eq(0), any());
    assertEquals(0, handler.numOutstandingRequests());
  }

  @Test
  public void handleFailedFetch() throws Exception {
    StreamChunkSlice streamChunkSlice = new StreamChunkSlice(1, 0);
    TransportResponseHandler handler = new TransportResponseHandler(new LocalChannel());
    ChunkReceivedCallback callback = mock(ChunkReceivedCallback.class);
    handler.addFetchRequest(streamChunkSlice, callback);
    assertEquals(1, handler.numOutstandingRequests());

    handler.handle(new ChunkFetchFailure(streamChunkSlice, "some error msg"));
    verify(callback, times(1)).onFailure(eq(0), any());
    assertEquals(0, handler.numOutstandingRequests());
  }

  @Test
  public void clearAllOutstandingRequests() throws Exception {
    TransportResponseHandler handler = new TransportResponseHandler(new LocalChannel());
    ChunkReceivedCallback callback = mock(ChunkReceivedCallback.class);
    handler.addFetchRequest(new StreamChunkSlice(1, 0), callback);
    handler.addFetchRequest(new StreamChunkSlice(1, 1), callback);
    handler.addFetchRequest(new StreamChunkSlice(1, 2), callback);
    assertEquals(3, handler.numOutstandingRequests());

    handler.handle(new ChunkFetchSuccess(new StreamChunkSlice(1, 0), new TestManagedBuffer(12)));
    handler.exceptionCaught(new Exception("duh duh duhhhh"));

    // should fail both b2 and b3
    verify(callback, times(1)).onSuccess(eq(0), any());
    verify(callback, times(1)).onFailure(eq(1), any());
    verify(callback, times(1)).onFailure(eq(2), any());
    assertEquals(0, handler.numOutstandingRequests());
  }

  @Test
  public void handleSuccessfulRPC() throws Exception {
    TransportResponseHandler handler = new TransportResponseHandler(new LocalChannel());
    RpcResponseCallback callback = mock(RpcResponseCallback.class);
    handler.addRpcRequest(12345, callback);
    assertEquals(1, handler.numOutstandingRequests());

    // This response should be ignored.
    handler.handle(new RpcResponse(54321, new NioManagedBuffer(ByteBuffer.allocate(7))));
    assertEquals(1, handler.numOutstandingRequests());

    ByteBuffer resp = ByteBuffer.allocate(10);
    handler.handle(new RpcResponse(12345, new NioManagedBuffer(resp)));
    verify(callback, times(1)).onSuccess(eq(ByteBuffer.allocate(10)));
    assertEquals(0, handler.numOutstandingRequests());
  }

  @Test
  public void handleFailedRPC() throws Exception {
    TransportResponseHandler handler = new TransportResponseHandler(new LocalChannel());
    RpcResponseCallback callback = mock(RpcResponseCallback.class);
    handler.addRpcRequest(12345, callback);
    assertEquals(1, handler.numOutstandingRequests());

    handler.handle(new RpcFailure(54321, "uh-oh!")); // should be ignored
    assertEquals(1, handler.numOutstandingRequests());

    handler.handle(new RpcFailure(12345, "oh no"));
    verify(callback, times(1)).onFailure(any());
    assertEquals(0, handler.numOutstandingRequests());
  }
}
