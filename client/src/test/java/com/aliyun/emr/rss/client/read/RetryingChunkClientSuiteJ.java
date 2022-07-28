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

package com.aliyun.emr.rss.client.read;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.netty.channel.Channel;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import com.aliyun.emr.rss.common.RssConf;
import com.aliyun.emr.rss.common.network.buffer.ManagedBuffer;
import com.aliyun.emr.rss.common.network.buffer.NioManagedBuffer;
import com.aliyun.emr.rss.common.network.client.ChunkReceivedCallback;
import com.aliyun.emr.rss.common.network.client.TransportClient;
import com.aliyun.emr.rss.common.network.client.TransportClientFactory;
import com.aliyun.emr.rss.common.network.client.TransportResponseHandler;
import com.aliyun.emr.rss.common.network.protocol.StreamHandle;
import com.aliyun.emr.rss.common.protocol.PartitionLocation;
import com.aliyun.emr.rss.common.util.ThreadUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyObject;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class RetryingChunkClientSuiteJ {

  private static final int MASTER_RPC_PORT = 1234;
  private static final int MASTER_PUSH_PORT = 1235;
  private static final int MASTER_FETCH_PORT = 1236;
  private static final int MASTER_REPLICATE_PORT = 1237;
  private static final int SLAVE_RPC_PORT = 4321;
  private static final int SLAVE_PUSH_PORT = 4322;
  private static final int SLAVE_FETCH_PORT = 4323;
  private static final int SLAVE_REPLICATE_PORT = 4324;
  private static final PartitionLocation masterLocation = new PartitionLocation(
    0, 1, "localhost", MASTER_RPC_PORT, MASTER_PUSH_PORT, MASTER_FETCH_PORT,
    MASTER_REPLICATE_PORT, PartitionLocation.Mode.Master);
  private static final PartitionLocation slaveLocation = new PartitionLocation(
    0, 1, "localhost", SLAVE_RPC_PORT, SLAVE_PUSH_PORT, SLAVE_FETCH_PORT,
    SLAVE_REPLICATE_PORT, PartitionLocation.Mode.Slave);

  static {
    masterLocation.setPeer(slaveLocation);
    slaveLocation.setPeer(masterLocation);
  }

  ManagedBuffer chunk0 = new NioManagedBuffer(ByteBuffer.wrap(new byte[13]));
  ManagedBuffer chunk1 = new NioManagedBuffer(ByteBuffer.wrap(new byte[7]));
  ManagedBuffer chunk2 = new NioManagedBuffer(ByteBuffer.wrap(new byte[19]));

  @Test
  public void testNoFailures() throws IOException, InterruptedException {
    ChunkReceivedCallback callback = mock(ChunkReceivedCallback.class);
    Map<Integer, List<Object>> interactions = ImmutableMap.<Integer, List<Object>>builder()
        .put(0, Arrays.asList(chunk0))
        .put(1, Arrays.asList(chunk1))
        .put(2, Arrays.asList(chunk2))
        .build();

    RetryingChunkClient client = performInteractions(interactions, callback);

    verify(callback, timeout(5000)).onSuccess(eq(0), eq(chunk0));
    verify(callback, timeout(5000)).onSuccess(eq(1), eq(chunk1));
    verify(callback, timeout(5000)).onSuccess(eq(2), eq(chunk2));
    verifyNoMoreInteractions(callback);

    assertEquals(0, client.getNumTries());
    assertEquals(masterLocation, client.getCurrentReplica().getLocation());
  }

  @Test
  public void testUnrecoverableFailure() throws IOException, InterruptedException {
    ChunkReceivedCallback callback = mock(ChunkReceivedCallback.class);
    Map<Integer, List<Object>> interactions = ImmutableMap.<Integer, List<Object>>builder()
        .put(0, Arrays.asList(new RuntimeException("Ouch!")))
        .put(1, Arrays.asList(chunk1))
        .put(2, Arrays.asList(chunk2))
        .build();
    RetryingChunkClient client = performInteractions(interactions, callback);

    verify(callback, timeout(5000)).onFailure(eq(0), any());
    verify(callback, timeout(5000)).onSuccess(eq(1), eq(chunk1));
    verify(callback, timeout(5000)).onSuccess(eq(2), eq(chunk2));
    verifyNoMoreInteractions(callback);

    assertEquals(0, client.getNumTries());
    assertEquals(masterLocation, client.getCurrentReplica().getLocation());
  }

  @Test
  public void testDuplicateSuccess() throws IOException, InterruptedException {
    ChunkReceivedCallback callback = mock(ChunkReceivedCallback.class);

    Map<Integer, List<Object>> interactions = ImmutableMap.<Integer, List<Object>>builder()
        .put(0, Arrays.asList(chunk0, chunk1))
        .build();
    RetryingChunkClient client = performInteractions(interactions, callback);
    verify(callback, timeout(5000)).onSuccess(eq(0), eq(chunk0));
    verifyNoMoreInteractions(callback);

    assertEquals(0, client.getNumTries());
    assertEquals(masterLocation, client.getCurrentReplica().getLocation());
  }

  @Test
  public void testSingleIOException() throws IOException, InterruptedException {
    Map<Integer, Object> result = new ConcurrentHashMap<>();
    ChunkReceivedCallback callback = mock(ChunkReceivedCallback.class);
    Semaphore signal = new Semaphore(3);
    signal.acquire(3);

    Answer<Void> answer = invocation -> {
      synchronized (signal) {
        int chunkIndex = (Integer) invocation.getArguments()[0];
        assertFalse(result.containsKey(chunkIndex));
        Object value = invocation.getArguments()[1];
        result.put(chunkIndex, value);
        signal.release();
      }
      return null;
    };
    doAnswer(answer).when(callback).onSuccess(anyInt(), anyObject());
    doAnswer(answer).when(callback).onFailure(anyInt(), anyObject());

    Map<Integer, List<Object>> interactions = ImmutableMap.<Integer, List<Object>>builder()
      .put(0, Arrays.asList(new IOException(), chunk0))
      .put(1, Arrays.asList(chunk1))
      .put(2, Arrays.asList(chunk2))
      .build();
    RetryingChunkClient client = performInteractions(interactions, callback);

    while (!signal.tryAcquire(3, 500, TimeUnit.MILLISECONDS));
    assertEquals(1, client.getNumTries());
    assertEquals(slaveLocation, client.getCurrentReplica().getLocation());
    assertEquals(chunk0, result.get(0));
    assertEquals(chunk1, result.get(1));
    assertEquals(chunk2, result.get(2));
  }

  @Test
  public void testTwoIOExceptions() throws IOException, InterruptedException {
    Map<Integer, Object> result = new ConcurrentHashMap<>();
    ChunkReceivedCallback callback = mock(ChunkReceivedCallback.class);
    Semaphore signal = new Semaphore(3);
    signal.acquire(3);

    Answer<Void> answer = invocation -> {
      synchronized (signal) {
        int chunkIndex = (Integer) invocation.getArguments()[0];
        assertFalse(result.containsKey(chunkIndex));
        Object value = invocation.getArguments()[1];
        result.put(chunkIndex, value);
        signal.release();
      }
      return null;
    };
    doAnswer(answer).when(callback).onSuccess(anyInt(), anyObject());
    doAnswer(answer).when(callback).onFailure(anyInt(), anyObject());

    Map<Integer, List<Object>> interactions = ImmutableMap.<Integer, List<Object>>builder()
      .put(0, Arrays.asList(new IOException("first ioexception"), chunk0))
      .put(1, Arrays.asList(new IOException("second ioexception"), chunk1))
      .put(2, Arrays.asList(chunk2))
      .build();
    RetryingChunkClient client = performInteractions(interactions, callback);

    while (!signal.tryAcquire(3, 500, TimeUnit.MILLISECONDS));
    assertEquals(1, client.getNumTries());
    assertEquals(slaveLocation, client.getCurrentReplica().getLocation());
    assertEquals(chunk0, result.get(0));
    assertEquals(chunk1, result.get(1));
    assertEquals(chunk2, result.get(2));
  }

  @Test
  public void testThreeIOExceptions() throws IOException, InterruptedException {
    Map<Integer, Object> result = new ConcurrentHashMap<>();
    ChunkReceivedCallback callback = mock(ChunkReceivedCallback.class);
    Semaphore signal = new Semaphore(3);
    signal.acquire(3);

    Answer<Void> answer = invocation -> {
      synchronized (signal) {
        int chunkIndex = (Integer) invocation.getArguments()[0];
        assertFalse(result.containsKey(chunkIndex));
        Object value = invocation.getArguments()[1];
        result.put(chunkIndex, value);
        signal.release();
      }
      return null;
    };
    doAnswer(answer).when(callback).onSuccess(anyInt(), anyObject());
    doAnswer(answer).when(callback).onFailure(anyInt(), anyObject());

    Map<Integer, List<Object>> interactions = ImmutableMap.<Integer, List<Object>>builder()
      .put(0, Arrays.asList(new IOException("first ioexception"), chunk0))
      .put(1, Arrays.asList(new IOException("second ioexception"), chunk1))
      .put(2, Arrays.asList(new IOException("third ioexception"), chunk2))
      .build();

    RetryingChunkClient client = performInteractions(interactions, callback);
    while (!signal.tryAcquire(3, 500, TimeUnit.MILLISECONDS));
    assertEquals(1, client.getNumTries());
    assertEquals(slaveLocation, client.getCurrentReplica().getLocation());
    assertEquals(chunk0, result.get(0));
    assertEquals(chunk1, result.get(1));
    assertEquals(chunk2, result.get(2));
  }

  @Test
  public void testFailedWithIOExceptions() throws IOException, InterruptedException {
    Map<Integer, Object> result = new ConcurrentHashMap<>();
    ChunkReceivedCallback callback = mock(ChunkReceivedCallback.class);
    Semaphore signal = new Semaphore(3);
    signal.acquire(3);

    Answer<Void> answer = invocation -> {
      synchronized (signal) {
        int chunkIndex = (Integer) invocation.getArguments()[0];
        assertFalse(result.containsKey(chunkIndex));
        Object value = invocation.getArguments()[1];
        result.put(chunkIndex, value);
        signal.release();
      }
      return null;
    };
    doAnswer(answer).when(callback).onSuccess(anyInt(), anyObject());
    doAnswer(answer).when(callback).onFailure(anyInt(), anyObject());

    IOException ioe = new IOException("failed exception");
    Map<Integer, List<Object>> interactions = ImmutableMap.<Integer, List<Object>>builder()
      .put(0, Arrays.asList(ioe, ioe, ioe, ioe, ioe, chunk0))
      .put(1, Arrays.asList(ioe, ioe, ioe, ioe, ioe, chunk1))
      .put(2, Arrays.asList(ioe, ioe, ioe, ioe, ioe, chunk2))
      .build();

    RetryingChunkClient client = performInteractions(interactions, callback);
    while (!signal.tryAcquire(3, 500, TimeUnit.MILLISECONDS));
    // Note: this may exceeds the max retries we want, but it doesn't master.
    assertEquals(4, client.getNumTries());
    assertEquals(masterLocation, client.getCurrentReplica().getLocation());
    assertEquals(ioe, result.get(0));
    assertEquals(ioe, result.get(1));
    assertEquals(ioe, result.get(2));
  }

  @Test
  public void testRetryAndUnrecoverable() throws IOException, InterruptedException {
    Map<Integer, Object> result = new ConcurrentHashMap<>();
    ChunkReceivedCallback callback = mock(ChunkReceivedCallback.class);
    Semaphore signal = new Semaphore(3);
    signal.acquire(3);

    Answer<Void> answer = invocation -> {
      synchronized (signal) {
        int chunkIndex = (Integer) invocation.getArguments()[0];
        assertFalse(result.containsKey(chunkIndex));
        Object value = invocation.getArguments()[1];
        result.put(chunkIndex, value);
        signal.release();
      }
      return null;
    };
    doAnswer(answer).when(callback).onSuccess(anyInt(), anyObject());
    doAnswer(answer).when(callback).onFailure(anyInt(), anyObject());

    Exception re = new RuntimeException("failed exception");
    Map<Integer, List<Object>> interactions = ImmutableMap.<Integer, List<Object>>builder()
      .put(0, Arrays.asList(new IOException("first ioexception"), re, chunk0))
      .put(1, Arrays.asList(chunk1))
      .put(2, Arrays.asList(new IOException("second ioexception"), chunk2))
      .build();

    performInteractions(interactions, callback);
    while (!signal.tryAcquire(3, 500, TimeUnit.MILLISECONDS));
    assertEquals(re, result.get(0));
    assertEquals(chunk1, result.get(1));
    assertEquals(chunk2, result.get(2));
  }

  private static RetryingChunkClient performInteractions(
      Map<Integer, List<Object>> interactions,
      ChunkReceivedCallback callback) throws IOException, InterruptedException {
    RssConf conf = new RssConf();
    conf.set("rss.data.io.maxRetries", "1");
    conf.set("rss.data.io.retryWait", "0");

    // Contains all chunk ids that are referenced across all interactions.
    LinkedHashSet<Integer> chunkIds = Sets.newLinkedHashSet(interactions.keySet());

    final TransportClient client = new DummyTransportClient(chunkIds.size(), interactions);
    final TransportClientFactory clientFactory = mock(TransportClientFactory.class);
    doAnswer(invocation -> client).when(clientFactory).createClient(anyString(), anyInt());

    RetryingChunkClient retryingChunkClient = new RetryingChunkClient(
        conf, "test", masterLocation, callback, clientFactory);
    chunkIds.stream().sorted().forEach(retryingChunkClient::fetchChunk);
    return retryingChunkClient;
  }

  private static class DummyTransportClient extends TransportClient {

    private static final Channel channel = mock(Channel.class);
    private static final TransportResponseHandler handler = mock(TransportResponseHandler.class);

    private final long streamId = new Random().nextInt(Integer.MAX_VALUE) * 1000L;
    private final int numChunks;
    private final Map<Integer, List<Object>> interactions;
    private final Map<Integer, Integer> chunkIdToInterActionIndex;

    private final ScheduledExecutorService schedule =
      ThreadUtils.newDaemonThreadPoolScheduledExecutor("test-fetch-chunk", 3);

    DummyTransportClient(int numChunks, Map<Integer, List<Object>> interactions) {
      super(channel, handler);
      this.numChunks = numChunks;
      this.interactions = interactions;
      this.chunkIdToInterActionIndex = new ConcurrentHashMap<>();
      interactions.keySet().forEach((chunkId) -> chunkIdToInterActionIndex.putIfAbsent(chunkId, 0));
    }

    @Override
    public void fetchChunk(long streamId, int chunkId, ChunkReceivedCallback callback) {
      schedule.schedule(() -> {
        Object action;
        List<Object> interaction = interactions.get(chunkId);
        synchronized (chunkIdToInterActionIndex) {
          int index = chunkIdToInterActionIndex.get(chunkId);
          assertTrue(index < interaction.size());
          action = interaction.get(index);
          chunkIdToInterActionIndex.put(chunkId, index + 1);
        }

        if (action instanceof ManagedBuffer) {
          callback.onSuccess(chunkId, (ManagedBuffer) action);
        } else if (action instanceof Exception) {
          callback.onFailure(chunkId, (Exception) action);
        } else {
          fail("Can only handle ManagedBuffers and Exceptions, got " + action);
        }
      }, 500, TimeUnit.MILLISECONDS);
    }

    @Override
    public ByteBuffer sendRpcSync(ByteBuffer message, long timeoutMs) {
      StreamHandle handle = new StreamHandle(streamId, numChunks);
      return handle.toByteBuffer();
    }

    @Override
    public void close() {
      super.close();
      schedule.shutdownNow();
    }
  }
}
