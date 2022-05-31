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

package com.aliyun.emr.rss.client.write;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import com.aliyun.emr.rss.client.ShuffleClient;
import com.aliyun.emr.rss.common.RssConf;

public class DataPusher {
  private final long WAIT_TIME_NANOS = TimeUnit.MILLISECONDS.toNanos(500);

  private final LinkedBlockingQueue<DataTask> idleQueue;
  private final LinkedBlockingQueue<DataTask> workingQueue;

  private final ReentrantLock idleLock = new ReentrantLock();
  private final Condition idleFull = idleLock.newCondition();

  private final AtomicReference<IOException> exception = new AtomicReference<>();

  private final String appId;
  private final int shuffleId;
  private final int mapId;
  private final int attemptId;
  private final int numMappers;
  private final int numPartitions;
  private final ShuffleClient client;
  private final Consumer<Integer> afterPush;

  private volatile boolean terminated;
  private LongAdder[] mapStatusLengths;

  private Map<Integer, CompositeBuffer> compositeBuffers = new ConcurrentHashMap<>();
  private LongAdder totalCompositeSize = new LongAdder();
  private final int MAX_COMPOSITE_SIZE;
  private final int SEND_BUFFER_SIZE;
  private final DataTask FlushTask = new DataTask(0);

  public DataPusher(
      String appId,
      int shuffleId,
      int mapId,
      int attemptId,
      long taskId,
      int numMappers,
      int numPartitions,
      RssConf conf,
      ShuffleClient client,
      Consumer<Integer> afterPush,
      LongAdder[] mapStatusLengths) throws IOException {
    final int capacity = RssConf.pushDataQueueCapacity(conf);
    final int bufferSize = RssConf.pushDataBufferSize(conf);
    MAX_COMPOSITE_SIZE = RssConf.pushDataMaxCompositeSize(conf);
    SEND_BUFFER_SIZE = RssConf.pushDataBufferSize(conf);

    idleQueue = new LinkedBlockingQueue<>(capacity);
    workingQueue = new LinkedBlockingQueue<>(capacity);

    for (int i = 0; i < capacity; i++) {
      try {
        idleQueue.put(new DataTask(bufferSize));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      }
    }

    this.appId = appId;
    this.shuffleId = shuffleId;
    this.mapId = mapId;
    this.attemptId = attemptId;
    this.numMappers = numMappers;
    this.numPartitions = numPartitions;
    this.client = client;
    this.afterPush = afterPush;
    this.mapStatusLengths = mapStatusLengths;

    new Thread("DataPusher-" + taskId) {
      private void reclaimTask(DataTask task) throws InterruptedException {
        idleLock.lockInterruptibly();
        try {
          idleQueue.put(task);
          if (idleQueue.remainingCapacity() == 0) {
            idleFull.signal();
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          exception.set(new IOException(e));
        } finally {
          idleLock.unlock();
        }
      }

      @Override
      public void run() {
        while (!terminated && exception.get() == null) {
          try {
            DataTask task = workingQueue.poll(WAIT_TIME_NANOS, TimeUnit.NANOSECONDS);
            if (task == null) {
              continue;
            }
            processData(task);
            reclaimTask(task);
          } catch (InterruptedException e) {
            exception.set(new IOException(e));
          } catch (IOException e) {
            exception.set(e);
          }
        }
      }
    }.start();
  }

  public void addTask(int partitionId, byte[] buffer, int size) throws IOException {
    try {
      DataTask task = null;
      while (task == null) {
        checkException();
        task = idleQueue.poll(WAIT_TIME_NANOS, TimeUnit.NANOSECONDS);
      }
      task.setSize(size);
      task.setPartitionId(partitionId);
      System.arraycopy(buffer, 0, task.getBuffer(), 0, size);
      while (!workingQueue.offer(task, WAIT_TIME_NANOS, TimeUnit.NANOSECONDS)) {
        checkException();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      IOException ioe = new IOException(e);
      exception.set(ioe);
      throw ioe;
    }
  }

  private void addFlushTask() throws IOException {
    try {
      DataTask task = null;
      while (task == null) {
        checkException();
        task = idleQueue.poll(WAIT_TIME_NANOS, TimeUnit.NANOSECONDS);
      }
      task = FlushTask;
      while (!workingQueue.offer(task, WAIT_TIME_NANOS, TimeUnit.NANOSECONDS)) {
        checkException();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      IOException ioe = new IOException(e);
      exception.set(ioe);
      throw ioe;
    }
  }

  public void waitOnTermination() throws IOException {
    try {
      addFlushTask();
      idleLock.lockInterruptibly();
      waitIdleQueueFullWithLock();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      exception.set(new IOException(e));
    }

    terminated = true;
    compositeBuffers.clear();
    idleQueue.clear();
    workingQueue.clear();
    checkException();
  }

  private void checkException() throws IOException {
    if (exception.get() != null) {
      throw exception.get();
    }
  }

  private void processData(DataTask task) throws IOException {
    if (task.equals(FlushTask)) {
      flush();
      return;
    }

    int partitionId = task.getPartitionId();
    byte[] data = task.getBuffer();
    CompositeBuffer compositeBuf = compositeBuffers.computeIfAbsent(partitionId,
      (s) -> new CompositeBuffer());

    CompressedBuffer compressData = client.compressData(shuffleId, mapId, attemptId,
      data, 0, task.getSize());
    int bytesWritten = compressData.getSize();
    compositeBuf.add(compressData);
    totalCompositeSize.add(bytesWritten);
    afterPush.accept(bytesWritten);
    mapStatusLengths[task.getPartitionId()].add(bytesWritten);

    if (compositeBuf.getSize() > SEND_BUFFER_SIZE) {
      int bufferSize = compositeBuf.getSize();
      client.pushData(
        appId,
        shuffleId,
        mapId,
        attemptId,
        partitionId,
        0,
        bufferSize,
        numMappers,
        numPartitions,
        true,
        compositeBuf.toArray()
      );
      totalCompositeSize.add(-1 * bufferSize);
    }

    if (totalCompositeSize.sum() > MAX_COMPOSITE_SIZE) {
      flush();
    }
  }

  private void flush() throws IOException {
    for (Map.Entry<Integer, CompositeBuffer> entry : compositeBuffers.entrySet()) {
      int partitionId = entry.getKey();
      CompositeBuffer compositeBuffer = entry.getValue();
      int bufferSize = compositeBuffer.getSize();
      if (bufferSize > 0) {
        client.pushData(
          appId,
          shuffleId,
          mapId,
          attemptId,
          partitionId,
          0,
          bufferSize,
          numMappers,
          numPartitions,
          true,
          compositeBuffer.toArray()
        );
      }
    }
    totalCompositeSize.reset();
  }

  private void waitIdleQueueFullWithLock() {
    try {
      while (idleQueue.remainingCapacity() > 0 && exception.get() == null) {
        idleFull.await(WAIT_TIME_NANOS, TimeUnit.NANOSECONDS);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      exception.set(new IOException(e));
    } finally {
      idleLock.unlock();
    }
  }
}
