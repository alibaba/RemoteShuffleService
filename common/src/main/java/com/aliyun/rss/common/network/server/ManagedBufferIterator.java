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

import java.io.File;
import java.io.IOException;
import java.util.BitSet;
import java.util.Iterator;

import com.aliyun.rss.common.network.buffer.FileSegmentManagedBuffer;
import com.aliyun.rss.common.network.buffer.ManagedBuffer;
import com.aliyun.rss.common.network.util.TransportConf;

public class ManagedBufferIterator implements Iterator<ManagedBuffer> {
  private final File file;
  private final long[] offsets;
  private final int numChunks;

  private final BitSet chunkTracker;
  private final TransportConf conf;

  private int index = 0;

  public ManagedBufferIterator(FileInfo fileInfo, TransportConf conf) throws IOException {
    file = fileInfo.file;
    numChunks = fileInfo.numChunks;
    if (numChunks > 0) {
      offsets = new long[numChunks + 1];
      for (int i = 0; i <= numChunks; i++) {
        offsets[i] = fileInfo.chunkOffsets.get(i);
      }
    } else {
      offsets = new long[1];
      offsets[0] = 0;
    }
    chunkTracker = new BitSet(numChunks);
    chunkTracker.clear();
    this.conf = conf;
  }

  @Override
  public boolean hasNext() {
    synchronized (chunkTracker) {
      return chunkTracker.cardinality() < numChunks;
    }
  }

  public boolean hasAlreadyRead(int chunkIndex) {
    synchronized (chunkTracker) {
      return chunkTracker.get(chunkIndex);
    }
  }

  @Override
  public ManagedBuffer next() {
    // This method is only used to clear the Managed Buffer when streamManager.connectionTerminated
    // is called.
    synchronized (chunkTracker) {
      index = chunkTracker.nextClearBit(index);
    }
    assert index < numChunks;
    return chunk(index);
  }

  public ManagedBuffer chunk(int chunkIndex) {
    synchronized (chunkTracker) {
      chunkTracker.set(chunkIndex, true);
    }
    final long offset = offsets[chunkIndex];
    final long length = offsets[chunkIndex + 1] - offset;
    return new FileSegmentManagedBuffer(conf, file, offset, length);
  }
}
