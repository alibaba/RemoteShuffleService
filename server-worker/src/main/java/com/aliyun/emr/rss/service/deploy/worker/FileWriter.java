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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import io.netty.buffer.ByteBuf;

import com.aliyun.emr.rss.common.RssConf;
import com.aliyun.emr.rss.common.protocol.PartitionSplitMode;

public final class FileWriter {
  public FileWriter(
      File file,
      File workingDir,
      long chunkSize,
      long flushBufferSize,
      RssConf rssConf,
      long splitThreshold,
      PartitionSplitMode splitMode) throws IOException {
  }

  public File getFile() {
    return null;
  }

  public ArrayList<Long> getChunkOffsets() {
    return null;
  }

  public long getFileLength() {
    return 0;
  }

  public void incrementPendingWrites() {
  }

  public void decrementPendingWrites() {
  }

  private void flush(boolean finalFlush) throws IOException {
  }

  /**
   * assume data size is less than chunk capacity
   *
   * @param data
   */
  public void write(ByteBuf data) throws IOException {
  }

  public long close() throws IOException {
    return 0;
  }

  public void destroy() {
  }

  public IOException getException() {
    return null;
  }

  public int hashCode() {
    return 0;
  }

  public boolean equals(Object obj) {
    return true;
  }

  public String toString() {
    return null;
  }

  public void flushOnMemoryPressure() throws IOException {
  }

  public void setSplitFlag() {
  }

  public long getSplitThreshold() {
    return 0;
  }

  public PartitionSplitMode getSplitMode() {
    return null;
  }
}
