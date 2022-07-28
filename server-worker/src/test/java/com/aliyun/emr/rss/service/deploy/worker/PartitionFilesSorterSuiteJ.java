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
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import com.aliyun.emr.rss.common.RssConf;
import com.aliyun.emr.rss.common.network.server.FileInfo;
import com.aliyun.emr.rss.common.network.server.MemoryTracker;
import com.aliyun.emr.rss.common.unsafe.Platform;

import static org.mockito.Mockito.when;

public class PartitionFilesSorterSuiteJ {
  private File shuffleFile;
  public final int CHUNK_SIZE = 8 * 1024 * 1024;
  private String originFileName;
  private long originFileLen;
  private FileWriter fileWriter;
  private long sortTimeout = 16 * 1000;

  public void prepare(boolean largefile) throws IOException {
    byte[] batchHeader = new byte[16];
    Random random = new Random();
    shuffleFile = File.createTempFile("RSS", "sort-suite");

    originFileName = shuffleFile.getAbsolutePath();
    FileOutputStream fileOutputStream = new FileOutputStream(shuffleFile);
    FileChannel channel = fileOutputStream.getChannel();
    Map<Integer, Integer> batchIds = new HashMap<>();

    int maxMapId = 50;
    int mapCount = 1000;
    if (largefile) {
      mapCount = 15000;
    }
    for (int i = 0; i < mapCount; i++) {
      int mapId = random.nextInt(maxMapId);
      int currentAttemptId = 0;
      int batchId = batchIds.compute(mapId, (k, v) -> {
        if (v == null) {
          v = 0;
        } else {
          v++;
        }
        return v;
      });
      int dataSize = random.nextInt(192 * 1024) + 65525;
      byte[] mockedData = new byte[dataSize];
      Platform.putInt(batchHeader, Platform.BYTE_ARRAY_OFFSET, mapId);
      Platform.putInt(batchHeader, Platform.BYTE_ARRAY_OFFSET + 4, currentAttemptId);
      Platform.putInt(batchHeader, Platform.BYTE_ARRAY_OFFSET + 8, batchId);
      Platform.putInt(batchHeader, Platform.BYTE_ARRAY_OFFSET + 12, dataSize);
      channel.write(ByteBuffer.wrap(batchHeader));
      random.nextBytes(mockedData);
      channel.write(ByteBuffer.wrap(mockedData));
    }
    originFileLen = channel.size();
    System.out.println(shuffleFile.getAbsolutePath() +
                         " filelen " + (double) originFileLen / 1024 / 1024.0 + "MB");

    MemoryTracker.initialize(0.8, 0.9, 0.5, 0.6, 10, 10, 10);
    fileWriter = Mockito.mock(FileWriter.class);
    when(fileWriter.getFile()).thenAnswer(i -> shuffleFile);
    when(fileWriter.getFileLength()).thenAnswer(i -> originFileLen);
    when(fileWriter.getChunkOffsets()).thenAnswer(i -> new ArrayList<Integer>());
  }

  public void clean() {
    shuffleFile.delete();
  }

  @Test
  public void testSmallFile() throws InterruptedException, IOException {
    prepare(false);
    RssConf conf = new RssConf();
    PartitionFilesSorter partitionFilesSorter = new PartitionFilesSorter(MemoryTracker.instance(),
      sortTimeout, CHUNK_SIZE, 1024 * 1024, new WorkerSource(conf));
    FileInfo info = partitionFilesSorter.openStream("application-1", originFileName,
      fileWriter, 5, 10);
    Thread.sleep(1000);
    System.out.println(info.toString());
    Assert.assertTrue(info.numChunks > 0);
    clean();
  }

  @Test
  @Ignore
  public void testLargeFile() throws InterruptedException, IOException {
    prepare(true);
    RssConf conf = new RssConf();
    PartitionFilesSorter partitionFilesSorter = new PartitionFilesSorter(MemoryTracker.instance(),
      sortTimeout, CHUNK_SIZE, 1024 * 1024, new WorkerSource(conf));
    FileInfo info = partitionFilesSorter.openStream("application-1", originFileName,
      fileWriter, 5, 10);
    Thread.sleep(30000);
    System.out.println(info.toString());
    Assert.assertTrue(info.numChunks > 0);
    clean();
  }
}
