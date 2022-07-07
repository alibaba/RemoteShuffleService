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

package com.aliyun.emr.rss.client;

import java.io.IOException;

import com.aliyun.emr.rss.client.read.RssInputStream;
import com.aliyun.emr.rss.common.RssConf;
import com.aliyun.emr.rss.common.rpc.RpcEndpointRef;

public abstract class ShuffleClient implements Cloneable {
  public static ShuffleClient get(RpcEndpointRef driverRef, RssConf rssConf) {
    return null;
  }

  public static ShuffleClient get(String driverHost, int port, RssConf conf) {
    return null;
  }

  public abstract void setupMetaServiceRef(RpcEndpointRef endpointRef);

  public abstract int pushData(
      String applicationId,
      int shuffleId,
      int mapId,
      int attemptId,
      int reduceId,
      byte[] data,
      int offset,
      int length,
      int numMappers,
      int numPartitions) throws IOException;

  public abstract void prepareForMergeData(
      int shuffleId,
      int mapId,
      int attemptId) throws IOException;

  public abstract int mergeData(
      String applicationId,
      int shuffleId,
      int mapId,
      int attemptId,
      int reduceId,
      byte[] data,
      int offset,
      int length,
      int numMappers,
      int numPartitions) throws IOException;

  public abstract void pushMergedData(
      String applicationId,
      int shuffleId,
      int mapId,
      int attemptId
  ) throws IOException;

  public abstract void mapperEnd(
      String applicationId,
      int shuffleId,
      int mapId,
      int attemptId,
      int numMappers) throws IOException;

  public abstract void cleanup(String applicationId, int shuffleId, int mapId, int attemptId);

  public abstract RssInputStream readPartition(
      String applicationId,
      int shuffleId,
      int reduceId,
      int attemptNumber,
      int startMapIndex,
      int endMapIndex) throws IOException;

  public abstract RssInputStream readPartition(
      String applicationId,
      int shuffleId,
      int reduceId,
      int attemptNumber) throws IOException;

  /**
   * 注销
   * @param applicationId
   * @param shuffleId
   * @return
   */
  public abstract boolean unregisterShuffle(
      String applicationId, int shuffleId, boolean isDriver);

  public abstract void shutDown();
}
