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
import com.aliyun.emr.rss.client.write.CompressedBuffer;
import com.aliyun.emr.rss.common.RssConf;
import com.aliyun.emr.rss.common.rpc.RpcEndpointRef;

/**
 * ShuffleClient有可能是进程单例
 * 具体的PartitionLocation应该隐藏在实现里面
 */
public abstract class ShuffleClient implements Cloneable {
  private static volatile ShuffleClient _instance;
  private static volatile boolean initFinished = false;

  protected ShuffleClient() {}

  public static ShuffleClient get(RpcEndpointRef driverRef, RssConf rssConf) {
    if (null == _instance || !initFinished) {
      synchronized (ShuffleClient.class) {
        if (null == _instance) {
          // During the execution of Spark tasks, each task may be interrupted due to speculative
          // tasks. If the Task is interrupted while obtaining the ShuffleClient and the
          // ShuffleClient is building a singleton, it may cause the MetaServiceEndpoint to not be
          // assigned. An Executor will only construct a ShuffleClient singleton once. At this time,
          // when communicating with MetaService, it will cause a NullPointerException.
          _instance = new ShuffleClientImpl(rssConf);
          _instance.setupMetaServiceRef(driverRef);
          initFinished = true;
        } else if (!initFinished) {
          _instance.shutDown();
          _instance = new ShuffleClientImpl(rssConf);
          _instance.setupMetaServiceRef(driverRef);
          initFinished = true;
        }
      }
    }
    return _instance;
  }

  public static ShuffleClient get(String driverHost, int port, RssConf conf) {
    if (null == _instance || !initFinished) {
      synchronized (ShuffleClient.class) {
        if (null == _instance) {
          // During the execution of Spark tasks, each task may be interrupted due to speculative
          // tasks. If the Task is interrupted while obtaining the ShuffleClient and the
          // ShuffleClient is building a singleton, it may cause the MetaServiceEndpoint to not be
          // assigned. An Executor will only construct a ShuffleClient singleton once. At this time,
          // when communicating with MetaService, it will cause a NullPointerException.
          _instance = new ShuffleClientImpl(conf);
          _instance.setupMetaServiceRef(driverHost, port);
          initFinished = true;
        } else if (!initFinished) {
          _instance.shutDown();
          _instance = new ShuffleClientImpl(conf);
          _instance.setupMetaServiceRef(driverHost, port);
          initFinished = true;
        }
      }
    }
    return _instance;
  }

  public abstract void setupMetaServiceRef(String host, int port);

  public abstract void setupMetaServiceRef(RpcEndpointRef endpointRef);

  /**
   * 往具体的一个reduce partition里写数据
   *
   * @param applicationId
   * @param shuffleId
   * @param mapId         taskContext.partitionId
   * @param attemptId     taskContext.attemptNumber()
   * @param reduceId
   * @param offset
   * @param length
   * @param compressed
   * @param data
   */
  public abstract int pushData(
      String applicationId,
      int shuffleId,
      int mapId,
      int attemptId,
      int reduceId,
      int offset,
      int length,
      int numMappers,
      int numPartitions,
      boolean compressed,
      byte[]... data) throws IOException;

  public abstract CompressedBuffer compressData(
      int shuffleId,
      int mapId,
      int attemptId,
      byte[] data,
      int offset,
      int length
  );

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

  /**
   * report partitionlocations written by the completed map task
   * @param applicationId
   * @param shuffleId
   * @param mapId
   * @param attemptId
   */
  public abstract void mapperEnd(
      String applicationId,
      int shuffleId,
      int mapId,
      int attemptId,
      int numMappers) throws IOException;

  /**
   * cleanup states of the map task
   * @param applicationId
   * @param shuffleId
   * @param mapId
   * @param attemptId
   */
  public abstract void cleanup(String applicationId, int shuffleId, int mapId, int attemptId);

  /**
   * reduce端分区读取
   * 按照 mapperId+mapperAttemptNum+batchId 去重
   * batchId是隐藏在实现里的发送时序自增变量
   * @param applicationId
   * @param shuffleId
   * @param reduceId
   * @param startMapIndex
   * @param endMapIndex
   * @return
   */
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
