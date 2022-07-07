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

package com.aliyun.emr.rss.common.network.protocol;

import io.netty.buffer.ByteBuf;

import com.aliyun.emr.rss.common.network.buffer.ManagedBuffer;

public final class PushMergedData extends AbstractMessage implements RequestMessage {
  public long requestId;

  // 0 for master, 1 for slave, see PartitionLocation.Mode
  public final byte mode;

  public final String shuffleKey;
  public final String[] partitionUniqueIds;
  public final int[] batchOffsets;

  public PushMergedData(
      byte mode,
      String shuffleKey,
      String[] partitionIds,
      int[] batchOffsets,
      ManagedBuffer body) {
    this(0L, mode, shuffleKey, partitionIds, batchOffsets, body);
  }

  private PushMergedData(
      long requestId,
      byte mode,
      String shuffleKey,
      String[] partitionUniqueIds,
      int[] batchOffsets,
      ManagedBuffer body) {
    this.requestId = requestId;
    this.mode = mode;
    this.shuffleKey = shuffleKey;
    this.partitionUniqueIds = partitionUniqueIds;
    this.batchOffsets = batchOffsets;
  }

  @Override
  public Type type() {
    return Type.PushMergedData;
  }

  @Override
  public int encodedLength() {
    return 0;
  }

  @Override
  public void encode(ByteBuf buf) {
  }

  public static PushMergedData decode(ByteBuf buf) {
    return null;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public boolean equals(Object other) {
    return false;
  }

  @Override
  public String toString() {
    return null;
  }
}
