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

import java.util.Arrays;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

import com.aliyun.emr.rss.common.network.buffer.ManagedBuffer;
import com.aliyun.emr.rss.common.network.buffer.NettyManagedBuffer;

public final class MergedData extends AbstractMessage implements RequestMessage {
  public long requestId;

  // 0 for master, 1 for slave, see PartitionLocation.Mode
  public final byte mode;

  public final String shuffleKey;
  public final String[] partitionUniqueIds;
  public final int[] batchOffsets;
  public boolean finalPush;
  public int groupedBatchId;

  public MergedData(
      byte mode,
      String shuffleKey,
      String[] partitionIds,
      int[] batchOffsets,
      ManagedBuffer body,
      boolean finalPush,
      int groupedBatchId) {
    this(0L, mode, shuffleKey, partitionIds, batchOffsets, body,finalPush, groupedBatchId);
  }

  private MergedData(
      long requestId,
      byte mode,
      String shuffleKey,
      String[] partitionUniqueIds,
      int[] batchOffsets,
      ManagedBuffer body,
      boolean finalPush,
      int groupedBatchId) {
    super(body, true);
    this.requestId = requestId;
    this.mode = mode;
    this.shuffleKey = shuffleKey;
    this.partitionUniqueIds = partitionUniqueIds;
    this.batchOffsets = batchOffsets;
    this.finalPush = finalPush;
    this.groupedBatchId = groupedBatchId;
  }

  @Override
  public Type type() {
    return Type.PushMergedData;
  }

  @Override
  public int encodedLength() {
    return 8 + 1 + Encoders.Strings.encodedLength(shuffleKey) +
        Encoders.StringArrays.encodedLength(partitionUniqueIds) +
        Encoders.IntArrays.encodedLength(batchOffsets) + 1 + 4;
  }

  @Override
  public void encode(ByteBuf buf) {
    buf.writeLong(requestId);
    buf.writeByte(mode);
    Encoders.Strings.encode(buf, shuffleKey);
    Encoders.StringArrays.encode(buf, partitionUniqueIds);
    Encoders.IntArrays.encode(buf, batchOffsets);
    buf.writeBoolean(finalPush);
    buf.writeInt(groupedBatchId);
  }

  public static MergedData decode(ByteBuf buf) {
    long requestId = buf.readLong();
    byte mode = buf.readByte();
    String shuffleKey = Encoders.Strings.decode(buf);
    String[] partitionIds = Encoders.StringArrays.decode(buf);
    int[] batchOffsets = Encoders.IntArrays.decode(buf);
    boolean finalPush = buf.readBoolean();
    int groupedBatchId = buf.readInt();
    return new MergedData(
        requestId,
        mode,
        shuffleKey,
        partitionIds,
        batchOffsets,
        new NettyManagedBuffer(buf.retain()),
        finalPush,
        groupedBatchId);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        requestId, mode, shuffleKey, partitionUniqueIds, batchOffsets, body());
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof MergedData) {
      MergedData o = (MergedData) other;
      return requestId == o.requestId
          && mode == o.mode
          && shuffleKey.equals(o.shuffleKey)
          && Arrays.equals(partitionUniqueIds, o.partitionUniqueIds)
          && Arrays.equals(batchOffsets, o.batchOffsets)
          && super.equals(o);
    }
    return false;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("requestId", requestId)
        .add("mode", mode)
        .add("shuffleKey", shuffleKey)
        .add("partitionIds", Arrays.toString(partitionUniqueIds))
        .add("batchOffsets", Arrays.toString(batchOffsets))
        .add("body size", body().size())
        .add("final push", finalPush)
        .toString();
  }
}
