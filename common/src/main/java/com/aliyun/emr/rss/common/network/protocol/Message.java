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

public interface Message extends Encodable {
  Type type();

  ManagedBuffer body();

  boolean isBodyInFrame();

  enum Type implements Encodable {
    ChunkFetchRequest(0), ChunkFetchSuccess(1), ChunkFetchFailure(2),
    RpcRequest(3), RpcResponse(4), RpcFailure(5),
    OneWayMessage(9), PushData(11), PushMergedData(12);

    private final byte id;

    Type(int id) {
      assert id < 128 : "Cannot have more than 128 message types";
      this.id = (byte) id;
    }

    public byte id() { return id; }

    @Override public int encodedLength() { return 1; }

    @Override public void encode(ByteBuf buf) { buf.writeByte(id); }

    public static Type decode(ByteBuf buf) {
      byte id = buf.readByte();
      switch (id) {
        case 0: return ChunkFetchRequest;
        case 1: return ChunkFetchSuccess;
        case 2: return ChunkFetchFailure;
        case 3: return RpcRequest;
        case 4: return RpcResponse;
        case 5: return RpcFailure;
        case 9: return OneWayMessage;
        case 11: return PushData;
        case 12: return PushMergedData;
        case -1: throw new IllegalArgumentException("User type messages cannot be decoded.");
        default: throw new IllegalArgumentException("Unknown message type: " + id);
      }
    }
  }
}
