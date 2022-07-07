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

public final class RpcRequest extends AbstractMessage implements RequestMessage {
  public RpcRequest(long requestId, ManagedBuffer message) {
  }

  @Override
  public Type type() { return Type.RpcRequest; }

  @Override
  public int encodedLength() {
    return 8 + 4;
  }

  @Override
  public void encode(ByteBuf buf) {
  }

  public static RpcRequest decode(ByteBuf buf) {
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
