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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CompositeBuffer {
  private List list;
  private int size;

  public CompositeBuffer() {
    list = new ArrayList();
  }

  public void add(CompressedBuffer compressedBuffer) {
    if (compressedBuffer.getSize() == compressedBuffer.getBuffer().length) {
      list.add(compressedBuffer.getBuffer());
      size += compressedBuffer.getSize();
    } else {
      byte[] tmpBytes = Arrays.copyOfRange(compressedBuffer.getBuffer(),
        0, compressedBuffer.getSize());
      list.add(tmpBytes);
      size += compressedBuffer.getSize();
    }
  }

  public int getSize() {
    return size;
  }

  public byte[][] toArray() {
    byte[][] tmp = new byte[list.size()][];
    for (int i = 0; i < list.size(); i++) {
      tmp[i] = (byte[]) list.get(i);
    }
    list.clear();
    size = 0;
    return tmp;
  }
}
