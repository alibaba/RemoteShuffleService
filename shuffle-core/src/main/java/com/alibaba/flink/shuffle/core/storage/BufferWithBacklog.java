/*
 * Copyright 2021 The Flink Remote Shuffle Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.flink.shuffle.core.storage;

import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.core.memory.Buffer;

/** Data buffer and the number of remaining data buffers returned to the data consumer. */
public class BufferWithBacklog {

    /** Data buffer read from the target {@link DataPartition}. */
    private final Buffer buffer;

    /** Number of remaining data buffers already read in the {@link DataPartitionReader}. */
    private final long backlog;

    public BufferWithBacklog(Buffer buffer, long backlog) {
        CommonUtils.checkArgument(buffer != null, "Must be not null.");
        CommonUtils.checkArgument(backlog >= 0, "Must be non-negative.");

        this.buffer = buffer;
        this.backlog = backlog;
    }

    public Buffer getBuffer() {
        return buffer;
    }

    public long getBacklog() {
        return backlog;
    }
}
