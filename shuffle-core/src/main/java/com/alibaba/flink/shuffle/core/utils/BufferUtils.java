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

package com.alibaba.flink.shuffle.core.utils;

import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.core.ids.DataPartitionID;
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.memory.Buffer;
import com.alibaba.flink.shuffle.core.memory.BufferDispatcher;
import com.alibaba.flink.shuffle.core.memory.BufferRecycler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.Collection;

/** Utility methods to manipulate buffers. */
public class BufferUtils {

    private static final Logger LOG = LoggerFactory.getLogger(BufferUtils.class);

    /** Recycles the given {@link Buffer} and logs the error if any exception occurs. */
    public static void recycleBuffer(@Nullable Buffer buffer) {
        if (buffer == null) {
            return;
        }

        try {
            buffer.release();
        } catch (Throwable throwable) {
            LOG.error("Fatal: failed to release the target buffer.", throwable);
        }
    }

    /** Recycles the given {@link Buffer}s and logs the error if any exception occurs. */
    public static void recycleBuffers(@Nullable Collection<Buffer> buffers) {
        if (buffers == null) {
            return;
        }

        for (Buffer buffer : buffers) {
            recycleBuffer(buffer);
        }
        // clear method is not supported by all collections
        CommonUtils.runQuietly(buffers::clear);
    }

    /**
     * Recycles the given {@link ByteBuffer} to the target {@link BufferDispatcher} logs the error
     * if any exception occurs.
     */
    public static void recycleBuffer(
            @Nullable ByteBuffer buffer,
            BufferDispatcher bufferDispatcher,
            JobID jobID,
            DataSetID dataSetID,
            DataPartitionID partitionID) {
        if (buffer == null) {
            return;
        }

        try {
            CommonUtils.checkArgument(bufferDispatcher != null, "Must be not mull.");

            bufferDispatcher.recycleBuffer(buffer, jobID, dataSetID, partitionID);
        } catch (Throwable throwable) {
            LOG.error("Fatal: failed to release the target buffer.", throwable);
        }
    }

    /**
     * Recycles the given {@link ByteBuffer}s to the target {@link BufferDispatcher} logs the error
     * if any exception occurs.
     */
    public static void recycleBuffers(
            @Nullable Collection<ByteBuffer> buffers,
            BufferDispatcher bufferDispatcher,
            JobID jobID,
            DataSetID dataSetID,
            DataPartitionID partitionID) {
        if (buffers == null) {
            return;
        }

        for (ByteBuffer buffer : buffers) {
            recycleBuffer(buffer, bufferDispatcher, jobID, dataSetID, partitionID);
        }
        // clear method is not supported by all collections
        CommonUtils.runQuietly(buffers::clear);
    }

    /**
     * Recycles the given {@link Buffer} with the target {@link BufferRecycler} logs the error if
     * any exception occurs.
     */
    public static void recycleBuffer(@Nullable ByteBuffer buffer, BufferRecycler recycler) {
        if (buffer == null) {
            return;
        }

        try {
            CommonUtils.checkArgument(recycler != null, "Must be not mull.");

            recycler.recycle(buffer);
        } catch (Throwable throwable) {
            LOG.error("Fatal: failed to release the target buffer.", throwable);
        }
    }
}
