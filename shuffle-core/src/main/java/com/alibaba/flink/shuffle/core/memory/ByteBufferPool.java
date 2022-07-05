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

package com.alibaba.flink.shuffle.core.memory;

import com.alibaba.flink.shuffle.common.utils.CommonUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/** Buffer pool to provide {@link ByteBuffer} on shuffle workers. */
public class ByteBufferPool implements BufferRecycler {

    private static final Logger LOG = LoggerFactory.getLogger(ByteBufferPool.class);

    private final String name;

    private final int numBuffers;

    private final int bufferSize;

    private final BlockingQueue<ByteBuffer> availableBuffers;

    private boolean isDestroyed;

    /**
     * @param name Name of this buffer pool.
     * @param numBuffers Total number of available buffers when start.
     * @param bufferSize Size of a single buffer.
     */
    public ByteBufferPool(String name, int numBuffers, int bufferSize) {
        LOG.info("Creating buffer pool, numBuffers={}, bufferSize={}.", numBuffers, bufferSize);
        this.name = name;
        this.numBuffers = numBuffers;
        this.bufferSize = bufferSize;
        this.availableBuffers = new LinkedBlockingDeque<>();
        for (int i = 0; i < numBuffers; i++) {
            ByteBuffer byteBuffer = CommonUtils.allocateDirectByteBuffer(bufferSize);
            availableBuffers.add(byteBuffer);
        }
        this.isDestroyed = false;
    }

    /** Name of buffer pool. */
    public String getName() {
        return name;
    }

    /** Request a buffer by non-blocking mode. */
    public ByteBuffer requestBuffer() {
        return availableBuffers.poll();
    }

    /** Request a buffer by blocking mode -- returns null when timeout. */
    public ByteBuffer requestBlocking(int timeoutInSeconds) throws InterruptedException {
        return availableBuffers.poll(timeoutInSeconds, TimeUnit.SECONDS);
    }

    /** Destroy this buffer pool. */
    public void destroy() {
        LOG.info("Destroying buffer pool, numBuffers={}, bufferSize={}.", numBuffers, bufferSize);
        availableBuffers.clear();
        isDestroyed = true;
    }

    /** Whether this buffer pool is already destroyed. */
    public boolean isDestroyed() {
        return isDestroyed;
    }

    /** Get the number of available buffers at the moment. */
    public int numAvailableBuffers() {
        return availableBuffers.size();
    }

    /** Return a {@link ByteBuffer} back to this buffer pool. */
    @Override
    public void recycle(ByteBuffer buffer) {
        buffer.clear();
        availableBuffers.add(buffer);
        if (availableBuffers.size() > numBuffers) {
            LOG.error(
                    "BUG: {} got more buffers than expectation {}.", name, availableBuffers.size());
        }
    }
}
