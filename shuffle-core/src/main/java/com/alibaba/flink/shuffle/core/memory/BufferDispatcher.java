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

import com.alibaba.flink.shuffle.common.exception.ShuffleException;
import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.core.ids.DataPartitionID;
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.listener.BufferListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkArgument;

/** Wrapper to allocate buffers from {@link ByteBufferPool}. */
public class BufferDispatcher {

    private static final Logger LOG = LoggerFactory.getLogger(BufferDispatcher.class);

    private final ByteBufferPool bufferPool;

    @GuardedBy("lock")
    private final Queue<BufferRequirement> bufferRequirements;

    private final int totalBuffers;

    @GuardedBy("lock")
    private boolean destroyed;

    private final Thread dispatcherThread;

    private final Object lock;

    private volatile long lastBufferWaitingTimeMs;

    /**
     * @param name Name of the underlying buffer pool.
     * @param numBuffers Total number of available buffers when start.
     * @param bufferSize Size of a single buffer.
     */
    public BufferDispatcher(String name, int numBuffers, int bufferSize) {
        this.bufferPool = new ByteBufferPool(name, numBuffers, bufferSize);
        this.totalBuffers = numBuffers;
        this.bufferRequirements = new ArrayDeque<>();
        this.destroyed = false;
        this.lock = new Object();
        this.dispatcherThread = new Thread(new Dispatcher());
        dispatcherThread.setName("Buffer Dispatcher");
        dispatcherThread.setDaemon(true);
        dispatcherThread.start();
    }

    /**
     * Request buffer(s) asynchronously. Note that buffers will be allocated by priorities and
     * notified by {@link BufferListener}.
     */
    public void requestBuffer(
            JobID jobID,
            DataSetID dataSetID,
            DataPartitionID dataPartitionID,
            int min,
            int max,
            BufferListener listener) {
        synchronized (lock) {
            bufferRequirements.add(new BufferRequirement(min, max, listener));
            lock.notifyAll();
        }
    }

    /** Destroy this {@link BufferDispatcher} and underlying {@link ByteBufferPool}. */
    public void destroy() {
        synchronized (lock) {
            bufferPool.destroy();
            destroyed = true;
            dispatcherThread.interrupt();
            lock.notifyAll();
        }
    }

    private class Dispatcher implements Runnable {

        private static final int REQUEST_TIMEOUT = 60 * 60; // in seconds.

        @Override
        public void run() {
            while (true) {
                List<ByteBuffer> buffers = new ArrayList<>();
                BufferRequirement bufferRequirement = null;
                try {
                    synchronized (lock) {
                        if (destroyed) {
                            break;
                        }
                        while (bufferRequirements.isEmpty() && !destroyed) {
                            lock.wait();
                        }
                        bufferRequirement = bufferRequirements.poll();
                        if (bufferRequirement == null) {
                            CommonUtils.checkState(
                                    destroyed,
                                    "Polled a bufferRequirement as null, but BufferDispatcher not destroyed yet.");
                            break;
                        }
                    }
                    long startTime = System.nanoTime();
                    while (buffers.size() < bufferRequirement.min) {
                        ByteBuffer buffer = bufferPool.requestBlocking(REQUEST_TIMEOUT);
                        if (buffer == null) {
                            throw new ShuffleException(
                                    "Memory shortage in " + bufferPool.getName());
                        }
                        buffers.add(buffer);
                    }
                    ByteBuffer buffer = null;
                    while (buffers.size() < bufferRequirement.max
                            && (buffer = bufferPool.requestBuffer()) != null) {
                        buffers.add(buffer);
                    }
                    bufferRequirement.bufferListener.notifyBuffers(buffers, null);
                    lastBufferWaitingTimeMs = (System.nanoTime() - startTime) / 1_000_000;
                } catch (Exception e) {
                    LOG.error("Exception when fulfilling buffer requirement.", e);
                    buffers.forEach(bufferPool::recycle);
                    if (bufferRequirement != null) {
                        bufferRequirement.bufferListener.notifyBuffers(null, e);
                    }
                }
            }
        }
    }

    public long getLastBufferWaitingTime() {
        return lastBufferWaitingTimeMs;
    }

    /** Get total number of buffers. */
    public int numTotalBuffers() {
        return totalBuffers;
    }

    /** Get number of available buffers. */
    public int numAvailableBuffers() {
        return bufferPool.numAvailableBuffers();
    }

    /** Recycle a buffer back for further allocation. */
    public void recycleBuffer(
            ByteBuffer buffer, JobID jobID, DataSetID dataSetID, DataPartitionID dataPartitionID) {
        bufferPool.recycle(buffer);
    }

    private static final class BufferRequirement {

        private final int min;
        private final int max;
        private final BufferListener bufferListener;

        BufferRequirement(int min, int max, BufferListener bufferListener) {
            checkArgument(
                    min > 0 && max > 0 && max >= min,
                    String.format("Invalid min=%d, max=%d.", min, max));
            this.min = min;
            this.max = max;
            this.bufferListener = bufferListener;
        }
    }
}
