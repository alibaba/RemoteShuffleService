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

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Test for {@link BufferDispatcher}. */
public class BufferDispatcherTest {

    @Test
    public void testRequestMaxBufferMoreThanPoolSize() throws Exception {
        BufferDispatcher bufferDispatcher = new BufferDispatcher("BUFFER POOL", 2, 16);
        Object lock = new Object();
        AtomicReference<List<ByteBuffer>> res = new AtomicReference<>();
        bufferDispatcher.requestBuffer(
                null,
                null,
                null,
                1,
                3,
                (buffers, exception) -> {
                    synchronized (lock) {
                        res.set(buffers);
                        lock.notify();
                    }
                });
        synchronized (lock) {
            if (res.get() == null) {
                lock.wait();
            }
        }
        assertTrue(res.get() != null);
        assertEquals(2, res.get().size());
        res.get().forEach((buffer) -> bufferDispatcher.recycleBuffer(buffer, null, null, null));
        bufferDispatcher.destroy();
    }

    @Test
    public void testRequestInsufficientBuffer() throws Exception {
        BufferDispatcher bufferDispatcher = new BufferDispatcher("BUFFER POOL", 2, 16);
        Object lock = new Object();
        AtomicReference<List<ByteBuffer>> res0 = new AtomicReference<>();
        bufferDispatcher.requestBuffer(
                null,
                null,
                null,
                1,
                1,
                (buffers, exception) -> {
                    synchronized (lock) {
                        res0.set(buffers);
                        lock.notify();
                    }
                });
        synchronized (lock) {
            if (res0.get() == null) {
                lock.wait();
            }
        }
        assertTrue(res0.get() != null);
        assertEquals(1, res0.get().size());

        AtomicReference<List<ByteBuffer>> res1 = new AtomicReference<>();
        bufferDispatcher.requestBuffer(
                null,
                null,
                null,
                2,
                2,
                (buffers, exception) -> {
                    synchronized (lock) {
                        res1.set(buffers);
                        lock.notify();
                    }
                });
        synchronized (lock) {
            if (res1.get() == null) {
                lock.wait(1000);
            }
        }
        assertTrue(res1.get() == null);

        res0.get().forEach((buffer) -> bufferDispatcher.recycleBuffer(buffer, null, null, null));
        synchronized (lock) {
            if (res1.get() == null) {
                lock.wait();
            }
        }
        assertTrue(res1.get() != null);
        assertEquals(2, res1.get().size());

        res1.get().forEach((buffer) -> bufferDispatcher.recycleBuffer(buffer, null, null, null));
        bufferDispatcher.destroy();
    }
}
