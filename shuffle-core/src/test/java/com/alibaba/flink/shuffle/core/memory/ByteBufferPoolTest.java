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
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Test for {@link ByteBufferPool}. */
public class ByteBufferPoolTest {

    @Test
    public void testRequestBuffers() throws Exception {
        ByteBufferPool bufferPool = new ByteBufferPool("BUFFER POOL", 2, 16);
        assertEquals(2, bufferPool.numAvailableBuffers());

        ByteBuffer buffer = bufferPool.requestBuffer();
        assertEquals(16, buffer.capacity());
        assertEquals(1, bufferPool.numAvailableBuffers());

        buffer = bufferPool.requestBlocking(1);
        assertTrue(buffer != null);
        assertEquals(0, bufferPool.numAvailableBuffers());
        assertTrue(bufferPool.requestBuffer() == null);
        assertNull(bufferPool.requestBlocking(1));
    }

    @Test
    public void testDestroyBufferPool() {
        ByteBufferPool bufferPool = new ByteBufferPool("BUFFER POOL", 2, 16);
        assertFalse(bufferPool.isDestroyed());
        bufferPool.destroy();
        assertTrue(bufferPool.isDestroyed());
    }

    @Test
    public void testRequestAndRecycleBuffers() throws Exception {
        ByteBufferPool bufferPool = new ByteBufferPool("BUFFER POOL", 2, 16);

        ByteBuffer buffer0 = bufferPool.requestBuffer();
        assertEquals(1, bufferPool.numAvailableBuffers());
        ByteBuffer buffer1 = bufferPool.requestBuffer();
        assertEquals(0, bufferPool.numAvailableBuffers());

        bufferPool.recycle(buffer0);
        assertEquals(1, bufferPool.numAvailableBuffers());
        bufferPool.recycle(buffer1);
        assertEquals(2, bufferPool.numAvailableBuffers());

        buffer0 = bufferPool.requestBuffer();
        buffer1 = bufferPool.requestBuffer();
        assertEquals(0, bufferPool.numAvailableBuffers());

        AtomicReference<ByteBuffer> gotBuffer = new AtomicReference(null);
        Object lock = new Object();
        Thread t =
                new Thread(
                        () -> {
                            try {
                                synchronized (lock) {
                                    ByteBuffer buffer = bufferPool.requestBlocking(1);
                                    assertTrue(buffer != null);
                                    gotBuffer.set(buffer);
                                    lock.notify();
                                }
                            } catch (Exception e) {
                            }
                        });
        t.start();
        bufferPool.recycle(buffer0);
        synchronized (lock) {
            if (gotBuffer.get() == null) {
                lock.wait();
            }
        }
        t.join(500);
        assertTrue(gotBuffer.get() != null);
        bufferPool.recycle(gotBuffer.get());
        bufferPool.recycle(buffer1);
        assertEquals(2, bufferPool.numAvailableBuffers());
        bufferPool.destroy();
    }
}
