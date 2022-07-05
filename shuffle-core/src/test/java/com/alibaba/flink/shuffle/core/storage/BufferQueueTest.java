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
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.core.memory.BufferDispatcher;

import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/** Tests for {@link BufferQueue}. */
public class BufferQueueTest {

    private BufferDispatcher dispatcher;

    @Before
    public void before() {
        dispatcher = new BufferDispatcher("TestDispatcher", 0, 1024);
    }

    @Test
    public void testPollBuffer() {
        ByteBuffer buffer1 = ByteBuffer.allocate(1024);
        ByteBuffer buffer2 = ByteBuffer.allocate(1024);
        BufferQueue bufferQueue = createBufferQueue();
        bufferQueue.add(Arrays.asList(buffer1, buffer2));

        assertEquals(2, bufferQueue.size());
        assertEquals(buffer1, bufferQueue.poll());

        assertEquals(1, bufferQueue.size());
        assertEquals(buffer2, bufferQueue.poll());

        assertEquals(0, bufferQueue.size());
        assertNull(bufferQueue.poll());
    }

    @Test
    public void testAddBuffer() {
        ByteBuffer buffer1 = ByteBuffer.allocate(1024);
        ByteBuffer buffer2 = ByteBuffer.allocate(1024);
        BufferQueue bufferQueue = createBufferQueue();

        assertEquals(0, bufferQueue.size());
        bufferQueue.add(buffer1);

        assertEquals(1, bufferQueue.size());
        bufferQueue.add(buffer2);

        assertEquals(2, bufferQueue.size());
        bufferQueue.add(Arrays.asList(ByteBuffer.allocate(1024), ByteBuffer.allocate(1024)));
        assertEquals(4, bufferQueue.size());
    }

    @Test
    public void testRecycleBuffer() {
        BufferQueue bufferQueue = createBufferQueue();
        assertEquals(0, dispatcher.numAvailableBuffers());

        bufferQueue.recycle(ByteBuffer.allocate(1024));
        assertEquals(1, dispatcher.numAvailableBuffers());

        bufferQueue.add(Arrays.asList(ByteBuffer.allocate(1024), ByteBuffer.allocate(1024)));
        bufferQueue.recycleAll();
        assertEquals(3, dispatcher.numAvailableBuffers());
    }

    @Test
    public void testReleaseBufferQueue() {
        BufferQueue bufferQueue = createBufferQueue();
        bufferQueue.add(Arrays.asList(ByteBuffer.allocate(1024), ByteBuffer.allocate(1024)));

        assertEquals(2, bufferQueue.size());
        bufferQueue.release();

        assertEquals(0, bufferQueue.size());
        assertEquals(2, dispatcher.numAvailableBuffers());

        try {
            bufferQueue.add(ByteBuffer.allocate(1024));
        } catch (IllegalStateException exception) {
            assertNull(bufferQueue.poll());
            return;
        }

        fail("IllegalStateException expected.");
    }

    private BufferQueue createBufferQueue() {
        DataPartition partition =
                new NoOpDataPartition(
                        new JobID(CommonUtils.randomBytes(16)),
                        new DataSetID(CommonUtils.randomBytes(16)),
                        new MapPartitionID(CommonUtils.randomBytes(16)));
        return new BufferQueue(partition, dispatcher);
    }
}
