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

package com.alibaba.flink.shuffle.core.executor;

import com.alibaba.flink.shuffle.common.utils.CommonUtils;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;

import static org.junit.Assert.assertEquals;

/** Tests for {@link SingleThreadExecutor}. */
public class SingleThreadExecutorTest {

    @Test(timeout = 60000, expected = RejectedExecutionException.class)
    public void testShutDown() throws Exception {
        SingleThreadExecutor executor = new SingleThreadExecutor("test-thread");
        executor.shutDown();
        executor.getExecutorThread().join();
        executor.execute(() -> {});
    }

    @Test
    public void testSingleThreadExecution() throws Exception {
        SingleThreadExecutor executor = new SingleThreadExecutor("test-thread");

        try {
            int count = 1000;
            CountDownLatch latch = new CountDownLatch(count);
            TestTask testTask = new TestTask(latch);

            for (int i = 0; i < count; ++i) {
                executor.execute(testTask);
            }

            latch.await();
            assertEquals(count, testTask.counter);
        } finally {
            executor.shutDown();
        }
    }

    private static class TestTask implements Runnable {

        private final CountDownLatch latch;

        private int counter;

        private TestTask(CountDownLatch latch) {
            CommonUtils.checkArgument(latch != null, "Must be not null.");
            this.latch = latch;
        }

        @Override
        public void run() {
            ++counter;
            latch.countDown();
        }
    }
}
