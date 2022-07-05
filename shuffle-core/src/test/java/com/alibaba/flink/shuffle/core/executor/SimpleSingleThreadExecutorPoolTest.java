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

import com.alibaba.flink.shuffle.common.exception.ShuffleException;
import com.alibaba.flink.shuffle.common.utils.CommonUtils;

import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

/** Tests for {@link SimpleSingleThreadExecutorPool}. */
public class SimpleSingleThreadExecutorPoolTest {

    @Test
    public void testFairness() throws Exception {
        int numExecutors = 10;
        int numExecutorRequests = 10;
        CountDownLatch latch = new CountDownLatch(numExecutors);
        SingleThreadExecutorPool executorPool = new SimpleSingleThreadExecutorPool(10, "test");
        ExecutorService executorService = Executors.newFixedThreadPool(numExecutors);

        try {
            ConcurrentHashMap<SingleThreadExecutor, AtomicInteger> executorCounters =
                    new ConcurrentHashMap<>();
            for (int i = 0; i < numExecutors; ++i) {
                TestTask testTask =
                        new TestTask(executorPool, executorCounters, numExecutorRequests, latch);
                executorService.submit(testTask);
            }
            latch.await();

            for (AtomicInteger counter : executorCounters.values()) {
                assertEquals(numExecutorRequests, counter.get());
            }
        } finally {
            executorService.shutdown();
            executorPool.destroy();
        }
    }

    @Test(expected = ShuffleException.class)
    public void testDestroy() {
        SingleThreadExecutorPool executorPool = new SimpleSingleThreadExecutorPool(10, "test");
        executorPool.destroy();
        executorPool.getSingleThreadExecutor();
    }

    private static class TestTask implements Runnable {

        private final SingleThreadExecutorPool executorPool;

        private final ConcurrentHashMap<SingleThreadExecutor, AtomicInteger> executorCounters;

        private final int numExecutorRequests;

        private final CountDownLatch latch;

        private TestTask(
                SingleThreadExecutorPool executorPool,
                ConcurrentHashMap<SingleThreadExecutor, AtomicInteger> executorCounters,
                int numExecutorRequests,
                CountDownLatch latch) {
            CommonUtils.checkArgument(executorPool != null, "Must be not null.");
            CommonUtils.checkArgument(executorCounters != null, "Must be not null.");
            CommonUtils.checkArgument(numExecutorRequests > 0, "Must be positive.");
            CommonUtils.checkArgument(latch != null, "Must be not null.");

            this.executorPool = executorPool;
            this.executorCounters = executorCounters;
            this.numExecutorRequests = numExecutorRequests;
            this.latch = latch;
        }

        @Override
        public void run() {
            for (int i = 0; i < numExecutorRequests; ++i) {
                SingleThreadExecutor executor = executorPool.getSingleThreadExecutor();
                AtomicInteger counter =
                        executorCounters.computeIfAbsent(
                                executor, (ignored) -> new AtomicInteger(0));
                counter.incrementAndGet();
            }
            latch.countDown();
        }
    }
}
