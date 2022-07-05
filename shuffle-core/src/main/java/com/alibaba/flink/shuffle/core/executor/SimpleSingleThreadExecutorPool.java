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

import java.util.ArrayDeque;
import java.util.Queue;

/**
 * A simple {@link SingleThreadExecutorPool} implementation which assigns all existing {@link
 * SingleThreadExecutor}s in a round-robin way. More complicated policies like work stealing can be
 * implemented in the future.
 */
public class SimpleSingleThreadExecutorPool implements SingleThreadExecutorPool {

    /** All available {@link SingleThreadExecutor}s. */
    private final Queue<SingleThreadExecutor> singleThreadExecutors = new ArrayDeque<>();

    /** Total number of {@link SingleThreadExecutor}s. */
    private final int numExecutors;

    /** Whether this {@link SingleThreadExecutorPool} has been destroyed or not. */
    private boolean isDestroyed;

    public SimpleSingleThreadExecutorPool(int numExecutors, String threadName) {
        CommonUtils.checkArgument(numExecutors > 0, "Must be positive.");
        CommonUtils.checkArgument(threadName != null, "Must be not null.");

        this.numExecutors = numExecutors;
        for (int i = 0; i < numExecutors; ++i) {
            this.singleThreadExecutors.add(new SingleThreadExecutor(threadName + "-" + i));
        }
    }

    @Override
    public SingleThreadExecutor getSingleThreadExecutor() {
        synchronized (singleThreadExecutors) {
            if (isDestroyed) {
                throw new ShuffleException("The executor pool has been destroyed.");
            }

            SingleThreadExecutor executor = CommonUtils.checkNotNull(singleThreadExecutors.poll());
            singleThreadExecutors.add(executor);
            return executor;
        }
    }

    @Override
    public int getNumExecutors() {
        return numExecutors;
    }

    @Override
    public void destroy() {
        synchronized (singleThreadExecutors) {
            isDestroyed = true;

            for (SingleThreadExecutor singleThreadExecutor : singleThreadExecutors) {
                CommonUtils.runQuietly(singleThreadExecutor::shutDown);
            }
            singleThreadExecutors.clear();
        }
    }
}
