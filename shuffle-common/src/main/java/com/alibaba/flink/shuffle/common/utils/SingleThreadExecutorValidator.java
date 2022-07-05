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

package com.alibaba.flink.shuffle.common.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;

/**
 * A tool class to ensure the current method is running in the thread of a specified single thread
 * executor.
 */
public class SingleThreadExecutorValidator {

    private static final Logger LOG = LoggerFactory.getLogger(SingleThreadExecutorValidator.class);

    private final Thread targetThread;

    public SingleThreadExecutorValidator(Executor executor) {
        CompletableFuture<Thread> targetThreadFuture = new CompletableFuture<>();
        executor.execute(() -> targetThreadFuture.complete(Thread.currentThread()));
        targetThread = checkNotNull(targetThreadFuture.join());
    }

    public SingleThreadExecutorValidator(Thread targetThread) {
        this.targetThread = targetThread;
    }

    public void assertRunningInTargetThread() {
        if (Thread.currentThread() != targetThread) {
            RuntimeException exception =
                    new RuntimeException(
                            "Expected running in "
                                    + targetThread
                                    + ", but running in "
                                    + Thread.currentThread());
            LOG.warn("Validate Failed", exception);
            throw exception;
        }
    }
}
