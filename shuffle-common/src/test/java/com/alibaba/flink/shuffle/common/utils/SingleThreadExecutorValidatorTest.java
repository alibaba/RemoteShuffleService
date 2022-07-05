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

import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.fail;

/** Tests the {@link SingleThreadExecutorValidator}. */
public class SingleThreadExecutorValidatorTest {

    @Test
    public void testRunningInSameThread() throws ExecutionException, InterruptedException {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            SingleThreadExecutorValidator validator = new SingleThreadExecutorValidator(executor);

            // The following call should succeed.
            executor.submit(validator::assertRunningInTargetThread).get();

        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testRunningInDifferentThread() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            SingleThreadExecutorValidator validator = new SingleThreadExecutorValidator(executor);

            try {
                validator.assertRunningInTargetThread();
                fail("The check should failed");
            } catch (RuntimeException e) {
                // Expected exception
            }
        } finally {
            executor.shutdownNow();
        }
    }
}
