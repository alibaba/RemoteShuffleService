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

package com.alibaba.flink.shuffle.common.functions;

import com.alibaba.flink.shuffle.common.exception.ShuffleException;
import com.alibaba.flink.shuffle.common.utils.ExceptionUtils;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/** Closeable interface which allows to close a resource in a non blocking fashion. */
public interface AutoCloseableAsync extends AutoCloseable {

    /**
     * Trigger the closing of the resource and return the corresponding close future.
     *
     * @return Future which is completed once the resource has been closed
     */
    CompletableFuture<Void> closeAsync();

    @Override
    default void close() throws Exception {
        try {
            closeAsync().get();
        } catch (ExecutionException exception) {
            throw new ShuffleException(
                    "Could not close resource.",
                    ExceptionUtils.stripException(exception, ExecutionException.class));
        }
    }
}
