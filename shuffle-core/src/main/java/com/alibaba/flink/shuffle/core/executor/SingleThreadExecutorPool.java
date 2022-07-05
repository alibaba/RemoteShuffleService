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

/** A pool from where to allocate {@link SingleThreadExecutor}s. */
public interface SingleThreadExecutorPool {

    /** Gets a {@link SingleThreadExecutor} from this single-thread executor pool. */
    SingleThreadExecutor getSingleThreadExecutor();

    /** Returns the numbers of {@link SingleThreadExecutor} in this executor pool. */
    int getNumExecutors();

    /** Destroys this executor pool and releases all resources. */
    void destroy();
}
