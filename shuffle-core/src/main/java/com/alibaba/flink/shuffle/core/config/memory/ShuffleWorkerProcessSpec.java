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

package com.alibaba.flink.shuffle.core.config.memory;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.common.config.MemorySize;
import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.core.config.MemoryOptions;
import com.alibaba.flink.shuffle.core.config.WorkerOptions;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;

/** Specification of ShuffleWorker memory. */
public class ShuffleWorkerProcessSpec extends AbstractCommonProcessSpec {

    private static final long serialVersionUID = -7719025949639324784L;

    private final MemorySize frameworkHeap;
    private final MemorySize frameworkOffHeap;
    private final MemorySize networkOffHeap;

    public ShuffleWorkerProcessSpec(Configuration memConfig) {
        super(
                memConfig.getMemorySize(WorkerOptions.JVM_METASPACE),
                memConfig.getMemorySize(WorkerOptions.JVM_OVERHEAD));
        this.frameworkHeap =
                checkNotNull(memConfig.getMemorySize(WorkerOptions.FRAMEWORK_HEAP_MEMORY));
        this.frameworkOffHeap =
                checkNotNull(memConfig.getMemorySize(WorkerOptions.FRAMEWORK_OFF_HEAP_MEMORY));

        MemorySize readingMemory =
                CommonUtils.checkNotNull(
                        memConfig.getMemorySize(MemoryOptions.MEMORY_SIZE_FOR_DATA_READING));
        MemorySize writingMemory =
                CommonUtils.checkNotNull(
                        memConfig.getMemorySize(MemoryOptions.MEMORY_SIZE_FOR_DATA_WRITING));

        this.networkOffHeap = readingMemory.add(writingMemory);
    }

    @Override
    public MemorySize getJvmHeapMemorySize() {
        return frameworkHeap;
    }

    @Override
    public MemorySize getJvmDirectMemorySize() {
        return frameworkOffHeap.add(networkOffHeap);
    }

    public MemorySize getFrameworkOffHeap() {
        return frameworkOffHeap;
    }

    public MemorySize getNetworkOffHeap() {
        return networkOffHeap;
    }
}
