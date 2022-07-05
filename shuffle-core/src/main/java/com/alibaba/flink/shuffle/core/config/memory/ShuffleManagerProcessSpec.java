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
import com.alibaba.flink.shuffle.core.config.ManagerOptions;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;

/** Specification of ShuffleManager memory. */
public class ShuffleManagerProcessSpec extends AbstractCommonProcessSpec {

    private static final long serialVersionUID = -1434517095656544445L;

    private final MemorySize frameworkHeap;
    private final MemorySize frameworkOffHeap;

    public ShuffleManagerProcessSpec(Configuration memConfig) {
        super(
                memConfig.getMemorySize(ManagerOptions.JVM_METASPACE),
                memConfig.getMemorySize(ManagerOptions.JVM_OVERHEAD));
        this.frameworkHeap =
                checkNotNull(memConfig.getMemorySize(ManagerOptions.FRAMEWORK_HEAP_MEMORY));
        this.frameworkOffHeap =
                checkNotNull(memConfig.getMemorySize(ManagerOptions.FRAMEWORK_OFF_HEAP_MEMORY));
    }

    @Override
    public MemorySize getJvmHeapMemorySize() {
        return frameworkHeap;
    }

    @Override
    public MemorySize getJvmDirectMemorySize() {
        return frameworkOffHeap;
    }
}
