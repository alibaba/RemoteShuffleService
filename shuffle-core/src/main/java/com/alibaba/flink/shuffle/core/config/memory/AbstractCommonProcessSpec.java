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

import com.alibaba.flink.shuffle.common.config.MemorySize;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;

/**
 * Common memory components of shuffle manager and worker processes.
 *
 * <p>The process memory consists of the following components.
 *
 * <ul>
 *   <li>JVM Heap
 *   <li>JVM Direct(Off-Heap)
 *   <li>JVM Metaspace
 *   <li>JVM Overhead
 * </ul>
 *
 * <p>The relationships of process memory components are shown below. The memory of the kubernetes
 * container will be set to the Total Process Memory.
 *
 * <pre>
 *               ┌ ─ ─ Total Process Memory  ─ ─ ┐
 *               │┌─────────────────────────────┐│
 *                │        JVM Heap             │
 *               │└─────────────────────────────┘│
 *               │┌─────────────────────────────┐│
 *                │        JVM Direct           │
 *               │└─────────────────────────────┘│
 *               │┌─────────────────────────────┐│
 *                │        JVM Metaspace        │
 *               │└─────────────────────────────┘│
 *                ┌─────────────────────────────┐
 *               ││        JVM Overhead         ││
 *                └─────────────────────────────┘
 *               └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
 * </pre>
 */
public abstract class AbstractCommonProcessSpec implements ProcessMemorySpec {

    private static final long serialVersionUID = -8835426046927573310L;

    private final MemorySize metaspace;
    private final MemorySize overhead;

    public AbstractCommonProcessSpec(MemorySize metaspace, MemorySize overhead) {
        this.metaspace = checkNotNull(metaspace);
        this.overhead = checkNotNull(overhead);
    }

    @Override
    public MemorySize getJvmMetaspaceSize() {
        return metaspace;
    }

    @Override
    public MemorySize getJvmOverheadSize() {
        return overhead;
    }

    @Override
    public MemorySize getTotalProcessMemorySize() {
        return getJvmHeapMemorySize()
                .add(getJvmDirectMemorySize())
                .add(getJvmMetaspaceSize())
                .add(getJvmOverheadSize());
    }
}
