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

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/** Test for {@link ShuffleManagerProcessSpec}. */
public class ShuffleManagerProcessSpecTest {

    @Test
    public void testShuffleManagerProcessSpec() {
        Configuration memConfig = new Configuration();

        memConfig.setMemorySize(ManagerOptions.FRAMEWORK_HEAP_MEMORY, MemorySize.parse("256m"));
        memConfig.setMemorySize(ManagerOptions.FRAMEWORK_OFF_HEAP_MEMORY, MemorySize.parse("128m"));
        memConfig.setMemorySize(ManagerOptions.JVM_METASPACE, MemorySize.parse("32m"));
        memConfig.setMemorySize(ManagerOptions.JVM_OVERHEAD, MemorySize.parse("32m"));

        ShuffleManagerProcessSpec processSpec = new ShuffleManagerProcessSpec(memConfig);

        assertThat(processSpec.getJvmHeapMemorySize(), is(MemorySize.parse("256m")));
        assertThat(processSpec.getJvmDirectMemorySize(), is(MemorySize.parse("128m")));
        assertThat(processSpec.getJvmOverheadSize(), is(MemorySize.parse("32m")));
        assertThat(processSpec.getJvmMetaspaceSize(), is(MemorySize.parse("32m")));
        assertThat(processSpec.getTotalProcessMemorySize(), is(MemorySize.parse("448m")));
    }
}
