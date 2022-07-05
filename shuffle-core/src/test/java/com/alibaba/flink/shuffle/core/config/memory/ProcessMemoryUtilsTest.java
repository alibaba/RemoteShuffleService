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
import com.alibaba.flink.shuffle.core.config.memory.util.ProcessMemoryUtils;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/** Test for {@link ProcessMemoryUtils}. */
public class ProcessMemoryUtilsTest {

    @Test
    public void testGenerateJavaStartCommand() {
        ProcessMemorySpec memorySpec =
                new TestingProcessMemorySpec(
                        MemorySize.parse("32mb"),
                        MemorySize.parse("64mb"),
                        MemorySize.parse("128mb"),
                        MemorySize.parse("256mb"));

        String jvmOptions = "-XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:ParallelGCThreads=4";

        String command = ProcessMemoryUtils.generateJvmArgsStr(memorySpec, jvmOptions);

        assertThat(
                command,
                is(
                        "-Xmx"
                                + 32 * 1024 * 1024
                                + " -Xms"
                                + 32 * 1024 * 1024
                                + " -XX:MaxDirectMemorySize="
                                + 64 * 1024 * 1024
                                + " -XX:MaxMetaspaceSize="
                                + 128 * 1024 * 1024
                                + " -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:ParallelGCThreads=4"));
    }

    /** Simple {@link ProcessMemorySpec} implementation for testing purposes. */
    public static class TestingProcessMemorySpec implements ProcessMemorySpec {

        private final MemorySize heapMemory;
        private final MemorySize directMemory;
        private final MemorySize metaspace;
        private final MemorySize overhead;

        public TestingProcessMemorySpec(
                MemorySize heapMemory,
                MemorySize directMemory,
                MemorySize metaspace,
                MemorySize overhead) {
            this.directMemory = directMemory;
            this.heapMemory = heapMemory;
            this.metaspace = metaspace;
            this.overhead = overhead;
        }

        @Override
        public MemorySize getJvmHeapMemorySize() {
            return heapMemory;
        }

        @Override
        public MemorySize getJvmDirectMemorySize() {
            return directMemory;
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
            return heapMemory.add(directMemory).add(metaspace).add(overhead);
        }
    }
}
