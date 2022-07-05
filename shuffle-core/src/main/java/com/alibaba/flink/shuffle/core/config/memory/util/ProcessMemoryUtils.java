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

package com.alibaba.flink.shuffle.core.config.memory.util;

import com.alibaba.flink.shuffle.core.config.memory.ProcessMemorySpec;

import java.util.ArrayList;
import java.util.List;

/** Utils for calculating JVM args. */
public class ProcessMemoryUtils {

    public static String generateJvmArgsStr(ProcessMemorySpec memorySpec, String jvmExtraOptions) {
        List<String> commandStrings = new ArrayList<>();
        // jvm mem opts
        commandStrings.add(generateJvmMemArgsStr(memorySpec));
        // jvm extra opts
        commandStrings.add(jvmExtraOptions);

        return String.join(" ", commandStrings);
    }

    private static String generateJvmMemArgsStr(ProcessMemorySpec memorySpec) {
        final List<String> jvmArgs = new ArrayList<>();

        jvmArgs.add("-Xmx" + memorySpec.getJvmHeapMemorySize().getBytes());
        jvmArgs.add("-Xms" + memorySpec.getJvmHeapMemorySize().getBytes());
        jvmArgs.add("-XX:MaxDirectMemorySize=" + memorySpec.getJvmDirectMemorySize().getBytes());
        jvmArgs.add("-XX:MaxMetaspaceSize=" + memorySpec.getJvmMetaspaceSize().getBytes());

        return String.join(" ", jvmArgs);
    }
}
