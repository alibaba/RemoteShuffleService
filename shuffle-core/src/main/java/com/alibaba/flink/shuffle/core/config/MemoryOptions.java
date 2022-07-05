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

package com.alibaba.flink.shuffle.core.config;

import com.alibaba.flink.shuffle.common.config.ConfigOption;
import com.alibaba.flink.shuffle.common.config.MemorySize;

/** Config options for memory. */
public class MemoryOptions {

    /**
     * Minimum valid size of memory can be configured for data writing and reading. The target
     * configuration options include {@link #MEMORY_SIZE_FOR_DATA_WRITING} and {@link
     * #MEMORY_SIZE_FOR_DATA_READING}
     */
    public static final MemorySize MIN_VALID_MEMORY_SIZE = MemorySize.parse("64m");

    /**
     * Size of the buffer to be allocated. Those allocated buffers will be used by both network and
     * storage for data transmission, data writing and data reading.
     */
    public static final ConfigOption<MemorySize> MEMORY_BUFFER_SIZE =
            new ConfigOption<MemorySize>("remote-shuffle.memory.buffer-size")
                    .defaultValue(MemorySize.parse("32k"))
                    .description(
                            "Size of the buffer to be allocated. Those allocated buffers will be "
                                    + "used by both network and storage for data transmission, data"
                                    + " writing and data reading.");

    /**
     * Size of memory to be allocated for data writing. Larger value means more direct memory
     * consumption which may lead to better performance. The configured value must be no smaller
     * than {@link #MIN_VALID_MEMORY_SIZE} and the buffer size configured by {@link
     * #MEMORY_BUFFER_SIZE}, otherwise an exception will be thrown.
     */
    public static final ConfigOption<MemorySize> MEMORY_SIZE_FOR_DATA_WRITING =
            new ConfigOption<MemorySize>("remote-shuffle.memory.data-writing-size")
                    .defaultValue(MemorySize.parse("4g"))
                    .description(
                            String.format(
                                    "Size of memory to be allocated for data writing. Larger value "
                                            + "means more direct memory consumption which may lead "
                                            + "to better performance. The configured value must be "
                                            + "no smaller than %s and the buffer size configured by"
                                            + " %s, otherwise an exception will be thrown.",
                                    MIN_VALID_MEMORY_SIZE.toHumanReadableString(),
                                    MEMORY_BUFFER_SIZE.key()));

    /**
     * Size of memory to be allocated for data reading. Larger value means more direct memory
     * consumption which may lead to better performance. The configured value must be no smaller
     * than {@link #MIN_VALID_MEMORY_SIZE} and the buffer size configured by {@link
     * #MEMORY_BUFFER_SIZE}, otherwise an exception will be thrown.
     */
    public static final ConfigOption<MemorySize> MEMORY_SIZE_FOR_DATA_READING =
            new ConfigOption<MemorySize>("remote-shuffle.memory.data-reading-size")
                    .defaultValue(MemorySize.parse("4g"))
                    .description(
                            String.format(
                                    "Size of memory to be allocated for data reading. Larger value "
                                            + "means more direct memory consumption which may lead "
                                            + "to better performance. The configured value must be "
                                            + "no smaller than %s and the buffer size configured by"
                                            + " %s, otherwise an exception will be thrown.",
                                    MIN_VALID_MEMORY_SIZE.toHumanReadableString(),
                                    MEMORY_BUFFER_SIZE.key()));

    // ------------------------------------------------------------------------

    /** Not intended to be instantiated. */
    private MemoryOptions() {}
}
