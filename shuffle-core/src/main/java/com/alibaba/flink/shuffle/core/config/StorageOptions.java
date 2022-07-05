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
import com.alibaba.flink.shuffle.core.storage.StorageType;

import java.time.Duration;

/** Config options for storage. */
public class StorageOptions {

    /** Minimum size of memory to be used for data partition writing and reading. */
    public static final MemorySize MIN_WRITING_READING_MEMORY_SIZE = MemorySize.parse("16m");

    /** Whether to enable data checksum for data integrity verification or not. */
    public static final ConfigOption<Boolean> STORAGE_ENABLE_DATA_CHECKSUM =
            new ConfigOption<Boolean>("remote-shuffle.storage.enable-data-checksum")
                    .defaultValue(false)
                    .description(
                            "Whether to enable data checksum for data integrity verification or not.");

    /**
     * Maximum number of tolerable failures before marking a data partition as corrupted, which will
     * trigger the reproduction of the corresponding data.
     */
    public static final ConfigOption<Integer> STORAGE_FILE_TOLERABLE_FAILURES =
            new ConfigOption<Integer>("remote-shuffle.storage.file-tolerable-failures")
                    .defaultValue(Integer.MAX_VALUE)
                    .description(
                            "Maximum number of tolerable failures before marking a data partition "
                                    + "as corrupted, which will trigger the reproduction of the "
                                    + "corresponding data.");

    /**
     * Local file system directories to persist partitioned data to. Multiple directories can be
     * configured and these directories should be separated by comma (,). Each configured directory
     * can be attached with an optional label which indicates the disk type. The valid disk types
     * include 'SSD' and 'HDD'. If no label is offered, the default type would be 'HDD'. Here is a
     * simple valid configuration example: <b>[SSD]/dir1/,[HDD]/dir2/,/dir3/</b>. This option must
     * be configured and the configured dirs must exist.
     */
    public static final ConfigOption<String> STORAGE_LOCAL_DATA_DIRS =
            new ConfigOption<String>("remote-shuffle.storage.local-data-dirs")
                    .defaultValue(null)
                    .description(
                            "Local file system directories to persist partitioned data to. Multiple"
                                    + " directories can be configured and these directories should "
                                    + "be separated by comma (,). Each configured directory can be "
                                    + "attached with an optional label which indicates the disk "
                                    + "type. The valid disk types include 'SSD' and 'HDD'. If no "
                                    + "label is offered, the default type would be 'HDD'. Here is a"
                                    + " simple valid configuration example: '[SSD]/dir1/,[HDD]/dir2"
                                    + "/,/dir3/'. This option must be configured and the configured"
                                    + " directories must exist.");

    /**
     * Preferred disk type to use for data storage. The valid types include 'SSD' and 'HDD'. If
     * there are disks of the preferred type, only those disks will be used. However, this is not a
     * strict restriction, which means if there is no disk of the preferred type, disks of other
     * types will be also used.
     */
    public static final ConfigOption<String> STORAGE_PREFERRED_TYPE =
            new ConfigOption<String>("remote-shuffle.storage.preferred-disk-type")
                    .defaultValue(StorageType.SSD.name())
                    .description(
                            "Preferred disk type to use for data storage. The valid types include "
                                    + "'SSD' and 'HDD'. If there are disks of the preferred type, "
                                    + "only those disks will be used. However, this is not a strict "
                                    + "restriction, which means if there is no disk of the preferred"
                                    + " type, disks of other types will be also used.");

    /**
     * Number of threads to be used by data store for data partition processing of each HDD. The
     * actual number of threads per disk will be min[configured value, 4 * (number of processors)].
     */
    public static final ConfigOption<Integer> STORAGE_NUM_THREADS_PER_HDD =
            new ConfigOption<Integer>("remote-shuffle.storage.hdd.num-executor-threads")
                    .defaultValue(8)
                    .description(
                            "Number of threads to be used by data store for data partition processing"
                                    + " of each HDD. The actual number of threads per disk will be "
                                    + "min[configured value, 4 * (number of processors)].");

    /**
     * Number of threads to be used by data store for data partition processing of each SSD. The
     * actual number of threads per disk will be min[configured value, 4 * (number of processors)].
     */
    public static final ConfigOption<Integer> STORAGE_SSD_NUM_EXECUTOR_THREADS =
            new ConfigOption<Integer>("remote-shuffle.storage.ssd.num-executor-threads")
                    .defaultValue(32)
                    .description(
                            "Number of threads to be used by data store for data partition processing"
                                    + " of each SSD. The actual number of threads per disk will be "
                                    + "min[configured value, 4 * (number of processors)].");

    /**
     * Number of threads to be used by data store for in-memory data partition processing. The
     * actual number of threads used will be min[configured value, 4 * (number of processors)].
     */
    public static final ConfigOption<Integer> STORAGE_MEMORY_NUM_EXECUTOR_THREADS =
            new ConfigOption<Integer>("remote-shuffle.storage.memory.num-executor-threads")
                    .defaultValue(Integer.MAX_VALUE)
                    .description(
                            "Number of threads to be used by data store for in-memory data partition"
                                    + " processing. The actual number of threads used will be min["
                                    + "configured value, 4 * (number of processors)].");

    /**
     * Maximum memory size to use for the data writing of each data partition. Note that if the
     * configured value is smaller than {@link #MIN_WRITING_READING_MEMORY_SIZE}, the minimum {@link
     * #MIN_WRITING_READING_MEMORY_SIZE} will be used.
     */
    public static final ConfigOption<MemorySize> STORAGE_MAX_PARTITION_WRITING_MEMORY =
            new ConfigOption<MemorySize>("remote-shuffle.storage.partition.max-writing-memory")
                    .defaultValue(MemorySize.parse("32m"))
                    .description(
                            String.format(
                                    "Maximum memory size to use for the data writing of each data "
                                            + "partition. Note that if the configured value is "
                                            + "smaller than %s, the minimum %s will be used.",
                                    MIN_WRITING_READING_MEMORY_SIZE.toHumanReadableString(),
                                    MIN_WRITING_READING_MEMORY_SIZE.toHumanReadableString()));

    /**
     * Maximum memory size to use for the data reading of each data partition. Note that if the
     * configured value is smaller than {@link #MIN_WRITING_READING_MEMORY_SIZE}, the minimum {@link
     * #MIN_WRITING_READING_MEMORY_SIZE} will be used.
     */
    public static final ConfigOption<MemorySize> STORAGE_MAX_PARTITION_READING_MEMORY =
            new ConfigOption<MemorySize>("remote-shuffle.storage.partition.max-reading-memory")
                    .defaultValue(MemorySize.parse("32m"))
                    .description(
                            String.format(
                                    "Maximum memory size to use for the data reading of each data "
                                            + "partition. Note that if the configured value is "
                                            + "smaller than %s, the minimum %s will be used.",
                                    MIN_WRITING_READING_MEMORY_SIZE.toHumanReadableString(),
                                    MIN_WRITING_READING_MEMORY_SIZE.toHumanReadableString()));

    /**
     * The update interval of the worker check thread which will periodically get the status like
     * disk space at this time interval.
     */
    public static final ConfigOption<Duration> STORAGE_CHECK_UPDATE_PERIOD =
            new ConfigOption<Duration>("remote-shuffle.storage.check-update-period")
                    .defaultValue(Duration.ofSeconds(10))
                    .description(
                            "The update interval of the worker check thread which will periodically"
                                    + " get the status like disk space at this time interval.");

    /**
     * Minimum reserved space size per disk for shuffle workers. This option is used to filter out
     * the workers with small remaining storage space to ensure that the storage space will not be
     * exhausted to avoid the 'No space left on device' exception. The default value 5g is pretty
     * small to avoid wasting too large storage resources and for production usage, we suggest
     * setting this to a larger value, for example, 50g.
     */
    public static final ConfigOption<MemorySize> STORAGE_MIN_RESERVED_SPACE_BYTES =
            new ConfigOption<MemorySize>("remote-shuffle.storage.min-reserved-space-bytes")
                    .defaultValue(MemorySize.parse("5g"))
                    .description(
                            "Minimum reserved space size per disk for shuffle workers. This option"
                                    + " is used to filter out the workers with small remaining "
                                    + "storage space to ensure that the storage space will not be "
                                    + "exhausted to avoid the 'No space left on device' exception."
                                    + " The default value 5g is pretty small to avoid wasting too "
                                    + "large storage resources and for production usage, we suggest"
                                    + " setting this to a larger value, for example, 50g.");

    /**
     * Maximum usable space size per disk for shuffle workers. This option is used to filter out the
     * workers which already store too much data and reserve enough space for other applications.
     */
    public static final ConfigOption<MemorySize> STORAGE_MAX_USABLE_SPACE_BYTES =
            new ConfigOption<MemorySize>("remote-shuffle.storage.max-usable-space-bytes")
                    .defaultValue(MemorySize.MAX_VALUE)
                    .description(
                            "Maximum usable space size per disk for shuffle workers. This option"
                                    + " is used to filter out the workers which already store too "
                                    + "much data and reserve enough space for other applications.");

    // ------------------------------------------------------------------------

    /** Not intended to be instantiated. */
    private StorageOptions() {}
}
