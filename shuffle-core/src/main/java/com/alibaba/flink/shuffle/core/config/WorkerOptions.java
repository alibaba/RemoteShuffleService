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

import java.time.Duration;

/** The options for the shuffle worker. */
public class WorkerOptions {

    // ------------------------------------------------------------------------
    //  General ShuffleWorker Options
    // ------------------------------------------------------------------------

    /**
     * The external address of the network interface where the shuffle worker is exposed. If not
     * set, it will be determined automatically. Note: Different workers may need different values
     * for this usually it can be specified in a non-shared shuffle worker specific configuration
     * file.
     */
    public static final ConfigOption<String> HOST =
            new ConfigOption<String>("remote-shuffle.worker.host")
                    .defaultValue(null)
                    .description(
                            "The external address of the network interface where the shuffle worker"
                                    + " is exposed. If not set, it will be determined automatically."
                                    + " Note: Different workers may need different values for this "
                                    + "option, usually it can be specified in a non-shared shuffle"
                                    + " worker specific configuration file.");

    /**
     * The automatic address binding policy used by the shuffle worker if {@link #HOST} is not set.
     * The valid types include 'name' and 'ip': 'name' means using hostname as binding address, 'ip'
     * means using host's ip address as binding address.
     */
    public static final ConfigOption<String> HOST_BIND_POLICY =
            new ConfigOption<String>("remote-shuffle.worker.bind-policy")
                    .defaultValue("ip")
                    .description(
                            String.format(
                                    "The automatic address binding policy used by the shuffle"
                                            + " worker if '%s' is not set. The valid types include"
                                            + " 'name' and 'ip': 'name' means using hostname as "
                                            + "binding address, 'ip' means using host's ip address "
                                            + "as binding address.",
                                    HOST.key()));

    /** The local address of the network interface that the shuffle worker binds to. */
    public static final ConfigOption<String> BIND_HOST =
            new ConfigOption<String>("remote-shuffle.worker.bind-host")
                    .defaultValue("0.0.0.0")
                    .description(
                            "The local address of the network interface that the shuffle worker "
                                    + "binds to.");

    /**
     * Defines network port range the shuffle worker expects incoming RPC connections. Accepts a
     * list of ports (”50100,50101”), ranges (“50100-50200”) or a combination of both. The default
     * '0' means that the shuffle worker will search for a free port itself.
     */
    public static final ConfigOption<String> RPC_PORT =
            new ConfigOption<String>("remote-shuffle.worker.rpc-port")
                    .defaultValue("0")
                    .description(
                            "Defines network port range the shuffle worker expects incoming RPC "
                                    + "connections. Accepts a list of ports (”50100,50101”), ranges"
                                    + " (“50100-50200”) or a combination of both. The default '0' "
                                    + "means that the shuffle worker will search for a free port "
                                    + "itself.");

    /** The local network port that the shuffle worker binds to. */
    public static final ConfigOption<Integer> RPC_BIND_PORT =
            new ConfigOption<Integer>("remote-shuffle.worker.rpc-bind-port")
                    .defaultValue(null)
                    .description(
                            "The local network port that the shuffle worker binds to. If not "
                                    + "configured, the external port (configured by '"
                                    + RPC_PORT.key()
                                    + "') will be used.");

    /**
     * Maximum time to wait before reproducing the data stored in the lost worker (heartbeat
     * timeout). The lost worker may become available again in this timeout.
     */
    public static final ConfigOption<Duration> MAX_WORKER_RECOVER_TIME =
            new ConfigOption<Duration>("remote-shuffle.worker.max-recovery-time")
                    .defaultValue(Duration.ofMinutes(3))
                    .description(
                            "Maximum time to wait before reproducing the data stored in the lost "
                                    + "worker (heartbeat timeout). The lost worker may become "
                                    + "available again in this timeout.");

    // ------------------------------------------------------------------------
    //  ShuffleWorker Memory Options
    // ------------------------------------------------------------------------

    /** Heap memory size to be used by the shuffle worker. */
    public static final ConfigOption<MemorySize> FRAMEWORK_HEAP_MEMORY =
            new ConfigOption<MemorySize>("remote-shuffle.worker.memory.heap-size")
                    .defaultValue(MemorySize.parse("1g"))
                    .description("Heap memory size to be used by the shuffle worker.");

    /** Off-heap memory size to be used by the shuffle worker. */
    public static final ConfigOption<MemorySize> FRAMEWORK_OFF_HEAP_MEMORY =
            new ConfigOption<MemorySize>("remote-shuffle.worker.memory.off-heap-size")
                    .defaultValue(MemorySize.parse("128m"))
                    .description("Off-heap memory size to be used by the shuffle worker.");

    /** JVM metaspace size to be used by the shuffle worker. */
    public static final ConfigOption<MemorySize> JVM_METASPACE =
            new ConfigOption<MemorySize>("remote-shuffle.worker.memory.jvm-metaspace-size")
                    .defaultValue(MemorySize.parse("128m"))
                    .description("JVM metaspace size to be used by the shuffle worker.");

    /** JVM overhead size for the shuffle worker java process. */
    public static final ConfigOption<MemorySize> JVM_OVERHEAD =
            new ConfigOption<MemorySize>("remote-shuffle.worker.memory.jvm-overhead-size")
                    .defaultValue(MemorySize.parse("128m"))
                    .description("JVM overhead size for the shuffle worker java process.");

    /** Java options to start the JVM of the shuffle worker with. */
    public static final ConfigOption<String> JVM_OPTIONS =
            new ConfigOption<String>("remote-shuffle.worker.jvm-opts")
                    .defaultValue("")
                    .description("Java options to start the JVM of the shuffle worker with.");

    /** Not intended to be instantiated. */
    private WorkerOptions() {}
}
