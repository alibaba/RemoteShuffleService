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

/** The configuration for the shuffle manager. */
public class ManagerOptions {

    // ------------------------------------------------------------------------
    //  General ShuffleManager Options
    // ------------------------------------------------------------------------

    /** Defines the network address to connect to for communication with the shuffle manager. */
    public static final ConfigOption<String> RPC_ADDRESS =
            new ConfigOption<String>("remote-shuffle.manager.rpc-address")
                    .defaultValue(null)
                    .description(
                            "Defines the network address to connect to for communication with the "
                                    + "shuffle manager.");

    /** The local address of the network interface that the shuffle manager binds to. */
    public static final ConfigOption<String> RPC_BIND_ADDRESS =
            new ConfigOption<String>("remote-shuffle.manager.rpc-bind-address")
                    .defaultValue(null)
                    .description(
                            "The local address of the network interface that the shuffle manager "
                                    + "binds to.");

    /** Defines the network port to connect to for communication with the shuffle manager. */
    public static final ConfigOption<Integer> RPC_PORT =
            new ConfigOption<Integer>("remote-shuffle.manager.rpc-port")
                    .defaultValue(23123)
                    .description(
                            "Defines the network port to connect to for communication with the "
                                    + "shuffle manager.");

    /** The local network port that the shuffle manager binds to. */
    public static final ConfigOption<Integer> RPC_BIND_PORT =
            new ConfigOption<Integer>("remote-shuffle.manager.rpc-bind-port")
                    .defaultValue(null)
                    .description(
                            "The local network port that the shuffle manager binds to. If not "
                                    + "configured, the external port (configured by '"
                                    + RPC_PORT.key()
                                    + "') will be used.");

    // ------------------------------------------------------------------------
    //  ShuffleManager Memory Options
    // ------------------------------------------------------------------------

    /** Heap memory size to be used by the shuffle manager. */
    public static final ConfigOption<MemorySize> FRAMEWORK_HEAP_MEMORY =
            new ConfigOption<MemorySize>("remote-shuffle.manager.memory.heap-size")
                    .defaultValue(MemorySize.parse("4g"))
                    .description("Heap memory size to be used by the shuffle manager.");

    /** Off-heap memory size to be used by the shuffle manager. */
    public static final ConfigOption<MemorySize> FRAMEWORK_OFF_HEAP_MEMORY =
            new ConfigOption<MemorySize>("remote-shuffle.manager.memory.off-heap-size")
                    .defaultValue(MemorySize.parse("128m"))
                    .description("Off-heap memory size to be used by the shuffle manager.");

    /** JVM metaspace size to be used by the shuffle manager. */
    public static final ConfigOption<MemorySize> JVM_METASPACE =
            new ConfigOption<MemorySize>("remote-shuffle.manager.memory.jvm-metaspace-size")
                    .defaultValue(MemorySize.parse("128m"))
                    .description("JVM metaspace size to be used by the shuffle manager.");

    /** JVM overhead size for the shuffle manager java process. */
    public static final ConfigOption<MemorySize> JVM_OVERHEAD =
            new ConfigOption<MemorySize>("remote-shuffle.manager.memory.jvm-overhead-size")
                    .defaultValue(MemorySize.parse("128m"))
                    .description("JVM overhead size for the shuffle manager java process.");

    /** Java options to start the JVM of the shuffle manager with. */
    public static final ConfigOption<String> JVM_OPTIONS =
            new ConfigOption<String>("remote-shuffle.manager.jvm-opts")
                    .defaultValue("")
                    .description("Java options to start the JVM of the shuffle manager with.");

    /**
     * Worker selection strategy deciding which worker the next data partition to store. Different
     * selection strategies can be specified through this option.
     */
    public static final ConfigOption<String> PARTITION_PLACEMENT_STRATEGY =
            new ConfigOption<String>("remote-shuffle.manager.partition-placement-strategy")
                    .defaultValue("round-robin")
                    .description(
                            "Worker selection strategy deciding which worker the next data partition to store."
                                    + " Different selection strategies can be specified through this option:"
                                    + " 'min-num': select the next worker with minimum number of data partitions;"
                                    + " 'random': select the next worker in random order;"
                                    + " 'round-robin': select the next worker in round-robin order;"
                                    + " 'locality': select the local worker first, if not satisfied, select other remote workers in round-robin order.");

    // ------------------------------------------------------------------------

    /** Not intended to be instantiated. */
    private ManagerOptions() {}
}
