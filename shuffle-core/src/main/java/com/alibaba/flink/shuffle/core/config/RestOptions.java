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

/** Options for rest. */
public class RestOptions {

    /** Local address of the network interface that the rest server binds to. */
    public static final ConfigOption<String> REST_BIND_HOST =
            new ConfigOption<String>("remote-shuffle.rest.bind-host")
                    .defaultValue("0.0.0.0")
                    .description(
                            "Local address of the network interface that the rest server binds to.");

    /** ShuffleManager rest server bind port. */
    public static final ConfigOption<Integer> REST_MANAGER_BIND_PORT =
            new ConfigOption<Integer>("remote-shuffle.rest.manager.bind-port")
                    .defaultValue(23101)
                    .description("ShuffleManager rest server bind port.");

    /** ShuffleWorker rest server bind port. */
    public static final ConfigOption<Integer> REST_WORKER_BIND_PORT =
            new ConfigOption<Integer>("remote-shuffle.rest.worker.bind-port")
                    .defaultValue(23103)
                    .description("ShuffleWorker rest server bind port.");

    // ------------------------------------------------------------------------

    /** Not intended to be instantiated. */
    private RestOptions() {}
}
