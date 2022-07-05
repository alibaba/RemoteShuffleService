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

import java.time.Duration;

/** Options for RPC service. */
public class RpcOptions {

    /** Timeout for client <-> manager and worker <-> manager rpc calls. */
    public static final ConfigOption<Duration> RPC_TIMEOUT =
            new ConfigOption<Duration>("remote-shuffle.rpc.timeout")
                    .defaultValue(Duration.ofSeconds(30))
                    .description(
                            "Timeout for client <-> manager and worker <-> manager rpc calls.");

    /** Maximum size of messages can be sent through rpc calls. */
    public static final ConfigOption<String> AKKA_FRAME_SIZE =
            new ConfigOption<String>("remote-shuffle.rpc.akka-frame-size")
                    .defaultValue("10485760b")
                    .description("Maximum size of messages can be sent through rpc calls.");

    // ------------------------------------------------------------------------

    /** Not intended to be instantiated. */
    private RpcOptions() {}
}
