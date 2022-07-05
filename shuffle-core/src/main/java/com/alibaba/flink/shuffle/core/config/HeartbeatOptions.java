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

/** The configuration for the heart beat. */
public class HeartbeatOptions {

    /** Time interval for shuffle manager to request heartbeat from shuffle worker. */
    public static final ConfigOption<Duration> HEARTBEAT_WORKER_INTERVAL =
            new ConfigOption<Duration>("remote-shuffle.worker.heartbeat.interval")
                    .defaultValue(Duration.ofSeconds(10))
                    .description(
                            "Time interval for shuffle manager to request heartbeat from shuffle "
                                    + "worker.");

    /** Timeout for shuffle manager and shuffle worker to request and receive heartbeat. */
    public static final ConfigOption<Duration> HEARTBEAT_WORKER_TIMEOUT =
            new ConfigOption<Duration>("remote-shuffle.worker.heartbeat.timeout")
                    .defaultValue(Duration.ofSeconds(60))
                    .description(
                            "Timeout for shuffle manager and shuffle worker to request and receive"
                                    + " heartbeat.");

    /** Time interval for shuffle client to request heartbeat from shuffle manager. */
    public static final ConfigOption<Duration> HEARTBEAT_JOB_INTERVAL =
            new ConfigOption<Duration>("remote-shuffle.client.heartbeat.interval")
                    .defaultValue(Duration.ofSeconds(10))
                    .description(
                            "Time interval for shuffle client to request heartbeat from shuffle "
                                    + "manager.");

    /** Timeout for shuffle client and shuffle manager to request and receive heartbeat. */
    public static final ConfigOption<Duration> HEARTBEAT_JOB_TIMEOUT =
            new ConfigOption<Duration>("remote-shuffle.client.heartbeat.timeout")
                    .defaultValue(Duration.ofSeconds(120))
                    .description(
                            "Timeout for shuffle client and shuffle manager to request and receive"
                                    + " heartbeat.");

    // ------------------------------------------------------------------------

    /** Not intended to be instantiated. */
    private HeartbeatOptions() {}
}
