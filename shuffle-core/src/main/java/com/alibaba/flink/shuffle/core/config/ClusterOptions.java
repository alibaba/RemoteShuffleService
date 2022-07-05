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

/** Options which control the cluster behaviour. */
public class ClusterOptions {

    /**
     * The unique ID of the remote shuffle cluster used by HA. It must start with '/'. Different
     * shuffle clusters must be configured with different cluster id. This config must be consistent
     * between the shuffle manager and workers.
     */
    public static final ConfigOption<String> REMOTE_SHUFFLE_CLUSTER_ID =
            new ConfigOption<String>("remote-shuffle.cluster.id")
                    .defaultValue("/flink-shuffle-cluster")
                    .description(
                            "The unique ID of the remote shuffle cluster used by high-availability."
                                    + " It must start with '/'. Different shuffle clusters must be "
                                    + "configured with different cluster id. This config must be "
                                    + "consistent between the shuffle manager and workers.");

    /**
     * Defines the timeout for the shuffle worker or client registration to the shuffle manager. If
     * the duration is exceeded without a successful registration, then the shuffle worker or client
     * terminates.
     */
    public static final ConfigOption<Duration> REGISTRATION_TIMEOUT =
            new ConfigOption<Duration>("remote-shuffle.cluster.registration.timeout")
                    .defaultValue(Duration.ofMinutes(5))
                    .description(
                            "Defines the timeout for the shuffle worker or client registration to "
                                    + "the shuffle manager. If the duration is exceeded without a "
                                    + "successful registration, then the shuffle worker or client "
                                    + "terminates.");

    /** The pause made after a registration attempt caused an exception (other than timeout). */
    public static final ConfigOption<Duration> ERROR_REGISTRATION_DELAY =
            new ConfigOption<Duration>("remote-shuffle.cluster.registration.error-delay")
                    .defaultValue(Duration.ofSeconds(10))
                    .description(
                            "The pause made after a registration attempt caused an exception "
                                    + "(other than timeout).");

    /** The pause made after the registration attempt was refused. */
    public static final ConfigOption<Duration> REFUSED_REGISTRATION_DELAY =
            new ConfigOption<Duration>("remote-shuffle.cluster.registration.refused-delay")
                    .defaultValue(Duration.ofSeconds(30))
                    .description("The pause made after the registration attempt was refused.");

    // ------------------------------------------------------------------------

    /** Not intended to be instantiated. */
    private ClusterOptions() {}
}
