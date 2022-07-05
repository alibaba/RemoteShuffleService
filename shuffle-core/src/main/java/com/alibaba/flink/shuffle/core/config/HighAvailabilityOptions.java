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

/** The set of configuration options relating to high-availability settings. */
public class HighAvailabilityOptions {

    /**
     * Defines high-availability mode used for the cluster execution. A value of "NONE" signals no
     * highly available setup. To enable high-availability, set this mode to "ZOOKEEPER". Can also
     * be set to FQN of HighAvailability factory class.
     */
    public static final ConfigOption<String> HA_MODE =
            new ConfigOption<String>("remote-shuffle.high-availability.mode")
                    .defaultValue("NONE")
                    .description(
                            "Defines high-availability mode used for the cluster execution."
                                    + " To enable high-availability, set this mode to 'ZOOKEEPER' "
                                    + "or specify FQN of factory class.");
    // ------------------------------------------------------------------------
    //  ZooKeeper Options
    // ------------------------------------------------------------------------

    /**
     * The ZooKeeper quorum to use when running the remote shuffle cluster in a high-availability
     * mode with ZooKeeper.
     */
    public static final ConfigOption<String> HA_ZOOKEEPER_QUORUM =
            new ConfigOption<String>("remote-shuffle.ha.zookeeper.quorum")
                    .defaultValue(null)
                    .description(
                            "The ZooKeeper quorum to use when running the remote shuffle cluster "
                                    + "in a high-availability mode with ZooKeeper.");

    /**
     * The root path under which the remote shuffle cluster stores its entries in ZooKeeper.
     * Different remote shuffle clusters will be distinguished by the cluster id.
     */
    public static final ConfigOption<String> HA_ZOOKEEPER_ROOT =
            new ConfigOption<String>("remote-shuffle.ha.zookeeper.root-path")
                    .defaultValue("flink-remote-shuffle")
                    .description(
                            "The root path in ZooKeeper under which the remote shuffle cluster "
                                    + "stores its entries. Different remote shuffle clusters will "
                                    + "be distinguished by the cluster id.");

    // ------------------------------------------------------------------------
    //  ZooKeeper Client Settings
    // ------------------------------------------------------------------------

    /** Defines the session timeout for the ZooKeeper session. */
    public static final ConfigOption<Duration> ZOOKEEPER_SESSION_TIMEOUT =
            new ConfigOption<Duration>("remote-shuffle.ha.zookeeper.session-timeout")
                    .defaultValue(Duration.ofSeconds(60))
                    .description("Defines the session timeout for the ZooKeeper session.");

    /** Defines the connection timeout for the ZooKeeper client. */
    public static final ConfigOption<Duration> ZOOKEEPER_CONNECTION_TIMEOUT =
            new ConfigOption<Duration>("remote-shuffle.ha.zookeeper.connection-timeout")
                    .defaultValue(Duration.ofSeconds(15))
                    .description("Defines the connection timeout for the ZooKeeper client.");

    /** Defines the pause between consecutive connection retries. */
    public static final ConfigOption<Duration> ZOOKEEPER_RETRY_WAIT =
            new ConfigOption<Duration>("remote-shuffle.ha.zookeeper.retry-wait")
                    .defaultValue(Duration.ofSeconds(5))
                    .description("Defines the pause between consecutive connection retries.");

    /** Defines the number of connection retries before the client gives up. */
    public static final ConfigOption<Integer> ZOOKEEPER_MAX_RETRY_ATTEMPTS =
            new ConfigOption<Integer>("remote-shuffle.ha.zookeeper.max-retry-attempts")
                    .defaultValue(3)
                    .description(
                            "Defines the number of connection retries before the client gives up.");

    // ------------------------------------------------------------------------

    /** Not intended to be instantiated. */
    private HighAvailabilityOptions() {}
}
