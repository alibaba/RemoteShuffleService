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

/** The set of configuration options relating to network stack. */
public class TransferOptions {

    /** Data port to write shuffle data to and read shuffle data from shuffle workers. */
    public static final ConfigOption<Integer> SERVER_DATA_PORT =
            new ConfigOption<Integer>("remote-shuffle.transfer.server.data-port")
                    .defaultValue(10086)
                    .description(
                            "Data port to write shuffle data to and read shuffle data from shuffle"
                                    + " workers.");

    /** The number of Netty threads at the server (shuffle worker) side. */
    public static final ConfigOption<Integer> NUM_THREADS_SERVER =
            new ConfigOption<Integer>("remote-shuffle.transfer.server.num-threads")
                    .defaultValue(Math.max(32, Runtime.getRuntime().availableProcessors()))
                    .description(
                            "The number of Netty threads at the server (shuffle worker) side.");

    /**
     * The maximum TCP connection backlog of the Netty server. The default 0 means that the Netty's
     * default value will be used.
     */
    public static final ConfigOption<Integer> CONNECT_BACKLOG =
            new ConfigOption<Integer>("remote-shuffle.transfer.server.backlog")
                    .defaultValue(0) // default: 0 => Netty's default
                    .description(
                            "The maximum TCP connection backlog of the Netty server. The default "
                                    + "'0' means that the Netty's default value will be used.");

    /**
     * The number of Netty threads at the client (flink job) side. The default '-1' means that 2 *
     * (the number of slots) will be used.
     */
    public static final ConfigOption<Integer> NUM_THREADS_CLIENT =
            new ConfigOption<Integer>("remote-shuffle.transfer.client.num-threads")
                    .defaultValue(-1) // default: -1 => Not specified and let upper layer deduce.
                    .description(
                            "The number of Netty threads at the client (flink job) side. The "
                                    + "default '-1' means that 2 * (the number of slots) will be "
                                    + "used.");

    /** The TCP connection setup timeout of the Netty client. */
    public static final ConfigOption<Duration> CLIENT_CONNECT_TIMEOUT =
            new ConfigOption<Duration>("remote-shuffle.transfer.client.connect-timeout")
                    .defaultValue(Duration.ofMinutes(2))
                    .description("The TCP connection setup timeout of the Netty client.");

    /** Number of retries when failed to connect to the remote shuffle worker. */
    public static final ConfigOption<Integer> CONNECTION_RETRIES =
            new ConfigOption<Integer>("remote-shuffle.transfer.client.connect-retries")
                    .defaultValue(3)
                    .description(
                            "Number of retries when failed to connect to the remote shuffle worker.");

    /** Time to wait between two consecutive connection retries. */
    public static final ConfigOption<Duration> CONNECTION_RETRY_WAIT =
            new ConfigOption<Duration>("remote-shuffle.transfer.client.connect-retry-wait")
                    .defaultValue(Duration.ofSeconds(3))
                    .description("Time to wait between two consecutive connection retries.");

    /**
     * The Netty transport type, either 'nio' or 'epoll'. The 'auto' means selecting the proper mode
     * automatically based on the platform. Note that the 'epoll' mode can get better performance,
     * less GC and have more advanced features which are only available on modern Linux.
     */
    public static final ConfigOption<String> TRANSPORT_TYPE =
            new ConfigOption<String>("remote-shuffle.transfer.transport-type")
                    .defaultValue("auto")
                    .description(
                            "The Netty transport type, either 'nio' or 'epoll'. The 'auto' means "
                                    + "selecting the proper mode automatically based on the platform."
                                    + " Note that the 'epoll' mode can get better performance, less"
                                    + " GC and have more advanced features which are only available"
                                    + " on modern Linux.");
    /**
     * The Netty send and receive buffer size. The default '0' means the system buffer size (cat
     * /proc/sys/net/ipv4/tcp_[rw]mem) and is 4 MiB in modern Linux.
     */
    public static final ConfigOption<MemorySize> SEND_RECEIVE_BUFFER_SIZE =
            new ConfigOption<MemorySize>("remote-shuffle.transfer.send-receive-buffer-size")
                    .defaultValue(MemorySize.ZERO) // default: 0 => Netty's default
                    .description(
                            "The Netty send and receive buffer size. The default '0b' means the "
                                    + "system buffer size (cat /proc/sys/net/ipv4/tcp_[rw]mem) and"
                                    + " is 4 MiB in modern Linux.");

    /** The time interval to send heartbeat between the Netty server and Netty client. */
    public static final ConfigOption<Duration> HEARTBEAT_INTERVAL =
            new ConfigOption<Duration>("remote-shuffle.transfer.heartbeat.interval")
                    .defaultValue(Duration.ofMinutes(1))
                    .description(
                            "The time interval to send heartbeat between the Netty server and Netty"
                                    + " client.");

    /** Heartbeat timeout used to detect broken TCP connections. */
    public static final ConfigOption<Duration> HEARTBEAT_TIMEOUT =
            new ConfigOption<Duration>("remote-shuffle.transfer.heartbeat.timeout")
                    .defaultValue(Duration.ofMinutes(5))
                    .description("Heartbeat timeout used to detect broken Netty connections.");

    // ------------------------------------------------------------------------

    /** Not intended to be instantiated. */
    private TransferOptions() {}
}
