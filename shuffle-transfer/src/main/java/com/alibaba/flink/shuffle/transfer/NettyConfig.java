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

package com.alibaba.flink.shuffle.transfer;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.common.utils.NetUtils;
import com.alibaba.flink.shuffle.core.config.TransferOptions;
import com.alibaba.flink.shuffle.core.config.WorkerOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkArgument;
import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;

/** Wrapper for network configurations. */
public class NettyConfig {

    private static final Logger LOG = LoggerFactory.getLogger(NettyConfig.class);

    public static final String SERVER_THREAD_GROUP_NAME = "Remote Shuffle Netty Server";

    public static final String CLIENT_THREAD_GROUP_NAME = "Remote Shuffle Netty Client";

    private final Configuration config; // optional configuration

    private final int numPreferredClientThreads;

    public NettyConfig(Configuration config) {
        this(config, 1);
    }

    public NettyConfig(Configuration config, int numPreferredClientThreads) {
        checkArgument(numPreferredClientThreads > 0, "Must be positive.");

        this.config = checkNotNull(config);

        this.numPreferredClientThreads = numPreferredClientThreads;

        LOG.info(this.toString());
    }

    public InetAddress getServerAddress() throws UnknownHostException {
        String address = checkNotNull(config.getString(WorkerOptions.BIND_HOST));
        return InetAddress.getByName(address);
    }

    public int getServerPort() {
        int serverPort = config.getInteger(TransferOptions.SERVER_DATA_PORT);
        checkArgument(NetUtils.isValidHostPort(serverPort), "Invalid port number.");
        return serverPort;
    }

    public int getServerConnectBacklog() {
        return config.getInteger(TransferOptions.CONNECT_BACKLOG);
    }

    public int getServerNumThreads() {
        int numThreads = config.getInteger(TransferOptions.NUM_THREADS_SERVER);
        checkArgument(numThreads > 0, "Number of server thread must be positive.");
        return numThreads;
    }

    public int getClientNumThreads() {
        int configValue = config.getInteger(TransferOptions.NUM_THREADS_CLIENT);
        return configValue <= 0 ? numPreferredClientThreads : configValue;
    }

    public int getConnectionRetries() {
        int connectionRetries = config.getInteger(TransferOptions.CONNECTION_RETRIES);
        return Math.max(1, connectionRetries);
    }

    public Duration getConnectionRetryWait() {
        return config.getDuration(TransferOptions.CONNECTION_RETRY_WAIT);
    }

    public int getClientConnectTimeoutSeconds() {
        return CommonUtils.checkedDownCast(
                config.getDuration(TransferOptions.CLIENT_CONNECT_TIMEOUT).getSeconds());
    }

    public int getSendAndReceiveBufferSize() {
        return CommonUtils.checkedDownCast(
                config.getMemorySize(TransferOptions.SEND_RECEIVE_BUFFER_SIZE).getBytes());
    }

    public int getHeartbeatTimeoutSeconds() {
        int heartbeatTimeout =
                CommonUtils.checkedDownCast(
                        config.getDuration(TransferOptions.HEARTBEAT_TIMEOUT).getSeconds());
        checkArgument(
                heartbeatTimeout > 0,
                "Heartbeat timeout must be positive and no less than 1 second.");
        return heartbeatTimeout;
    }

    public int getHeartbeatIntervalSeconds() {
        int heartbeatInterval =
                CommonUtils.checkedDownCast(
                        config.getDuration(TransferOptions.HEARTBEAT_INTERVAL).getSeconds());
        checkArgument(
                heartbeatInterval > 0,
                "Heartbeat interval must be positive and no less than 1 second.");
        return heartbeatInterval;
    }

    public TransportType getTransportType() {
        String transport = config.getString(TransferOptions.TRANSPORT_TYPE);

        switch (transport) {
            case "nio":
                return TransportType.NIO;
            case "epoll":
                return TransportType.EPOLL;
            default:
                return TransportType.AUTO;
        }
    }

    public Configuration getConfig() {
        return config;
    }

    @Override
    public String toString() {
        String format =
                "NettyConfig ["
                        + "server port: %d, "
                        + "transport type: %s, "
                        + "number of server threads: %d, "
                        + "number of client threads: %d (%s), "
                        + "server connect backlog: %d (%s), "
                        + "client connect timeout (sec): %d, "
                        + "client connect retries: %d, "
                        + "client connect retry wait (sec): %d, "
                        + "send/receive buffer size (bytes): %d (%s), "
                        + "heartbeat timeout %d, "
                        + "heartbeat interval %d]";

        String def = "use Netty's default";
        String man = "manual";

        return String.format(
                format,
                getServerPort(),
                getTransportType(),
                getServerNumThreads(),
                getClientNumThreads(),
                getClientNumThreads() == 0 ? def : man,
                getServerConnectBacklog(),
                getServerConnectBacklog() == 0 ? def : man,
                getClientConnectTimeoutSeconds(),
                getConnectionRetries(),
                getConnectionRetryWait().getSeconds(),
                getSendAndReceiveBufferSize(),
                getSendAndReceiveBufferSize() == 0 ? def : man,
                getHeartbeatTimeoutSeconds(),
                getHeartbeatIntervalSeconds());
    }

    /** Netty transportation types. */
    public enum TransportType {
        NIO,
        EPOLL,
        AUTO
    }
}
