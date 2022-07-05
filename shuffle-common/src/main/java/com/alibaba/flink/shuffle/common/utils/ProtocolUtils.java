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

package com.alibaba.flink.shuffle.common.utils;

import static com.alibaba.flink.shuffle.common.utils.StringUtils.stringToBytes;

/** Utils to handle versions and protocol compatibility. */
public class ProtocolUtils {

    private static final int CURRENT_VERSION = 0;

    private static final int EMPTY_BUFFER_SIZE = Integer.MAX_VALUE;

    private static final long EMPTY_OFFSET = -2;

    private static final String EMPTY_EXTRA_MESSAGE = "{}";

    private static final String EMPTY_DATA_PARTITION_TYPE_FACTORY = "emptyDataPartitionTypeFactory";

    /** Returns the current protocol version used. */
    public static int currentProtocolVersion() {
        return CURRENT_VERSION;
    }

    /** Returns the number of empty byte buffers. */
    public static int emptyBufferSize() {
        return EMPTY_BUFFER_SIZE;
    }

    /** Returns the empty offset value. */
    public static long emptyOffset() {
        return EMPTY_OFFSET;
    }

    /** Returns the empty string of data partition type factory. */
    public static String emptyDataPartitionType() {
        return EMPTY_DATA_PARTITION_TYPE_FACTORY;
    }

    /** Returns the empty extra message. */
    public static String emptyExtraMessage() {
        return EMPTY_EXTRA_MESSAGE;
    }

    /** Returns the empty extra message bytes. */
    public static byte[] emptyExtraMessageBytes() {
        return stringToBytes(EMPTY_EXTRA_MESSAGE);
    }

    /**
     * Returns true if the client protocol version is compatible with the server protocol version,
     * including both control flow and data flow. This method is used at the server side.
     */
    public static boolean isClientProtocolCompatible(int clientProtocolVersion) {
        return compatibleVersion() <= clientProtocolVersion;
    }

    /**
     * Returns true if the client protocol version is compatible with the server protocol version,
     * including both control flow and data flow. This method is used at the client side.
     */
    public static boolean isServerProtocolCompatible(
            int serverProtocolVersion, int serverCompatibleVersion) {
        return currentProtocolVersion() <= serverProtocolVersion
                && serverCompatibleVersion <= currentProtocolVersion();
    }

    /** Returns the minimum supported version compatible with the current version. */
    public static int compatibleVersion() {
        return 0;
    }
}
