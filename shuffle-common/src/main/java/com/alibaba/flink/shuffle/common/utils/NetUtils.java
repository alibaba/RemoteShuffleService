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

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Random;

/** Network related Utility methods. */
public class NetUtils {

    public static int getAvailablePort() {
        Random random = new Random();
        for (int i = 0; i < 1024; i++) {
            int port = random.nextInt(65535 - 10240) + 10240;
            try (ServerSocket serverSocket = new ServerSocket(port)) {
                return serverSocket.getLocalPort();
            } catch (IOException ignored) {
            }
        }
        throw new RuntimeException("Could not find a free permitted port on the machine.");
    }

    public static boolean isValidHostPort(int port) {
        return 0 <= port && port <= 65535;
    }
}
