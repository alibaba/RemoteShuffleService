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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * System.exit() need to be called when a fatal error is encountered. However, in some cases, such
 * as deployment on yarn, System.exit() can't be called because it may affect other processes, e.g.,
 * Yarn Node Manager process.
 *
 * <p>In order to avoid affecting other processes, this class will use different strategies to deal
 * with these fatal errors according to different deployment environments.
 */
public class FatalErrorExitUtils {
    private static final Logger LOG = LoggerFactory.getLogger(FatalErrorExitUtils.class);

    private static volatile boolean needStopProcess = true;

    public static void setNeedStopProcess(boolean needStopProcess) {
        FatalErrorExitUtils.needStopProcess = needStopProcess;
    }

    /** Only for tests. */
    public static boolean isNeedStopProcess() {
        return needStopProcess;
    }

    public static void exitProcessIfNeeded(int exitCode) {
        exitProcessIfNeeded(exitCode, null);
    }

    public static void exitProcessIfNeeded(int exitCode, Throwable t) {
        StringBuilder sb =
                new StringBuilder("Stopping the process with code ").append(exitCode).append(". ");
        sb.append("Whether the process should be exit? ").append(needStopProcess).append(". ");
        if (!needStopProcess) {
            sb.append("Ignore the stop operation and return directly.");
        }
        if (t != null) {
            LOG.error(sb.toString(), t);
        } else {
            LOG.error(sb.toString());
        }

        if (needStopProcess) {
            System.exit(exitCode);
        }
    }
}
