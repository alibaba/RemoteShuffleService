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
import sun.misc.Signal;

/**
 * This signal handler / signal logger is based on Apache Hadoop's
 * org.apache.hadoop.util.SignalLogger.
 *
 * <p>This class is copied from Apache Flink (org.apache.flink.runtime.util.SignalHandler).
 */
public class SignalHandler {

    private static boolean registered = false;

    /** Our signal handler. */
    private static class Handler implements sun.misc.SignalHandler {

        private final Logger log;
        private final sun.misc.SignalHandler prevHandler;

        Handler(String name, Logger log) {
            this.log = log;
            prevHandler = Signal.handle(new Signal(name), this);
        }

        /**
         * Handle an incoming signal.
         *
         * @param signal The incoming signal
         */
        @Override
        public void handle(Signal signal) {
            log.info(
                    "RECEIVED SIGNAL {}: SIG{}. Shutting down as requested.",
                    signal.getNumber(),
                    signal.getName());
            prevHandler.handle(signal);
        }
    }

    /**
     * Register some signal handlers.
     *
     * @param log The slf4j logger
     */
    public static void register(final Logger log) {
        synchronized (SignalHandler.class) {
            if (registered) {
                return;
            }
            registered = true;

            final String[] signals =
                    OperatingSystem.isWindows()
                            ? new String[] {"TERM", "INT"}
                            : new String[] {"TERM", "HUP", "INT"};

            StringBuilder bld = new StringBuilder();
            bld.append("Registered UNIX signal handlers for [");

            String separator = "";
            for (String signalName : signals) {
                try {
                    new Handler(signalName, log);
                    bld.append(separator);
                    bld.append(signalName);
                    separator = ", ";
                } catch (Exception e) {
                    log.info("Error while registering signal handler", e);
                }
            }
            bld.append("]");
            log.info(bld.toString());
        }
    }
}
