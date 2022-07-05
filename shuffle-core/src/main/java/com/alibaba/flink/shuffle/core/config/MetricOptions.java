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

/** Config options for metrics. */
public class MetricOptions {

    /**
     * Specify the implementation classes of metrics reporter. Separate by ';' if there are multiple
     * class names. Each class name needs a package name prefix, e.g. a.b.c.Factory1;a.b.c.Factory2.
     */
    public static final ConfigOption<String> METRICS_REPORTER_CLASSES =
            new ConfigOption<String>("remote-shuffle.metrics.reporter.factories")
                    .defaultValue(null)
                    .description(
                            "Specify the implementation classes of metrics reporter. Separate by "
                                    + "';' if there are multiple class names. Each class name needs"
                                    + " a package name prefix, e.g. a.b.c.Factory1;a.b.c.Factory2.");

    // ------------------------------------------------------------------------

    /** Not intended to be instantiated. */
    private MetricOptions() {}
}
