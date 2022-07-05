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

package com.alibaba.flink.shuffle.metrics;

/** Common constants used in metrics and reporters. */
public class MetricsConstants {

    /** The prefix string for all reporter configurations. */
    public static final String METRICS_REPORTER_PREFIX = "metrics.reporter.";

    /** The delimiter used for configurations. */
    public static final String CONFIGURATION_ARGS_DELIMITER = ",";

    // ---------------------------------------------------------------
    // Metrics suffixes
    // ---------------------------------------------------------------
    public static final String METRIC_COUNT_SUFFIX = "count";
    public static final String METRIC_MAX_SUFFIX = "max";
    public static final String METRIC_MIN_SUFFIX = "min";
    public static final String METRIC_MEAN_SUFFIX = "mean";
    public static final String METRIC_MEDIAN_SUFFIX = "median";
    public static final String METRIC_50PERCENTILE_SUFFIX = "50percentile";
    public static final String METRIC_75PERCENTILE_SUFFIX = "75percentile";
    public static final String METRIC_95PERCENTILE_SUFFIX = "95percentile";
    public static final String METRIC_98PERCENTILE_SUFFIX = "98percentile";
    public static final String METRIC_99PERCENTILE_SUFFIX = "99percentile";
    public static final String METRIC_999PERCENTILE_SUFFIX = "999percentile";
    public static final String METRIC_STD_DEV_SUFFIX = "stddev";
    public static final String METRIC_MEAN_RATE_SUFFIX = "rate_mean";
    public static final String METRIC_1_MIN_RATE_SUFFIX = "rate_1_min";
    public static final String METRIC_5_MIN_RATE_SUFFIX = "rate_5_min";
    public static final String METRIC_15_MIN_RATE_SUFFIX = "rate_15_min";
}
