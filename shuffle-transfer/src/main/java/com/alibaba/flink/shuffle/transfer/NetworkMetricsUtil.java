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

import com.alibaba.flink.shuffle.metrics.entry.MetricUtils;

import com.alibaba.metrics.Counter;
import com.alibaba.metrics.Meter;

/** Constants and util methods of network metrics. */
public class NetworkMetricsUtil {

    /** Metric group name. */
    public static final String NETWORK = "remote-shuffle.network";

    /** Current number of tcp writing connections. */
    public static final String NUM_WRITING_CONNECTIONS = NETWORK + ".num_writing_connections";

    /** Current number of tcp reading connections. */
    public static final String NUM_READING_CONNECTIONS = NETWORK + ".num_reading_connections";

    /** Current number of writing flows. */
    public static final String NUM_WRITING_FLOWS = NETWORK + ".num_writing_flows";

    /** Current number of reading flows. */
    public static final String NUM_READING_FLOWS = NETWORK + ".num_reading_flows";

    /** Current writing throughput in bytes. */
    public static final String WRITING_THROUGHPUT_BYTES = NETWORK + ".writing_throughput_bytes";

    /** Current reading throughput in bytes. */
    public static final String READING_THROUGHPUT_BYTES = NETWORK + ".reading_throughput_bytes";

    public static Counter registerNumWritingConnections() {
        return MetricUtils.getCounter(NETWORK, NUM_WRITING_CONNECTIONS);
    }

    public static Counter registerNumReadingConnections() {
        return MetricUtils.getCounter(NETWORK, NUM_READING_CONNECTIONS);
    }

    public static Counter registerNumWritingFlows() {
        return MetricUtils.getCounter(NETWORK, NUM_WRITING_FLOWS);
    }

    public static Counter registerNumReadingFlows() {
        return MetricUtils.getCounter(NETWORK, NUM_READING_FLOWS);
    }

    public static Meter registerWritingThroughputBytes() {
        return MetricUtils.getMeter(NETWORK, WRITING_THROUGHPUT_BYTES);
    }

    public static Meter registerReadingThroughputBytes() {
        return MetricUtils.getMeter(NETWORK, READING_THROUGHPUT_BYTES);
    }
}
