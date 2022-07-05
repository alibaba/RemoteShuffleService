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

package com.alibaba.flink.shuffle.metrics.reporter;

import com.alibaba.metrics.reporter.MetricManagerReporter;

import java.util.Properties;

/**
 * {@link MetricManagerReporter} factory. Metric reporters that can be instantiated with a factory.
 */
public interface MetricReporterFactory {

    /**
     * Creates a new metric reporter.
     *
     * @param conf configurations for reporters
     * @return created metric reporter
     */
    MetricManagerReporter createMetricReporter(final Properties conf);
}
