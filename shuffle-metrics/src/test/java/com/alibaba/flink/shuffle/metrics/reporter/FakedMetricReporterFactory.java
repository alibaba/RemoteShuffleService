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

import com.alibaba.flink.shuffle.common.config.Configuration;

import com.alibaba.metrics.reporter.MetricManagerReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/** A Faked implementation for {@link MetricReporterFactory}. */
public class FakedMetricReporterFactory implements MetricReporterFactory {
    private static final Logger LOG = LoggerFactory.getLogger(FakedMetricReporterFactory.class);
    static volatile int methodCallCount = 0;
    private static Configuration conf;

    @Override
    public MetricManagerReporter createMetricReporter(Properties conf) {
        methodCallCount++;
        LOG.info("Faked metric reporter method is called");
        this.conf = new Configuration(conf);
        return null;
    }

    public static Configuration getConf() {
        return conf;
    }

    public static int getMethodCallCount() {
        return methodCallCount;
    }

    public static void resetMethodCallCount() {
        methodCallCount = 0;
    }
}
