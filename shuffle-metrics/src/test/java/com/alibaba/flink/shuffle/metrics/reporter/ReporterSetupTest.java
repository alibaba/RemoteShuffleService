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

import org.junit.Test;

import java.util.Properties;

import static com.alibaba.flink.shuffle.core.config.MetricOptions.METRICS_REPORTER_CLASSES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests for {@link ReporterSetup}. */
public class ReporterSetupTest {
    @Test
    public void testInitReporterFromConfiguration() {
        FakedMetricReporterFactory.resetMethodCallCount();
        Properties properties = new Properties();
        properties.setProperty(
                METRICS_REPORTER_CLASSES.key(),
                "com.alibaba.flink.shuffle.metrics.reporter.FakedMetricReporterFactory");
        Configuration conf = new Configuration(properties);

        assertEquals(0, FakedMetricReporterFactory.getMethodCallCount());
        ReporterSetup.fromConfiguration(conf);
        assertEquals(1, FakedMetricReporterFactory.getMethodCallCount());
    }

    @Test
    public void testMultipleSameReporters() {
        FakedMetricReporterFactory.resetMethodCallCount();
        Properties properties = new Properties();
        properties.setProperty(
                METRICS_REPORTER_CLASSES.key(),
                "com.alibaba.flink.shuffle.metrics.reporter.FakedMetricReporterFactory,"
                        + "com.alibaba.flink.shuffle.metrics.reporter.FakedMetricReporterFactory");
        Configuration conf = new Configuration(properties);

        assertEquals(0, FakedMetricReporterFactory.getMethodCallCount());
        ReporterSetup.fromConfiguration(conf);
        assertEquals(1, FakedMetricReporterFactory.getMethodCallCount());
    }

    @Test
    public void testMultipleDifferentReporters() {
        FakedMetricReporterFactory.resetMethodCallCount();
        Properties properties = new Properties();
        properties.setProperty(
                METRICS_REPORTER_CLASSES.key(),
                "com.alibaba.flink.shuffle.metrics.reporter.FakedMetricReporterFactory,"
                        + "com.alibaba.flink.shuffle.metrics.reporter.AnotherFakedReporterFactory");
        Configuration conf = new Configuration(properties);

        assertEquals(0, FakedMetricReporterFactory.getMethodCallCount());
        ReporterSetup.fromConfiguration(conf);
        assertEquals(2, FakedMetricReporterFactory.getMethodCallCount());
    }

    @Test(expected = Exception.class)
    public void testLoadNonExistReporter() throws Exception {
        ReporterSetup.loadViaReflection("needFailed", new Configuration(new Properties()));
    }

    @Test
    public void testConfigurationArgsRight() {
        FakedMetricReporterFactory.resetMethodCallCount();
        assertTrue(
                FakedMetricReporterFactory.getConf() == null
                        || FakedMetricReporterFactory.getConf().getString("my.k1") == null);
        final String reporterKey = METRICS_REPORTER_CLASSES.key();
        final String reporterVal =
                "com.alibaba.flink.shuffle.metrics.reporter.FakedMetricReporterFactory";

        Properties properties = new Properties();
        properties.setProperty("my.k1", "v1");
        properties.setProperty("my.k2", "v2");
        properties.setProperty(reporterKey, reporterVal);
        Configuration conf = new Configuration(properties);
        ReporterSetup.fromConfiguration(conf);

        // Check args
        Configuration config = FakedMetricReporterFactory.getConf();
        assertTrue(config.getString("my.k1").equals("v1"));
        assertTrue(config.getString("my.k2").equals("v2"));
        assertFalse(config.getString("my.k1").equals("v2"));
        assertTrue(config.getString(reporterKey).equals(reporterVal));
    }
}
