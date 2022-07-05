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

package com.alibaba.flink.shuffle.metrics.entry;

import com.alibaba.metrics.Counter;
import com.alibaba.metrics.Gauge;
import com.alibaba.metrics.Metric;
import com.alibaba.metrics.MetricFilter;
import com.alibaba.metrics.MetricLevel;
import com.alibaba.metrics.MetricManager;
import com.alibaba.metrics.MetricName;
import com.alibaba.metrics.PersistentGauge;
import com.alibaba.metrics.integrate.ConfigFields;
import com.alibaba.metrics.integrate.MetricsIntegrateUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.Properties;

/** Tests for configuring metrics. */
public class MetricConfigTest {
    @Before
    public void setUp() {
        MetricManager.getIMetricManager().clear();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEnumValueOf() {
        MetricLevel.valueOf("test");
    }

    @Test
    public void testEnabled() {
        Properties properties = new Properties();
        Assert.assertTrue(MetricsIntegrateUtils.isEnabled(properties, "test"));

        properties.put("com.alibaba.metrics.tomcat.thread.enable", "false");
        Assert.assertFalse(
                MetricsIntegrateUtils.isEnabled(
                        properties, "com.alibaba.metrics.tomcat.thread.enable"));

        properties.put("com.alibaba.metrics.tomcat.thread.enable", "true");
        Assert.assertTrue(
                MetricsIntegrateUtils.isEnabled(
                        properties, "com.alibaba.metrics.tomcat.thread.enable"));
    }

    @Test
    public void testConfigMetricLevel() {
        Properties properties = new Properties();
        properties.put("com.alibaba.metrics.jvm.class_load.level", "CRITICAL");
        MetricsIntegrateUtils.registerJvmMetrics(properties);
        Map<MetricName, Gauge> gauges =
                MetricManager.getIMetricManager()
                        .getGauges(
                                "jvm",
                                new MetricFilter() {
                                    @Override
                                    public boolean matches(MetricName name, Metric metric) {
                                        return name.getKey().equals("jvm.class_load.loaded");
                                    }
                                });
        Assert.assertEquals(1, gauges.size());
        Assert.assertEquals(
                MetricLevel.CRITICAL,
                gauges.entrySet().iterator().next().getKey().getMetricLevel());
    }

    @Test
    public void testCleaner() {
        Counter c = MetricManager.getCounter("cleaner", MetricName.build("test.cleaner"));
        c.inc();
        Properties properties = new Properties();
        properties.put(ConfigFields.METRICS_CLEANER_ENABLE, "true");
        properties.put(ConfigFields.METRICS_CLEANER_KEEP_INTERVAL, "1");
        properties.put(ConfigFields.METRICS_CLEANER_DELAY, "1");
        MetricsIntegrateUtils.startMetricsCleaner(properties);
        try {
            Thread.sleep(2000);
            Assert.assertEquals(
                    0,
                    MetricManager.getIMetricManager()
                            .getCounters("cleaner", MetricFilter.ALL)
                            .size());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        MetricsIntegrateUtils.stopMetricsCleaner();
    }

    @Test
    public void testDisableCleaner() {
        Counter c = MetricManager.getCounter("cleaner2", MetricName.build("test.cleaner2"));
        c.inc();
        Properties properties = new Properties();
        properties.put(ConfigFields.METRICS_CLEANER_ENABLE, "false");
        MetricsIntegrateUtils.startMetricsCleaner(properties);
        try {
            Thread.sleep(2000);
            Assert.assertEquals(
                    1,
                    MetricManager.getIMetricManager()
                            .getCounters("cleaner2", MetricFilter.ALL)
                            .size());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        MetricsIntegrateUtils.stopMetricsCleaner();
    }

    @Test
    public void testCleanPersistentGauge() {
        PersistentGauge<Integer> g =
                new PersistentGauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return 1;
                    }
                };
        MetricManager.register("ppp", MetricName.build("ppp1"), g);
        Properties properties = new Properties();
        properties.put(ConfigFields.METRICS_CLEANER_ENABLE, "true");
        properties.put(ConfigFields.METRICS_CLEANER_KEEP_INTERVAL, "1");
        properties.put(ConfigFields.METRICS_CLEANER_DELAY, "1");
        MetricsIntegrateUtils.startMetricsCleaner(properties);
        try {
            Thread.sleep(2000);
            Assert.assertEquals(
                    1, MetricManager.getIMetricManager().getGauges("ppp", MetricFilter.ALL).size());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        MetricsIntegrateUtils.stopMetricsCleaner();
    }

    @Test
    public void testDoNotBeCleaned() {
        MetricManager.getCounter("cleaner3", MetricName.build("test.cleaner"));
        Properties properties = new Properties();
        properties.put(ConfigFields.METRICS_CLEANER_ENABLE, "true");
        properties.put(ConfigFields.METRICS_CLEANER_KEEP_INTERVAL, "10");
        properties.put(ConfigFields.METRICS_CLEANER_DELAY, "1");
        MetricsIntegrateUtils.startMetricsCleaner(properties);
        try {
            Thread.sleep(2000);
            Assert.assertEquals(
                    "Because keep interval is 10 seconds, the counter should not be cleaned.",
                    1,
                    MetricManager.getIMetricManager()
                            .getCounters("cleaner3", MetricFilter.ALL)
                            .size());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        MetricsIntegrateUtils.stopMetricsCleaner();
    }

    @Test
    public void testDisableFromSystemProperty() {
        System.setProperty(ConfigFields.METRICS_CLEANER_ENABLE, "false");
        Assert.assertFalse(
                MetricsIntegrateUtils.isEnabled(null, ConfigFields.METRICS_CLEANER_ENABLE));
        System.setProperty(ConfigFields.METRICS_CLEANER_ENABLE, "true");
    }
}
