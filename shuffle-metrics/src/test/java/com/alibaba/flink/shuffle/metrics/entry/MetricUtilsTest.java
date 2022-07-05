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

import com.alibaba.metrics.CachedGauge;
import com.alibaba.metrics.Clock;
import com.alibaba.metrics.Compass;
import com.alibaba.metrics.CompassImpl;
import com.alibaba.metrics.Counter;
import com.alibaba.metrics.CounterImpl;
import com.alibaba.metrics.FastCompass;
import com.alibaba.metrics.FastCompassImpl;
import com.alibaba.metrics.Gauge;
import com.alibaba.metrics.Histogram;
import com.alibaba.metrics.HistogramImpl;
import com.alibaba.metrics.ManualClock;
import com.alibaba.metrics.Meter;
import com.alibaba.metrics.MeterImpl;
import com.alibaba.metrics.MetricFilter;
import com.alibaba.metrics.MetricManager;
import com.alibaba.metrics.MetricName;
import com.alibaba.metrics.Reservoir;
import com.alibaba.metrics.ReservoirType;
import com.alibaba.metrics.Snapshot;
import com.alibaba.metrics.Timer;
import com.alibaba.metrics.TimerImpl;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link MetricUtils}. This class is used to test metric values. Different metric values
 * will be checked after updated in this class.
 */
public class MetricUtilsTest {

    private static final String TEST_GROUP = "test.group";

    // Test for counter
    private final Counter counter = new CounterImpl();

    // Test for Timer
    private final Reservoir reservoir = new FakedMetrics.FakedReservoir();

    private final Clock clock =
            new Clock() {
                // a mock clock that increments its ticker by 50 ms per call
                private long val = 0;

                @Override
                public long getTick() {
                    return val += 50000000;
                }
            };
    private final Timer timer = new TimerImpl(reservoir, clock, 60);

    // Test for Histogram
    private final FakedMetrics.FakedReservoir histogramReservoir =
            new FakedMetrics.FakedReservoir();
    private final Histogram histogram =
            new HistogramImpl(histogramReservoir, 10, 2, Clock.defaultClock());

    // Test for Gauge
    private final AtomicInteger value = new AtomicInteger(0);
    private final Gauge<Integer> gauge =
            new CachedGauge<Integer>(100, TimeUnit.MILLISECONDS) {
                @Override
                protected Integer loadValue() {
                    return value.incrementAndGet();
                }
            };

    // Test for Meter
    private final FakedMetrics.FakedClock timerClock = new FakedMetrics.FakedClock();

    private final Meter meter = new MeterImpl(timerClock);

    // Test for Compass
    private final Reservoir compassReservoir = new FakedMetrics.FakedReservoir();
    private final Clock compassClock =
            new Clock() {
                // a mock clock that increments its ticker by 50 ms per call
                private long val = 0;

                @Override
                public long getTick() {
                    return val += 50000000;
                }
            };

    private CompassImpl compass;

    @Before
    public void setUp() throws Exception {
        compass = new CompassImpl(ReservoirType.BUCKET, compassClock, 10, 60, 100, 2);
        compass.setReservoir(compassReservoir);
    }

    @Test
    public void testCounterValueUpdate() {
        final Counter counter = MetricUtils.getCounter(TEST_GROUP, "testCounter");
        assertEquals(0, counter.getCount());
        counter.inc();
        assertEquals(1, counter.getCount());
        counter.dec();
        counter.dec();
        assertEquals(-1, counter.getCount());
        counter.inc();
        assertEquals(0, counter.getCount());

        // increase more than 1
        counter.inc(20);
        assertEquals(20, counter.getCount());
        // decrease more than 1
        counter.dec(40);
        assertEquals(-20, counter.getCount());
        assertEquals(
                MetricManager.getIMetricManager()
                        .getCounter(TEST_GROUP, MetricName.build("testCounter"))
                        .getCount(),
                counter.getCount());
    }

    @Test
    public void testTimerInitValues() {
        assertTrue(MetricManager.getIMetricManager().listMetricGroups().contains(TEST_GROUP));
        assertEquals(0, timer.getCount());

        assertEquals(0.0, timer.getMeanRate(), 0.01);

        assertEquals(0.0, timer.getOneMinuteRate(), 0.001);

        assertEquals(0.0, timer.getOneMinuteRate(), 0.001);

        assertEquals(0.0, timer.getFifteenMinuteRate(), 0.001);
    }

    @Test
    public void testTimerValueUpdate() throws Exception {
        MetricUtils.registerMetric(TEST_GROUP, "testTimer", timer);
        // Test for ignore negative values
        timer.update(-1, TimeUnit.SECONDS);
        assertEquals(0, timer.getCount());

        // Test for update values
        assertEquals(0, timer.getCount());
        timer.update(1, TimeUnit.SECONDS);
        assertEquals(1, timer.getCount());
        timer.time().stop();
        assertEquals(2, timer.getCount());

        assertEquals(
                MetricManager.getIMetricManager()
                        .getTimer(TEST_GROUP, MetricName.build("testTimer"))
                        .getCount(),
                timer.getCount());
    }

    @Test
    public void testTimerTimesCallableInstances() throws Exception {
        final String value = timer.time(() -> "one");

        assertEquals(1, timer.getCount());
        assertEquals("one", value);
    }

    @Test
    public void testHistogramValueUpdate() {
        assertEquals(0, histogram.getCount());
        histogram.update(1);
        assertEquals(1, histogram.getCount());

        // test snapshot
        final Snapshot snapshot = new FakedMetrics.FakedSnapshot();
        histogramReservoir.setSnapshot(snapshot);
        assertEquals(histogram.getSnapshot(), snapshot);

        histogram.update(1);
        assertEquals(2, histogramReservoir.getUpdateCount());

        // update more values
        ManualClock clock = new ManualClock();
        Histogram histogram = new HistogramImpl(ReservoirType.BUCKET, 5, 2, clock);
        MetricUtils.registerMetric(TEST_GROUP, "testHistogram", histogram);
        clock.addSeconds(10);
        histogram.update(10);
        histogram.update(20);
        Snapshot snapshot1 = histogram.getSnapshot();
        assertEquals(15, snapshot1.getMean(), 0.001);
        clock.addSeconds(6);
        histogram.update(200);
        histogram.update(400);
        clock.addSeconds(5);
        Snapshot snapshot2 = histogram.getSnapshot();
        assertEquals(300, snapshot2.getMean(), 0.001);
        assertEquals(
                MetricManager.getIMetricManager()
                        .getHistogram(TEST_GROUP, MetricName.build("testHistogram"))
                        .getCount(),
                histogram.getCount());
    }

    @Test
    public void testGaugeValueUpdate() throws Exception {
        MetricUtils.registerMetric(TEST_GROUP, "testGauge", gauge);
        assertEquals(1, gauge.getValue().intValue());
        long lastUpdateTime = gauge.lastUpdateTime();
        assertEquals(1, gauge.getValue().intValue());
        assertEquals(lastUpdateTime, gauge.lastUpdateTime());
        Thread.sleep(150);
        assertEquals(2, gauge.getValue().intValue());
        assertEquals(2, gauge.getValue().intValue());
        assertEquals(
                MetricManager.getIMetricManager()
                        .getGauges(TEST_GROUP, MetricFilter.ALL)
                        .get(MetricName.build("testGauge"))
                        .getValue(),
                gauge.getValue());
    }

    @Test
    public void testMeterInitValue() {
        // Test init value
        assertEquals(0, meter.getCount());
        assertEquals(0.0, meter.getMeanRate(), 0.001);
        assertEquals(0.0, meter.getOneMinuteRate(), 0.001);
        assertEquals(0.0, meter.getFiveMinuteRate(), 0.001);
        assertEquals(0.0, meter.getFifteenMinuteRate(), 0.001);
    }

    @Test
    public void testMeterValueUpdate() {
        MetricUtils.registerMetric(TEST_GROUP, "testMeter", meter);
        // Test mark
        meter.mark();
        timerClock.setCurTime(TimeUnit.SECONDS.toNanos(10));
        meter.mark(2);
        assertEquals(0.3, meter.getMeanRate(), 0.001);
        assertEquals(0.1840, meter.getOneMinuteRate(), 0.001);
        assertEquals(0.1966, meter.getFiveMinuteRate(), 0.001);
        assertEquals(0.1988, meter.getFifteenMinuteRate(), 0.001);
        assertNotNull(
                MetricManager.getIMetricManager()
                        .getMeter(TEST_GROUP, MetricName.build("testMeter")));
    }

    @Test
    public void testCompassValueUpdate() {
        Compass.Context context = compass.time();
        context.markAddon("hit");
        context.markAddon("loss");
        context.markAddon("goodbye");
        context.stop();

        assertEquals(1, compass.getCount());

        assertEquals(1, compass.getAddonCounts().get("hit").getCount());
        assertEquals(1, compass.getAddonCounts().get("loss").getCount());
        assertEquals(compass.getAddonCounts().get("goodbye"), null);

        // Test multiple update values
        ManualClock clock = new ManualClock();
        Compass compass = new CompassImpl(ReservoirType.BUCKET, clock, 10, 60, 10, 5);
        MetricUtils.registerMetric(TEST_GROUP, "testCompass", compass);
        compass.update(10, TimeUnit.MILLISECONDS, true, null, "hit");
        compass.update(15, TimeUnit.MILLISECONDS, true, null, null);
        compass.update(20, TimeUnit.MILLISECONDS, false, "error1", null);
        clock.addSeconds(60);
        assertEquals(compass.getCount(), 3);
        assertEquals(compass.getSuccessCount(), 2);
        assertEquals(1, compass.getAddonCounts().get("hit").getBucketCounts().get(0L).intValue());
        assertEquals(compass.getSnapshot().getMean(), TimeUnit.MILLISECONDS.toNanos(15), 0.001);

        compass.update(10, TimeUnit.MILLISECONDS, true, null, "hit");
        compass.update(15, TimeUnit.MILLISECONDS, true, null, null);
        compass.update(20, TimeUnit.MILLISECONDS, false, "error1", null);

        clock.addSeconds(60);
        assertEquals(6, compass.getCount());
        assertEquals(3, compass.getInstantCount().get(60000L).intValue());
        assertEquals(4, compass.getSuccessCount());
        assertEquals(2, compass.getBucketSuccessCount().getBucketCounts().get(60000L).intValue());
        assertEquals(
                1, compass.getAddonCounts().get("hit").getBucketCounts().get(60000L).intValue());
        assertEquals(compass.getSnapshot().getMean(), TimeUnit.MILLISECONDS.toNanos(15), 0.001);
        assertEquals(
                MetricManager.getIMetricManager()
                        .getCompass(TEST_GROUP, MetricName.build("testCompass"))
                        .getSuccessCount(),
                compass.getSuccessCount());
    }

    @Test
    public void testFastCompassValueUpdate() {
        ManualClock clock = new ManualClock();
        FastCompass fastCompass = new FastCompassImpl(60, 10, clock, 10);
        MetricUtils.registerMetric(TEST_GROUP, "testFastCompass", fastCompass);
        fastCompass.record(10, "success");
        fastCompass.record(20, "error");
        fastCompass.record(15, "success");
        clock.addSeconds(60);
        // verify count
        assertTrue(fastCompass.getMethodCountPerCategory().containsKey("success"));
        assertEquals(
                2, fastCompass.getMethodCountPerCategory(0L).get("success").get(0L).intValue());
        assertTrue(fastCompass.getMethodCountPerCategory().containsKey("error"));
        assertEquals(1, fastCompass.getMethodCountPerCategory(0L).get("error").get(0L).intValue());
        // verify rt
        assertTrue(fastCompass.getMethodRtPerCategory().containsKey("success"));
        assertEquals(25, fastCompass.getMethodRtPerCategory(0L).get("success").get(0L).intValue());
        assertTrue(fastCompass.getMethodRtPerCategory().containsKey("error"));
        assertEquals(20, fastCompass.getMethodRtPerCategory(0L).get("error").get(0L).intValue());
        // total count
        long totalCount =
                fastCompass.getMethodCountPerCategory(0L).get("success").get(0L)
                        + fastCompass.getMethodCountPerCategory(0L).get("error").get(0L);
        assertEquals(3, totalCount);
        // average rt
        long avgRt =
                (fastCompass.getMethodRtPerCategory(0L).get("success").get(0L)
                                + fastCompass.getMethodRtPerCategory(0L).get("error").get(0L))
                        / totalCount;
        assertEquals(15, avgRt);
        // verify count and rt
        assertTrue(fastCompass.getCountAndRtPerCategory().containsKey("success"));
        assertEquals(
                (2L << 38) + 25,
                fastCompass.getCountAndRtPerCategory(0L).get("success").get(0L).longValue());
        assertTrue(fastCompass.getCountAndRtPerCategory().containsKey("error"));
        assertEquals(
                (1L << 38) + 20,
                fastCompass.getCountAndRtPerCategory(0L).get("error").get(0L).longValue());
        assertEquals(
                MetricManager.getIMetricManager()
                        .getFastCompass(TEST_GROUP, MetricName.build("testFastCompass"))
                        .getCountAndRtPerCategory()
                        .get("success")
                        .get(0L)
                        .longValue(),
                fastCompass.getCountAndRtPerCategory(0L).get("success").get(0L).longValue());
    }
}
