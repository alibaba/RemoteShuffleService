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

import com.alibaba.metrics.BucketCounter;
import com.alibaba.metrics.BucketCounterImpl;
import com.alibaba.metrics.Compass;
import com.alibaba.metrics.CompassImpl;
import com.alibaba.metrics.Counter;
import com.alibaba.metrics.FastCompass;
import com.alibaba.metrics.FastCompassImpl;
import com.alibaba.metrics.Gauge;
import com.alibaba.metrics.Histogram;
import com.alibaba.metrics.HistogramImpl;
import com.alibaba.metrics.ManualClock;
import com.alibaba.metrics.Meter;
import com.alibaba.metrics.MeterImpl;
import com.alibaba.metrics.MetricName;
import com.alibaba.metrics.MetricRegistry;
import com.alibaba.metrics.MetricRegistryImpl;
import com.alibaba.metrics.MetricRegistryListener;
import com.alibaba.metrics.ReservoirType;
import com.alibaba.metrics.Timer;
import com.alibaba.metrics.TimerImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static com.alibaba.metrics.MetricRegistry.name;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests for registering metrics. */
public class MetricsRegistryTest {
    private static final MetricName TIMER2 = MetricName.build("timer");
    private static final MetricName METER2 = MetricName.build("meter");
    private static final MetricName HISTOGRAM2 = MetricName.build("histogram");
    private static final MetricName COUNTER = MetricName.build("counter2");
    private static final MetricName GAUGE = MetricName.build("gauge");
    private static final MetricName GAUGE2 = MetricName.build("gauge2");
    private static final MetricName THING = MetricName.build("something");
    private static final MetricName COMPASS = MetricName.build("compass");
    private static final MetricName FAST_COMPASS = MetricName.build("fast.compass");

    private final FakedMetricRegistryListener listener = new FakedMetricRegistryListener();
    private final MetricRegistry registry = new MetricRegistryImpl(10);

    private final Gauge<String> gauge = new FakedMetrics.FakedGauge();
    private final Counter counter = new FakedMetrics.FakedCounter();
    private final Histogram histogram = new FakedMetrics.FakedHistogram();
    private final Meter meter = new FakedMetrics.FakedMeter();
    private final Timer timer = new FakedMetrics.FakedTimer();
    private final Compass compass = new FakedMetrics.FakedCompass();
    private final FastCompass fastCompass = new FakedMetrics.FakedFastCompass();

    @Before
    public void setUp() throws Exception {
        registry.addListener(listener);
    }

    @Test
    public void registeringAGaugeTriggerNotification() throws Exception {
        assertEquals(registry.register(THING, gauge), gauge);

        checkOnGaugeAddedCalledTimes(1, 0);
    }

    @Test
    public void removingAGaugeTriggerNotification() throws Exception {
        registry.register(THING, gauge);

        assertTrue(registry.remove(THING));

        checkOnGaugeRemovedCalledTimes(1, 0);
    }

    @Test
    public void accessingACounterRegistersAndReusesTheCounter() throws Exception {
        final Counter counter1 = registry.counter(THING);
        final Counter counter2 = registry.counter(THING);

        assertTrue(counter1 == counter2);

        checkOnCounterAddedCalledTimes(1, 0);
    }

    @Test
    public void removingACounterTriggerNotification() throws Exception {
        registry.register(THING, counter);

        assertTrue(registry.remove(THING));

        checkOnCounterRemovedCalledTimes(1, 0);
    }

    @Test
    public void registeringAHistogramTriggerNotification() throws Exception {
        assertEquals(registry.register(THING, histogram), histogram);

        checkOnHistogramAddedCalledTimes(1, 0);
    }

    @Test
    public void accessingAHistogramRegistersAndReusesIt() throws Exception {
        final Histogram histogram1 = registry.histogram(THING);
        final Histogram histogram2 = registry.histogram(THING);

        assertEquals(histogram1, histogram2);

        checkOnHistogramAddedCalledTimes(1, 0);
    }

    @Test
    public void removingAHistogramTriggerNotification() throws Exception {
        registry.register(THING, histogram);

        assertTrue(registry.remove(THING));

        checkOnHistogramRemovedCalledTimes(1, 0);
    }

    @Test
    public void registeringAMeterTriggerNotification() throws Exception {
        assertEquals(registry.register(THING, meter), meter);

        checkOnMeterAddedCalledTimes(1, 0);
    }

    @Test
    public void accessingAMeterRegistersAndReusesIt() throws Exception {
        final Meter meter1 = registry.meter(THING);
        final Meter meter2 = registry.meter(THING);

        assertEquals(meter1, meter2);

        checkOnMeterAddedCalledTimes(1, 0);
    }

    @Test
    public void removingAMeterTriggerNotification() throws Exception {
        registry.register(THING, meter);

        assertTrue(registry.remove(THING));

        checkOnMeterRemovedCalledTimes(1, 0);
    }

    @Test
    public void registeringATimerTriggerNotification() throws Exception {
        assertEquals(registry.register(THING, timer), timer);

        checkOnTimerAddedCalledTimes(1, 0);
    }

    @Test
    public void accessingATimerRegistersAndReusesIt() throws Exception {
        final Timer timer1 = registry.timer(THING);
        final Timer timer2 = registry.timer(THING);

        assertEquals(timer1, timer2);

        checkOnTimerAddedCalledTimes(1, 0);
    }

    @Test
    public void removingATimerTriggerNotification() throws Exception {
        registry.register(THING, timer);

        assertTrue(registry.remove(THING));

        checkOnTimerRemovedCalledTimes(1, 0);
    }

    @Test
    public void registeringACompassTriggerNotification() throws Exception {
        assertEquals(registry.register(THING, compass), compass);

        checkOnCompassAddedCalledTimes(1, 0);
    }

    @Test
    public void accessingACompassRegistersAndReusesIt() throws Exception {
        final Compass compass1 = registry.compass(THING);
        final Compass compass2 = registry.compass(THING);

        assertEquals(compass1, compass2);

        checkOnCompassAddedCalledTimes(1, 0);
    }

    @Test
    public void removingACompassTriggerNotification() throws Exception {
        registry.register(THING, compass);

        assertTrue(registry.remove(THING));

        checkOnCompassRemovedCalledTimes(1, 0);
    }

    @Test
    public void registeringAFastCompassTriggerNotification() throws Exception {
        assertEquals(registry.register(THING, fastCompass), fastCompass);

        checkOnFastCompassAddedCalledTimes(1, 0);
    }

    @Test
    public void accessingAFastCompassRegistersAndReusesIt() throws Exception {
        final FastCompass compass1 = registry.fastCompass(THING);
        final FastCompass compass2 = registry.fastCompass(THING);

        assertEquals(compass1, compass2);

        checkOnFastCompassAddedCalledTimes(1, 0);
    }

    @Test
    public void removingAFastCompassTriggerNotification() throws Exception {
        registry.register(THING, fastCompass);

        assertTrue(registry.remove(THING));

        checkOnFastCompassRemovedCalledTimes(1, 0);
    }

    @Test
    public void addingAListenerWithExistingMetricsCatchesItUp() throws Exception {
        registry.register(GAUGE2, gauge);
        registry.register(COUNTER, counter);
        registry.register(HISTOGRAM2, histogram);
        registry.register(METER2, meter);
        registry.register(TIMER2, timer);
        registry.register(COMPASS, compass);
        registry.register(FAST_COMPASS, fastCompass);

        final MetricRegistryListener other = new FakedMetricRegistryListener();
        registry.addListener(other);

        checkOnGaugeAddedCalledTimes(1, 0);
        checkOnCounterAddedCalledTimes(1, 0);
        checkOnHistogramAddedCalledTimes(1, 0);
        checkOnMeterAddedCalledTimes(1, 0);
        checkOnTimerAddedCalledTimes(1, 0);
        checkOnCompassAddedCalledTimes(1, 0);
        checkOnFastCompassAddedCalledTimes(1, 0);
    }

    @Test
    public void aRemovedListenerDoesNotReceiveUpdates() throws Exception {
        registry.register(GAUGE, gauge);
        checkOnGaugeAddedCalledTimes(1, 0);

        registry.removeListener(listener);
        registry.register(GAUGE2, gauge);
        checkOnGaugeAddedCalledTimes(0, 0);
    }

    private void checkOnGaugeAddedCalledTimes(int expect, int resetTo) {
        assertEquals(expect, listener.getOnGaugeAddedCalledTimes());
        listener.setOnGaugeAddedCalledTimes(resetTo);
        assertEquals(resetTo, listener.getOnGaugeAddedCalledTimes());
    }

    private void checkOnGaugeRemovedCalledTimes(int expect, int resetTo) {
        assertEquals(expect, listener.getOnGaugeRemovedCalledTimes());
        listener.setOnGaugeRemovedCalledTimes(resetTo);
        assertEquals(resetTo, listener.getOnGaugeRemovedCalledTimes());
    }

    private void checkOnCounterAddedCalledTimes(int expect, int resetTo) {
        assertEquals(expect, listener.getOnCounterAddedCalledTimes());
        listener.setOnCounterAddedCalledTimes(resetTo);
        assertEquals(resetTo, listener.getOnCounterAddedCalledTimes());
    }

    private void checkOnCounterRemovedCalledTimes(int expect, int resetTo) {
        assertEquals(expect, listener.getOnCounterRemovedCalledTimes());
        listener.setOnCounterRemovedCalledTimes(resetTo);
        assertEquals(resetTo, listener.getOnCounterRemovedCalledTimes());
    }

    private void checkOnHistogramAddedCalledTimes(int expect, int resetTo) {
        assertEquals(expect, listener.getOnHistogramAddedCalledTimes());
        listener.setOnHistogramAddedCalledTimes(resetTo);
        assertEquals(resetTo, listener.getOnHistogramAddedCalledTimes());
    }

    private void checkOnHistogramRemovedCalledTimes(int expect, int resetTo) {
        assertEquals(expect, listener.getOnHistogramRemovedCalledTimes());
        listener.setOnHistogramRemovedCalledTimes(resetTo);
        assertEquals(resetTo, listener.getOnHistogramRemovedCalledTimes());
    }

    private void checkOnMeterAddedCalledTimes(int expect, int resetTo) {
        assertEquals(expect, listener.getOnMeterAddedCalledTimes());
        listener.setOnMeterAddedCalledTimes(resetTo);
        assertEquals(resetTo, listener.getOnMeterAddedCalledTimes());
    }

    private void checkOnMeterRemovedCalledTimes(int expect, int resetTo) {
        assertEquals(expect, listener.getOnMeterRemovedCalledTimes());
        listener.setOnMeterRemovedCalledTimes(resetTo);
        assertEquals(resetTo, listener.getOnMeterRemovedCalledTimes());
    }

    private void checkOnTimerAddedCalledTimes(int expect, int resetTo) {
        assertEquals(expect, listener.getOnTimerAddedCalledTimes());
        listener.setOnTimerAddedCalledTimes(resetTo);
        assertEquals(resetTo, listener.getOnTimerAddedCalledTimes());
    }

    private void checkOnTimerRemovedCalledTimes(int expect, int resetTo) {
        assertEquals(expect, listener.getOnTimerRemovedCalledTimes());
        listener.setOnTimerRemovedCalledTimes(resetTo);
        assertEquals(resetTo, listener.getOnTimerRemovedCalledTimes());
    }

    private void checkOnCompassAddedCalledTimes(int expect, int resetTo) {
        assertEquals(expect, listener.getOnCompassAddedCalledTimes());
        listener.setOnCompassAddedCalledTimes(resetTo);
        assertEquals(resetTo, listener.getOnCompassAddedCalledTimes());
    }

    private void checkOnCompassRemovedCalledTimes(int expect, int resetTo) {
        assertEquals(expect, listener.getOnCompassRemovedCalledTimes());
        listener.setOnCompassRemovedCalledTimes(resetTo);
        assertEquals(resetTo, listener.getOnCompassRemovedCalledTimes());
    }

    private void checkOnFastCompassAddedCalledTimes(int expect, int resetTo) {
        assertEquals(expect, listener.getOnFastCompassAddedCalledTimes());
        listener.setOnFastCompassAddedCalledTimes(resetTo);
        assertEquals(resetTo, listener.getOnFastCompassAddedCalledTimes());
    }

    private void checkOnFastCompassRemovedCalledTimes(int expect, int resetTo) {
        assertEquals(expect, listener.getOnFastCompassRemovedCalledTimes());
        listener.setOnFastCompassRemovedCalledTimes(resetTo);
        assertEquals(resetTo, listener.getOnFastCompassRemovedCalledTimes());
    }

    @Test
    public void concatenatesStringsToFormADottedName() throws Exception {
        assertEquals(name("one", "two", "three"), MetricName.build("one.two.three"));
    }

    @Test
    @SuppressWarnings("NullArgumentToVariableArgMethod")
    public void elidesNullValuesFromNamesWhenOnlyOneNullPassedIn() throws Exception {
        assertEquals(name("one", (String) null), MetricName.build("one"));
    }

    @Test
    public void elidesNullValuesFromNamesWhenManyNullsPassedIn() throws Exception {
        assertEquals(name("one", null, null), MetricName.build("one"));
    }

    @Test
    public void elidesNullValuesFromNamesWhenNullAndNotNullPassedIn() throws Exception {
        assertEquals(name("one", null, "three"), MetricName.build("one.three"));
    }

    @Test
    public void elidesEmptyStringsFromNames() throws Exception {
        assertEquals(name("one", "", "three"), MetricName.build("one.three"));
    }

    @Test
    public void concatenatesClassesWithoutCanonicalNamesWithStrings() throws Exception {
        final Gauge<String> g =
                new Gauge<String>() {
                    @Override
                    public String getValue() {
                        return null;
                    }

                    @Override
                    public long lastUpdateTime() {
                        return 0;
                    }
                };

        assertEquals(
                name(g.getClass(), "one", "two"),
                MetricName.build(g.getClass().getName() + ".one.two"));
    }

    @Test
    public void testMaxMetricCount() {
        MetricRegistry registry = new MetricRegistryImpl(10);
        for (int i = 0; i < 20; i++) {
            registry.counter(MetricName.build("counter-" + i));
        }
        Assert.assertEquals(10, registry.getCounters().keySet().size());

        registry = new MetricRegistryImpl(10);
        for (int i = 0; i < 20; i++) {
            registry.meter(MetricName.build("meter-" + i));
        }
        Assert.assertEquals(10, registry.getMeters().keySet().size());

        registry = new MetricRegistryImpl(10);
        for (int i = 0; i < 20; i++) {
            registry.histogram(MetricName.build("histogram-" + i));
        }
        Assert.assertEquals(10, registry.getHistograms().keySet().size());

        registry = new MetricRegistryImpl(10);
        for (int i = 0; i < 20; i++) {
            registry.timer(MetricName.build("timer-" + i));
        }
        Assert.assertEquals(10, registry.getTimers().keySet().size());

        registry = new MetricRegistryImpl(10);
        for (int i = 0; i < 20; i++) {
            registry.compass(MetricName.build("compass-" + i));
        }
        Assert.assertEquals(10, registry.getCompasses().keySet().size());
    }

    @Test
    public void testIllegalMetricKey() {
        Assert.assertNotNull(registry.counter(MetricName.build("[aaa]")));
    }

    @Test
    public void testIllegalMetricTagValue() {
        registry.counter(MetricName.build("aaa").tagged("bbb", "[ccc]"));
    }

    @Test
    public void testLastUpdatedTime() {
        MetricRegistry registry = new MetricRegistryImpl();
        ManualClock clock = new ManualClock();
        BucketCounter c1 = new BucketCounterImpl(10, 10, clock, false);
        Meter m1 = new MeterImpl(clock, 10);
        Timer t1 = new TimerImpl(ReservoirType.BUCKET, clock, 10);
        Histogram h1 = new HistogramImpl(ReservoirType.BUCKET, 10, 10, clock);
        Compass comp1 = new CompassImpl(ReservoirType.BUCKET, clock, 10, 10, 10, 10);
        FastCompass fc1 = new FastCompassImpl(10, 10, clock, 10);
        registry.register("a", c1);
        registry.register("b", m1);
        registry.register("c", t1);
        registry.register("d", h1);
        registry.register("e", comp1);
        registry.register("f", fc1);
        clock.addSeconds(10);
        c1.update();
        clock.addSeconds(10);
        Assert.assertEquals(10000L, registry.lastUpdateTime());
        m1.mark();
        clock.addSeconds(10);
        Assert.assertEquals(20000L, registry.lastUpdateTime());
        t1.update(1, TimeUnit.SECONDS);
        clock.addSeconds(10);
        Assert.assertEquals(30000L, registry.lastUpdateTime());
        h1.update(1);
        clock.addSeconds(10);
        Assert.assertEquals(40000L, registry.lastUpdateTime());
        comp1.update(1, TimeUnit.SECONDS);
        clock.addSeconds(10);
        Assert.assertEquals(50000L, registry.lastUpdateTime());
        fc1.record(1, "aaa");
        clock.addSeconds(10);
        Assert.assertEquals(60000L, registry.lastUpdateTime());
    }
}
