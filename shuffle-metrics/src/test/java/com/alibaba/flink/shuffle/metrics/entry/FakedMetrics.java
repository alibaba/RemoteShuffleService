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
import com.alibaba.metrics.Clock;
import com.alibaba.metrics.Compass;
import com.alibaba.metrics.Counter;
import com.alibaba.metrics.FastCompass;
import com.alibaba.metrics.Gauge;
import com.alibaba.metrics.Histogram;
import com.alibaba.metrics.Meter;
import com.alibaba.metrics.Reservoir;
import com.alibaba.metrics.Snapshot;
import com.alibaba.metrics.Timer;

import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/** Faked implementation for different metric types. */
public class FakedMetrics {
    /** A faked implementation for {@link Gauge}. */
    public static class FakedGauge implements Gauge {
        @Override
        public Object getValue() {
            return null;
        }

        @Override
        public long lastUpdateTime() {
            return 0;
        }
    }

    /** A faked implementation for {@link Gauge}. */
    public static class FakedStringGauge implements Gauge<String> {
        @Override
        public String getValue() {
            return "abcde";
        }

        @Override
        public long lastUpdateTime() {
            return 0;
        }
    }

    /** A faked implementation for {@link Gauge}. */
    public static class FakedFloatGauge implements Gauge<Float> {
        @Override
        public Float getValue() {
            return 1.0f;
        }

        @Override
        public long lastUpdateTime() {
            return 0;
        }
    }

    /** A faked implementation for {@link Counter}. */
    public static class FakedCounter implements Counter {
        @Override
        public void inc() {}

        @Override
        public void inc(long n) {}

        @Override
        public void dec() {}

        @Override
        public void dec(long n) {}

        @Override
        public long getCount() {
            return 0;
        }

        @Override
        public long lastUpdateTime() {
            return 0;
        }
    }

    /** A faked implementation for {@link Histogram}. */
    public static class FakedHistogram implements Histogram {

        @Override
        public void update(int value) {}

        @Override
        public void update(long value) {}

        @Override
        public long getCount() {
            return 0;
        }

        @Override
        public long lastUpdateTime() {
            return 0;
        }

        @Override
        public Snapshot getSnapshot() {
            return new FakedSnapshot();
        }
    }

    /** A faked implementation for {@link Meter}. */
    public static class FakedMeter implements Meter {
        @Override
        public void mark() {}

        @Override
        public void mark(long n) {}

        @Override
        public long getCount() {
            return 0;
        }

        @Override
        public Map<Long, Long> getInstantCount() {
            return null;
        }

        @Override
        public Map<Long, Long> getInstantCount(long startTime) {
            return null;
        }

        @Override
        public int getInstantCountInterval() {
            return 0;
        }

        @Override
        public double getFifteenMinuteRate() {
            return 0;
        }

        @Override
        public double getFiveMinuteRate() {
            return 0;
        }

        @Override
        public double getMeanRate() {
            return 0;
        }

        @Override
        public double getOneMinuteRate() {
            return 0;
        }

        @Override
        public long lastUpdateTime() {
            return 0;
        }
    }

    /** A faked implementation for {@link Timer}. */
    public static class FakedTimer implements Timer {
        @Override
        public void update(long duration, TimeUnit unit) {}

        @Override
        public <T> T time(Callable<T> event) throws Exception {
            return null;
        }

        @Override
        public Context time() {
            return null;
        }

        @Override
        public long getCount() {
            return 0;
        }

        @Override
        public Map<Long, Long> getInstantCount() {
            return null;
        }

        @Override
        public Map<Long, Long> getInstantCount(long startTime) {
            return null;
        }

        @Override
        public int getInstantCountInterval() {
            return 0;
        }

        @Override
        public double getFifteenMinuteRate() {
            return 0;
        }

        @Override
        public double getFiveMinuteRate() {
            return 0;
        }

        @Override
        public double getMeanRate() {
            return 0;
        }

        @Override
        public double getOneMinuteRate() {
            return 0;
        }

        @Override
        public long lastUpdateTime() {
            return 0;
        }

        @Override
        public Snapshot getSnapshot() {
            return null;
        }
    }

    /** A faked implementation for {@link Compass}. */
    public static class FakedCompass implements Compass {
        @Override
        public void update(long duration, TimeUnit unit) {}

        @Override
        public void update(
                long duration, TimeUnit unit, boolean isSuccess, String errorCode, String addon) {}

        @Override
        public <T> T time(Callable<T> event) throws Exception {
            return null;
        }

        @Override
        public Context time() {
            return null;
        }

        @Override
        public Map<String, BucketCounter> getErrorCodeCounts() {
            return null;
        }

        @Override
        public double getSuccessRate() {
            return 0;
        }

        @Override
        public long getSuccessCount() {
            return 0;
        }

        @Override
        public BucketCounter getBucketSuccessCount() {
            return null;
        }

        @Override
        public Map<String, BucketCounter> getAddonCounts() {
            return null;
        }

        @Override
        public long getCount() {
            return 0;
        }

        @Override
        public Map<Long, Long> getInstantCount() {
            return null;
        }

        @Override
        public Map<Long, Long> getInstantCount(long startTime) {
            return null;
        }

        @Override
        public int getInstantCountInterval() {
            return 0;
        }

        @Override
        public double getFifteenMinuteRate() {
            return 0;
        }

        @Override
        public double getFiveMinuteRate() {
            return 0;
        }

        @Override
        public double getMeanRate() {
            return 0;
        }

        @Override
        public double getOneMinuteRate() {
            return 0;
        }

        @Override
        public long lastUpdateTime() {
            return 0;
        }

        @Override
        public Snapshot getSnapshot() {
            return null;
        }
    }

    /** A faked implementation for {@link FastCompass}. */
    public static class FakedFastCompass implements FastCompass {
        @Override
        public void record(long duration, String subCategory) {}

        @Override
        public Map<String, Map<Long, Long>> getMethodCountPerCategory() {
            return null;
        }

        @Override
        public Map<String, Map<Long, Long>> getMethodCountPerCategory(long startTime) {
            return null;
        }

        @Override
        public Map<String, Map<Long, Long>> getMethodRtPerCategory() {
            return null;
        }

        @Override
        public Map<String, Map<Long, Long>> getMethodRtPerCategory(long startTime) {
            return null;
        }

        @Override
        public Map<String, Map<Long, Long>> getCountAndRtPerCategory() {
            return null;
        }

        @Override
        public Map<String, Map<Long, Long>> getCountAndRtPerCategory(long startTime) {
            return null;
        }

        @Override
        public int getBucketInterval() {
            return 0;
        }

        @Override
        public long lastUpdateTime() {
            return 0;
        }
    }

    /** A faked implementation for {@link Reservoir}. */
    public static class FakedReservoir implements Reservoir {
        private Snapshot snapshot;
        int updateCount = 0;

        @Override
        public int size() {
            return 0;
        }

        @Override
        public void update(long value) {
            updateCount++;
        }

        @Override
        public Snapshot getSnapshot() {
            return snapshot;
        }

        public void setSnapshot(Snapshot snapshot) {
            this.snapshot = snapshot;
        }

        public void setUpdateCount(int updateCount) {
            this.updateCount = updateCount;
        }

        public int getUpdateCount() {
            return updateCount;
        }
    }

    /** A faked implementation for {@link Clock}. */
    public static class FakedClock extends Clock {
        long curTime = 0;

        @Override
        public long getTick() {
            return curTime;
        }

        public void setCurTime(long curTime) {
            this.curTime = curTime;
        }
    }

    /** A faked implementation for {@link Snapshot}. */
    public static class FakedSnapshot implements Snapshot {
        @Override
        public double getValue(double quantile) {
            return 0;
        }

        @Override
        public long[] getValues() {
            return new long[0];
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public double getMedian() {
            return 0;
        }

        @Override
        public double get75thPercentile() {
            return 0;
        }

        @Override
        public double get95thPercentile() {
            return 0;
        }

        @Override
        public double get98thPercentile() {
            return 0;
        }

        @Override
        public double get99thPercentile() {
            return 0;
        }

        @Override
        public double get999thPercentile() {
            return 0;
        }

        @Override
        public long getMax() {
            return 0;
        }

        @Override
        public double getMean() {
            return 0;
        }

        @Override
        public long getMin() {
            return 0;
        }

        @Override
        public double getStdDev() {
            return 0;
        }

        @Override
        public void dump(OutputStream output) {}
    }
}
