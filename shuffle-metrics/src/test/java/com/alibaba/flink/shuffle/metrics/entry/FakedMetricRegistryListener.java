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

import com.alibaba.metrics.Compass;
import com.alibaba.metrics.Counter;
import com.alibaba.metrics.FastCompass;
import com.alibaba.metrics.Gauge;
import com.alibaba.metrics.Histogram;
import com.alibaba.metrics.Meter;
import com.alibaba.metrics.MetricName;
import com.alibaba.metrics.MetricRegistryListener;
import com.alibaba.metrics.Timer;

/** Faked implementation for {@link MetricRegistryListener}. */
public class FakedMetricRegistryListener implements MetricRegistryListener {
    private int onGaugeAddedCalledTimes = 0;
    private int onGaugeRemovedCalledTimes = 0;
    private int onCounterAddedCalledTimes = 0;
    private int onCounterRemovedCalledTimes = 0;
    private int onHistogramAddedCalledTimes = 0;
    private int onHistogramRemovedCalledTimes = 0;
    private int onMeterAddedCalledTimes = 0;
    private int onMeterRemovedCalledTimes = 0;
    private int onTimerAddedCalledTimes = 0;
    private int onTimerRemovedCalledTimes = 0;
    private int onCompassAddedCalledTimes = 0;
    private int onCompassRemovedCalledTimes = 0;
    private int onFastCompassAddedCalledTimes = 0;
    private int onFastCompassRemovedCalledTimes = 0;

    @Override
    public void onGaugeAdded(MetricName name, Gauge<?> gauge) {
        onGaugeAddedCalledTimes++;
    }

    @Override
    public void onGaugeRemoved(MetricName name) {
        onGaugeRemovedCalledTimes++;
    }

    @Override
    public void onCounterAdded(MetricName name, Counter counter) {
        onCounterAddedCalledTimes++;
    }

    @Override
    public void onCounterRemoved(MetricName name) {
        onCounterRemovedCalledTimes++;
    }

    @Override
    public void onHistogramAdded(MetricName name, Histogram histogram) {
        onHistogramAddedCalledTimes++;
    }

    @Override
    public void onHistogramRemoved(MetricName name) {
        onHistogramRemovedCalledTimes++;
    }

    @Override
    public void onMeterAdded(MetricName name, Meter meter) {
        onMeterAddedCalledTimes++;
    }

    @Override
    public void onMeterRemoved(MetricName name) {
        onMeterRemovedCalledTimes++;
    }

    @Override
    public void onTimerAdded(MetricName name, Timer timer) {
        onTimerAddedCalledTimes++;
    }

    @Override
    public void onTimerRemoved(MetricName name) {
        onTimerRemovedCalledTimes++;
    }

    @Override
    public void onCompassAdded(MetricName name, Compass compass) {
        onCompassAddedCalledTimes++;
    }

    @Override
    public void onCompassRemoved(MetricName name) {
        onCompassRemovedCalledTimes++;
    }

    @Override
    public void onFastCompassAdded(MetricName name, FastCompass compass) {
        onFastCompassAddedCalledTimes++;
    }

    @Override
    public void onFastCompassRemoved(MetricName name) {
        onFastCompassRemovedCalledTimes++;
    }

    public int getOnGaugeAddedCalledTimes() {
        return onGaugeAddedCalledTimes;
    }

    public void setOnGaugeAddedCalledTimes(int onGaugeAddedCalledTimes) {
        this.onGaugeAddedCalledTimes = onGaugeAddedCalledTimes;
    }

    public int getOnCounterAddedCalledTimes() {
        return onCounterAddedCalledTimes;
    }

    public void setOnCounterAddedCalledTimes(int onCounterAddedCalledTimes) {
        this.onCounterAddedCalledTimes = onCounterAddedCalledTimes;
    }

    public int getOnGaugeRemovedCalledTimes() {
        return onGaugeRemovedCalledTimes;
    }

    public void setOnGaugeRemovedCalledTimes(int onGaugeRemovedCalledTimes) {
        this.onGaugeRemovedCalledTimes = onGaugeRemovedCalledTimes;
    }

    public int getOnCounterRemovedCalledTimes() {
        return onCounterRemovedCalledTimes;
    }

    public void setOnCounterRemovedCalledTimes(int onCounterRemovedCalledTimes) {
        this.onCounterRemovedCalledTimes = onCounterRemovedCalledTimes;
    }

    public int getOnHistogramAddedCalledTimes() {
        return onHistogramAddedCalledTimes;
    }

    public void setOnHistogramAddedCalledTimes(int onHistogramAddedCalledTimes) {
        this.onHistogramAddedCalledTimes = onHistogramAddedCalledTimes;
    }

    public int getOnHistogramRemovedCalledTimes() {
        return onHistogramRemovedCalledTimes;
    }

    public void setOnHistogramRemovedCalledTimes(int onHistogramRemovedCalledTimes) {
        this.onHistogramRemovedCalledTimes = onHistogramRemovedCalledTimes;
    }

    public int getOnMeterAddedCalledTimes() {
        return onMeterAddedCalledTimes;
    }

    public void setOnMeterAddedCalledTimes(int onMeterAddedCalledTimes) {
        this.onMeterAddedCalledTimes = onMeterAddedCalledTimes;
    }

    public int getOnMeterRemovedCalledTimes() {
        return onMeterRemovedCalledTimes;
    }

    public void setOnMeterRemovedCalledTimes(int onMeterRemovedCalledTimes) {
        this.onMeterRemovedCalledTimes = onMeterRemovedCalledTimes;
    }

    public int getOnTimerAddedCalledTimes() {
        return onTimerAddedCalledTimes;
    }

    public void setOnTimerAddedCalledTimes(int onTimerAddedCalledTimes) {
        this.onTimerAddedCalledTimes = onTimerAddedCalledTimes;
    }

    public int getOnTimerRemovedCalledTimes() {
        return onTimerRemovedCalledTimes;
    }

    public void setOnTimerRemovedCalledTimes(int onTimerRemovedCalledTimes) {
        this.onTimerRemovedCalledTimes = onTimerRemovedCalledTimes;
    }

    public int getOnCompassAddedCalledTimes() {
        return onCompassAddedCalledTimes;
    }

    public void setOnCompassAddedCalledTimes(int onCompassAddedCalledTimes) {
        this.onCompassAddedCalledTimes = onCompassAddedCalledTimes;
    }

    public int getOnCompassRemovedCalledTimes() {
        return onCompassRemovedCalledTimes;
    }

    public void setOnCompassRemovedCalledTimes(int onCompassRemovedCalledTimes) {
        this.onCompassRemovedCalledTimes = onCompassRemovedCalledTimes;
    }

    public int getOnFastCompassAddedCalledTimes() {
        return onFastCompassAddedCalledTimes;
    }

    public void setOnFastCompassAddedCalledTimes(int onFastCompassAddedCalledTimes) {
        this.onFastCompassAddedCalledTimes = onFastCompassAddedCalledTimes;
    }

    public int getOnFastCompassRemovedCalledTimes() {
        return onFastCompassRemovedCalledTimes;
    }

    public void setOnFastCompassRemovedCalledTimes(int onFastCompassRemovedCalledTimes) {
        this.onFastCompassRemovedCalledTimes = onFastCompassRemovedCalledTimes;
    }
}
