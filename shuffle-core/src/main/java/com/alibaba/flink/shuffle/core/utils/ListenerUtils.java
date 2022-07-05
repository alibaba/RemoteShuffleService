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

package com.alibaba.flink.shuffle.core.utils;

import com.alibaba.flink.shuffle.core.listener.BacklogListener;
import com.alibaba.flink.shuffle.core.listener.DataCommitListener;
import com.alibaba.flink.shuffle.core.listener.DataListener;
import com.alibaba.flink.shuffle.core.listener.DataRegionCreditListener;
import com.alibaba.flink.shuffle.core.listener.FailureListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/** Utility methods to manipulate listeners. */
public class ListenerUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ListenerUtils.class);

    /**
     * Notifies the available credits to the given {@link DataRegionCreditListener} and logs errors
     * if any exception occurs.
     */
    public static void notifyAvailableCredits(
            int availableCredits,
            int dataRegionIndex,
            @Nullable DataRegionCreditListener dataRegionCreditListener) {
        if (availableCredits <= 0 || dataRegionCreditListener == null) {
            return;
        }

        try {
            dataRegionCreditListener.notifyCredits(availableCredits, dataRegionIndex);
        } catch (Throwable throwable) {
            LOG.error("Fatal: failed to notify available credit to listener.", throwable);
        }
    }

    /**
     * Notifies the target {@link DataListener} of available data and logs errors if any exception
     * occurs.
     */
    public static void notifyAvailableData(@Nullable DataListener listener) {
        if (listener == null) {
            return;
        }

        try {
            listener.notifyDataAvailable();
        } catch (Throwable throwable) {
            LOG.error("Fatal: failed to notify available data to listener.", throwable);
        }
    }

    /**
     * Notifies the target {@link BacklogListener} of available backlog and logs errors if any
     * exception occurs.
     */
    public static void notifyBacklog(@Nullable BacklogListener listener, int backlog) {
        if (listener == null) {
            return;
        }

        try {
            listener.notifyBacklog(backlog);
        } catch (Throwable throwable) {
            LOG.error("Fatal: failed to notify available data to listener.", throwable);
        }
    }

    /**
     * Notifies the target {@link FailureListener} of the encountered failure data and logs errors
     * if any exception occurs.
     */
    public static void notifyFailure(
            @Nullable FailureListener listener, @Nullable Throwable error) {
        if (listener == null) {
            return;
        }

        try {
            listener.notifyFailure(error);
        } catch (Throwable throwable) {
            LOG.error("Fatal: failed to notify failure to listener.", throwable);
        }
    }

    /**
     * Notifies the target {@link DataCommitListener} that all the data has been committed and logs
     * errors if any exception occurs.
     */
    public static void notifyDataCommitted(@Nullable DataCommitListener listener) {
        if (listener == null) {
            return;
        }

        try {
            listener.notifyDataCommitted();
        } catch (Throwable throwable) {
            LOG.error("Fatal: failed to notify data committed event to listener.", throwable);
        }
    }
}
