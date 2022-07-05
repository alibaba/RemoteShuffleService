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

package com.alibaba.flink.shuffle.common.utils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Tests for utilities in {@link CommonUtils}. */
public class ExceptionUtilsTest {

    @Test
    public void testSummaryErrorMessageStack() {
        Throwable t = new Throwable("msg");
        assertEquals("[java.lang.Throwable: msg]", ExceptionUtils.summaryErrorMessageStack(t));

        t = new Throwable();
        assertEquals("[java.lang.Throwable: null]", ExceptionUtils.summaryErrorMessageStack(t));

        t = new Throwable("msga", new Throwable0("msgb"));
        assertEquals(
                "[java.lang.Throwable: msga] -> [com.alibaba.flink.shuffle.common.utils.ExceptionUtilsTest$Throwable0: msgb]",
                ExceptionUtils.summaryErrorMessageStack(t));

        t = new Throwable0("msga", new Throwable());
        assertEquals(
                "[com.alibaba.flink.shuffle.common.utils.ExceptionUtilsTest$Throwable0: msga] -> [java.lang.Throwable: null]",
                ExceptionUtils.summaryErrorMessageStack(t));

        t = new Throwable0("msga", new Throwable(null, new Throwable0("msgb")));
        assertEquals(
                "[com.alibaba.flink.shuffle.common.utils.ExceptionUtilsTest$Throwable0: msga] -> [java.lang.Throwable: null] -> [com.alibaba.flink.shuffle.common.utils.ExceptionUtilsTest$Throwable0: msgb]",
                ExceptionUtils.summaryErrorMessageStack(t));
    }

    private class Throwable0 extends Throwable {
        Throwable0(String msg) {
            super(msg);
        }

        Throwable0(String msg, Throwable t) {
            super(msg, t);
        }
    }
}
