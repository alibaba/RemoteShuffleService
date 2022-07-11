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

package com.alibaba.flink.shuffle.core.listener;

import com.alibaba.flink.shuffle.core.memory.Buffer;

/**
 * Listener to be notified when any credit ({@link Buffer}) is available for the target data region.
 */
public interface DataRegionCreditListener {

    /** Notifies the available credits of the corresponding data region to the listener. */
    void notifyCredits(int availableCredits, int dataRegionIndex);
}
