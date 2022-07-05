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

package com.alibaba.flink.shuffle.transfer;

/** Listener to be notified when there is any available credit. */
public abstract class CreditListener {

    private int numCreditsNeeded;

    private boolean isRegistered;

    void increaseNumCreditsNeeded(int numCredits) {
        numCreditsNeeded += numCredits;
    }

    void decreaseNumCreditsNeeded(int numCredits) {
        numCreditsNeeded -= numCredits;
    }

    int getNumCreditsNeeded() {
        return numCreditsNeeded;
    }

    boolean isRegistered() {
        return isRegistered;
    }

    void setRegistered(boolean registered) {
        isRegistered = registered;
    }

    public abstract void notifyAvailableCredits(int numCredits);
}
