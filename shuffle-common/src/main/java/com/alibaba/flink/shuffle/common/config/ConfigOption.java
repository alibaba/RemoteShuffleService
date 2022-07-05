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

package com.alibaba.flink.shuffle.common.config;

import com.alibaba.flink.shuffle.common.utils.CommonUtils;

import javax.annotation.Nullable;

/** Utility class to define config options. */
public class ConfigOption<T> {

    private final String key;

    @Nullable private T defaultValue;

    private String description;

    public ConfigOption(String key) {
        CommonUtils.checkArgument(key != null, "Must be not null.");
        this.key = key;
    }

    public String key() {
        return key;
    }

    public T defaultValue() {
        return defaultValue;
    }

    public ConfigOption<T> defaultValue(@Nullable T defaultValue) {
        this.defaultValue = defaultValue;
        return this;
    }

    public String description() {
        return description;
    }

    public ConfigOption<T> description(String description) {
        CommonUtils.checkArgument(description != null, "Must be not null.");

        this.description = description;
        return this;
    }

    @Override
    public String toString() {
        return key;
    }
}
