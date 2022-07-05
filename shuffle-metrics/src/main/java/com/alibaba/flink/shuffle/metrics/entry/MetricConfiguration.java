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

import com.alibaba.flink.shuffle.common.config.Configuration;

import java.util.Properties;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;

/**
 * This class is used to transform configurations, because the configurations can't be used directly
 * in the dependency metrics project.
 */
public class MetricConfiguration {

    private final Configuration conf;

    private final Properties properties;

    public MetricConfiguration(Configuration configuration) {
        this.conf = configuration;
        this.properties = parseMetricProperties(configuration);
    }

    /** Transform configurations into new formats used in the dependency metrics project. */
    private Properties parseMetricProperties(Configuration configuration) {
        checkNotNull(configuration);
        Properties properties = new Properties();
        properties.putAll(configuration.toProperties());
        return properties;
    }

    public Properties getProperties() {
        return properties;
    }

    public Configuration getConfiguration() {
        return conf;
    }
}
