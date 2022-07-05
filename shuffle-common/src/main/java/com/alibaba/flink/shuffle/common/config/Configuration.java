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

import com.alibaba.flink.shuffle.common.exception.ConfigurationException;
import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.common.utils.TimeUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static com.alibaba.flink.shuffle.common.config.StructuredOptionsSplitter.escapeWithSingleQuote;
import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkArgument;

/** A simple read-only configuration implementation based on {@link Properties}. */
public class Configuration {

    private static final Logger LOG = LoggerFactory.getLogger(Configuration.class);

    public static final String REMOTE_SHUFFLE_CONF_FILENAME = "remote-shuffle-conf.yaml";

    private final Properties configuration;

    public Configuration() {
        this.configuration = new Properties();
    }

    public Configuration(String confDir) throws IOException {
        CommonUtils.checkArgument(confDir != null, "Must be not null.");

        this.configuration = loadConfiguration(confDir);
    }

    public Configuration(Properties configuration) {
        CommonUtils.checkArgument(configuration != null, "Must be not null.");

        this.configuration = new Properties();
        this.configuration.putAll(configuration);
    }

    /** Dynamic configuration has higher priority than that loaded from configuration file. */
    public Configuration(String confDir, Properties dynamicConfiguration) throws IOException {
        CommonUtils.checkArgument(confDir != null, "Must be not null.");
        CommonUtils.checkArgument(dynamicConfiguration != null, "Must be not null.");

        this.configuration = new Properties();
        this.configuration.putAll(loadConfiguration(confDir));
        this.configuration.putAll(dynamicConfiguration);
    }

    public Configuration(Configuration other) {
        CommonUtils.checkArgument(other != null, "Must be not null.");
        CommonUtils.checkArgument(other.toProperties() != null, "Must be not null.");

        this.configuration = new Properties();
        this.configuration.putAll(other.toProperties());
    }

    public void addAll(Configuration other) {
        CommonUtils.checkArgument(other != null, "Must be not null.");
        CommonUtils.checkArgument(other.toProperties() != null, "Must be not null.");

        this.configuration.putAll(other.toProperties());
    }

    private <T> void setValueInternal(String key, T value) {
        CommonUtils.checkArgument(
                key != null && !key.trim().isEmpty(), "key must not be null or empty.");
        configuration.put(key, convertToString(value));
    }

    private <T> void setValueInternal(ConfigOption<T> option, T value) {
        CommonUtils.checkArgument(option != null);

        setValueInternal(option.key(), value);
    }

    public void setByte(ConfigOption<Byte> option, byte value) {
        setValueInternal(option, value);
    }

    public void setShort(ConfigOption<Short> option, short value) {
        setValueInternal(option, value);
    }

    public void setInteger(ConfigOption<Integer> option, int value) {
        setValueInternal(option, value);
    }

    public void setLong(ConfigOption<Long> option, long value) {
        setValueInternal(option, value);
    }

    public void setFloat(ConfigOption<Float> option, float value) {
        setValueInternal(option, value);
    }

    public void setDouble(ConfigOption<Double> option, double value) {
        setValueInternal(option, value);
    }

    public void setBoolean(ConfigOption<Boolean> option, boolean value) {
        setValueInternal(option, value);
    }

    public void setString(ConfigOption<String> option, String value) {
        setValueInternal(option, value);
    }

    public void setString(String key, String value) {
        setValueInternal(key, value);
    }

    public void setDuration(ConfigOption<Duration> option, Duration value) {
        setValueInternal(option, value);
    }

    public void setMemorySize(ConfigOption<MemorySize> option, MemorySize value) {
        setValueInternal(option, value);
    }

    public void setMap(ConfigOption<Map<String, String>> option, Map<String, String> value) {
        setValueInternal(option, value);
    }

    public <T> void setList(ConfigOption<T> option, T value) {
        setValueInternal(option, value);
    }

    public Byte getByte(String key) {
        return getByte(key, null);
    }

    public Byte getByte(ConfigOption<Byte> configOption) {
        CommonUtils.checkArgument(configOption != null, "Must be not null.");

        return getByte(configOption.key(), configOption.defaultValue());
    }

    public Byte getByte(ConfigOption<Byte> configOption, Byte defaultValue) {
        CommonUtils.checkArgument(configOption != null, "Must be not null.");

        return getByte(configOption.key(), defaultValue);
    }

    public Byte getByte(String key, Byte defaultValue) {
        CommonUtils.checkArgument(key != null, "Must be not null.");

        String value = configuration.getProperty(key);
        if (value == null) {
            return defaultValue;
        }

        try {
            return convertToByte(value);
        } catch (Exception exception) {
            throw new ConfigurationException("Illegal config value for " + key + ".");
        }
    }

    public Short getShort(String key) {
        return getShort(key, null);
    }

    public Short getShort(ConfigOption<Short> configOption) {
        CommonUtils.checkArgument(configOption != null, "Must be not null.");

        return getShort(configOption.key(), configOption.defaultValue());
    }

    public Short getShort(ConfigOption<Short> configOption, Short defaultValue) {
        CommonUtils.checkArgument(configOption != null, "Must be not null.");

        return getShort(configOption.key(), defaultValue);
    }

    public Short getShort(String key, Short defaultValue) {
        CommonUtils.checkArgument(key != null, "Must be not null.");

        String value = configuration.getProperty(key);
        if (value == null) {
            return defaultValue;
        }

        try {
            return convertToShort(value);
        } catch (Exception exception) {
            throw new ConfigurationException("Illegal config value for " + key + ".");
        }
    }

    public Integer getInteger(String key) {
        return getInteger(key, null);
    }

    public Integer getInteger(ConfigOption<Integer> configOption) {
        CommonUtils.checkArgument(configOption != null, "Must be not null.");

        return getInteger(configOption.key(), configOption.defaultValue());
    }

    public Integer getInteger(ConfigOption<Integer> configOption, Integer defaultValue) {
        CommonUtils.checkArgument(configOption != null, "Must be not null.");

        return getInteger(configOption.key(), defaultValue);
    }

    public Integer getInteger(String key, Integer defaultValue) {
        CommonUtils.checkArgument(key != null, "Must be not null.");

        String value = configuration.getProperty(key);
        if (value == null) {
            return defaultValue;
        }

        try {
            return convertToInteger(value);
        } catch (Exception exception) {
            throw new ConfigurationException("Illegal config value for " + key + ".");
        }
    }

    public Long getLong(String key) {
        return getLong(key, null);
    }

    public Long getLong(ConfigOption<Long> configOption) {
        CommonUtils.checkArgument(configOption != null, "Must be not null.");

        return getLong(configOption.key(), configOption.defaultValue());
    }

    public Long getLong(ConfigOption<Long> configOption, Long defaultValue) {
        CommonUtils.checkArgument(configOption != null, "Must be not null.");

        return getLong(configOption.key(), defaultValue);
    }

    public Long getLong(String key, Long defaultValue) {
        CommonUtils.checkArgument(key != null, "Must be not null.");

        String value = configuration.getProperty(key);
        if (value == null) {
            return defaultValue;
        }

        try {
            return convertToLong(value);
        } catch (Exception exception) {
            throw new ConfigurationException("Illegal config value for " + key + ".");
        }
    }

    public Double getDouble(String key) {
        return getDouble(key, null);
    }

    public Double getDouble(ConfigOption<Double> configOption) {
        CommonUtils.checkArgument(configOption != null, "Must be not null.");

        return getDouble(configOption.key(), configOption.defaultValue());
    }

    public Double getDouble(ConfigOption<Double> configOption, Double defaultValue) {
        CommonUtils.checkArgument(configOption != null, "Must be not null.");

        return getDouble(configOption.key(), defaultValue);
    }

    public Double getDouble(String key, Double defaultValue) {
        CommonUtils.checkArgument(key != null, "Must be not null.");

        String value = configuration.getProperty(key);
        if (value == null) {
            return defaultValue;
        }

        try {
            return convertToDouble(value);
        } catch (Exception exception) {
            throw new ConfigurationException("Illegal config value for " + key + ".");
        }
    }

    public Float getFloat(String key) {
        return getFloat(key, null);
    }

    public Float getFloat(ConfigOption<Float> configOption) {
        CommonUtils.checkArgument(configOption != null, "Must be not null.");

        return getFloat(configOption.key(), configOption.defaultValue());
    }

    public Float getFloat(ConfigOption<Float> configOption, Float defaultValue) {
        CommonUtils.checkArgument(configOption != null, "Must be not null.");

        return getFloat(configOption.key(), defaultValue);
    }

    public Float getFloat(String key, Float defaultValue) {
        CommonUtils.checkArgument(key != null, "Must be not null.");

        String value = configuration.getProperty(key);
        if (value == null) {
            return defaultValue;
        }

        try {
            return convertToFloat(value);
        } catch (Exception exception) {
            throw new ConfigurationException("Illegal config value for " + key + ".");
        }
    }

    public Boolean getBoolean(String key) {
        return getBoolean(key, null);
    }

    public Boolean getBoolean(ConfigOption<Boolean> configOption) {
        CommonUtils.checkArgument(configOption != null, "Must be not null.");

        return getBoolean(configOption.key(), configOption.defaultValue());
    }

    public Boolean getBoolean(ConfigOption<Boolean> configOption, Boolean defaultValue) {
        CommonUtils.checkArgument(configOption != null, "Must be not null.");

        return getBoolean(configOption.key(), defaultValue);
    }

    public Boolean getBoolean(String key, Boolean defaultValue) {
        CommonUtils.checkArgument(key != null, "Must be not null.");

        String value = configuration.getProperty(key);
        if (value == null) {
            return defaultValue;
        }

        try {
            return convertToBoolean(value);
        } catch (Exception exception) {
            throw new ConfigurationException(
                    "Illegal boolean config value for " + key + ", must be 'true' or 'false'.");
        }
    }

    public String getString(String key) {
        return getString(key, null);
    }

    public String getString(ConfigOption<String> configOption) {
        CommonUtils.checkArgument(configOption != null, "Must be not null.");

        return getString(configOption.key(), configOption.defaultValue());
    }

    public String getString(ConfigOption<String> configOption, String defaultValue) {
        CommonUtils.checkArgument(configOption != null, "Must be not null.");

        return getString(configOption.key(), defaultValue);
    }

    public String getString(String key, String defaultValue) {
        CommonUtils.checkArgument(key != null, "Must be not null.");

        String value = configuration.getProperty(key);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    public MemorySize getMemorySize(ConfigOption<MemorySize> configOption) {
        checkArgument(configOption != null, "Must be not null.");
        return getMemorySize(configOption, configOption.defaultValue());
    }

    public MemorySize getMemorySize(
            ConfigOption<MemorySize> configOption, MemorySize defaultValue) {
        checkArgument(configOption != null, "Must be not null.");
        return getMemorySize(configOption.key(), defaultValue);
    }

    public MemorySize getMemorySize(String key, MemorySize defaultValue) {
        checkArgument(key != null, "Must be not null.");

        String value = configuration.getProperty(key);
        if (value == null) {
            return defaultValue;
        }

        return convertToMemorySize(value);
    }

    public Duration getDuration(ConfigOption<Duration> configOption) {
        checkArgument(configOption != null, "Must be not null.");
        return getDuration(configOption, configOption.defaultValue());
    }

    public Duration getDuration(ConfigOption<Duration> configOption, Duration defaultValue) {
        checkArgument(configOption != null, "Must be not null.");
        return getDuration(configOption.key(), defaultValue);
    }

    public Duration getDuration(String key, Duration defaultValue) {
        checkArgument(key != null, "Must be not null.");

        String value = configuration.getProperty(key);
        if (value == null) {
            return defaultValue;
        }

        return convertToDuration(value);
    }

    public Map<String, String> getMap(ConfigOption<Map<String, String>> configOption) {
        checkArgument(configOption != null, "Must be not null.");
        return getMap(configOption, configOption.defaultValue());
    }

    public Map<String, String> getMap(
            ConfigOption<Map<String, String>> configOption, Map<String, String> defaultValue) {
        checkArgument(configOption != null, "Must be not null.");
        return getMap(configOption.key(), defaultValue);
    }

    public Map<String, String> getMap(String key, Map<String, String> defaultValue) {
        checkArgument(key != null, "Must be not null.");

        String value = configuration.getProperty(key);
        if (value == null) {
            return defaultValue;
        }

        return convertToMap(value);
    }

    @SuppressWarnings("unchecked")
    public <T> T getList(ConfigOption<T> configOption, Class<?> clazz) {
        checkArgument(configOption != null && configOption.key() != null);

        String value = configuration.getProperty(configOption.key());

        if (value == null) {
            return configOption.defaultValue();
        }

        return (T)
                StructuredOptionsSplitter.splitEscaped(value, ';').stream()
                        .map(s -> convertValue(s, clazz))
                        .collect(Collectors.toList());
    }

    private static Byte convertToByte(String value) {
        return Byte.parseByte(value);
    }

    private static Integer convertToInteger(String value) {
        return Integer.parseInt(value);
    }

    private static Long convertToLong(String value) {
        return Long.parseLong(value);
    }

    private static Short convertToShort(String value) {
        return Short.parseShort(value);
    }

    private static Double convertToDouble(String value) {
        return Double.parseDouble(value);
    }

    private static Float convertToFloat(String value) {
        return Float.parseFloat(value);
    }

    private static Boolean convertToBoolean(String value) {
        if (value.equalsIgnoreCase("true")) {
            return Boolean.TRUE;
        } else if (value.equalsIgnoreCase("false")) {
            return Boolean.FALSE;
        } else {
            throw new IllegalArgumentException(
                    "Illegal boolean config value, must be 'true' or 'false'.");
        }
    }

    private static Duration convertToDuration(String value) {
        return TimeUtils.parseDuration(value);
    }

    private static MemorySize convertToMemorySize(String value) {
        return MemorySize.parse(value);
    }

    private static Map<String, String> convertToMap(String value) {
        List<String> listOfRawProperties = StructuredOptionsSplitter.splitEscaped(value, ',');
        return listOfRawProperties.stream()
                .map(s -> StructuredOptionsSplitter.splitEscaped(s, ':'))
                .peek(
                        pair -> {
                            if (pair.size() != 2) {
                                throw new IllegalArgumentException(
                                        "Could not parse pair in the map " + pair);
                            }
                        })
                .collect(Collectors.toMap(a -> a.get(0), a -> a.get(1)));
    }

    /** Get the value of option. Return null if value key not present in the configuration. */
    @SuppressWarnings("unchecked")
    private static <T> T convertValue(String value, Class<?> clazz) {
        if (Byte.class.equals(clazz)) {
            return (T) convertToByte(value);
        } else if (Integer.class.equals(clazz)) {
            return (T) convertToInteger(value);
        } else if (Long.class.equals(clazz)) {
            return (T) convertToLong(value);
        } else if (Short.class.equals(clazz)) {
            return (T) convertToShort(value);
        } else if (Double.class.equals(clazz)) {
            return (T) convertToDouble(value);
        } else if (Float.class.equals(clazz)) {
            return (T) convertToFloat(value);
        } else if (Boolean.class.equals(clazz)) {
            return (T) convertToBoolean(value);
        } else if (String.class.equals(clazz)) {
            return (T) value;
        } else if (clazz == Duration.class) {
            return (T) convertToDuration(value);
        } else if (clazz == MemorySize.class) {
            return (T) convertToMemorySize(value);
        } else if (clazz == Map.class) {
            return (T) convertToMap(value);
        }

        throw new IllegalArgumentException("Unsupported type: " + clazz);
    }

    public Map<String, String> toMap() {
        Map<String, String> result = new HashMap<>();
        for (String propertyName : configuration.stringPropertyNames()) {
            result.put(propertyName, configuration.getProperty(propertyName));
        }
        return result;
    }

    public static Configuration fromMap(Map<String, String> map) {
        Properties properties = new Properties();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            properties.setProperty(entry.getKey(), entry.getValue());
        }
        return new Configuration(properties);
    }

    public Properties toProperties() {
        Properties clonedConfiguration = new Properties();
        clonedConfiguration.putAll(configuration);
        return clonedConfiguration;
    }

    /** Loads configuration from the configuration file. */
    /**
     * Loads a YAML-file of key-value pairs.
     *
     * <p>Colon and whitespace ": " separate key and value (one per line). The hash tag "#" starts a
     * single-line comment.
     *
     * <p>Example:
     *
     * <pre>
     * remote-shuffle.manager.rpc-address: localhost # network address for communication with the shuffle manager
     * remote-shuffle.manager.rpc-port   : 23123     # network port to connect to for communication with the shuffle manager
     * </pre>
     *
     * <p>This does not span the whole YAML specification, but only the *syntax* of simple YAML
     * key-value pairs (see issue #113 on GitHub). If at any point in time, there is a need to go
     * beyond simple key-value pairs syntax compatibility will allow to introduce a YAML parser
     * library.
     *
     * @param confDir the conf dir.
     * @see <a href="http://www.yaml.org/spec/1.2/spec.html">YAML 1.2 specification</a>
     */
    /** Loads configuration from the configuration file. */
    private static Properties loadConfiguration(String confDir) throws IOException {
        File confFile = new File(confDir, REMOTE_SHUFFLE_CONF_FILENAME);
        Properties configuration = new Properties();

        if (!confFile.exists()) {
            LOG.warn(
                    "Configuration file {} does not exist, only dynamic parameters will be used.",
                    confFile.getAbsolutePath());
            return configuration;
        }

        if (!confFile.isFile()) {
            throw new ConfigurationException(
                    String.format(
                            "Configuration file %s is not a normal file.",
                            confFile.getAbsoluteFile()));
        }

        LOG.info("Loading configurations from config file: {}", confFile);
        try (BufferedReader reader =
                new BufferedReader(new InputStreamReader(new FileInputStream(confFile)))) {

            String line;
            int lineNo = 0;
            while ((line = reader.readLine()) != null) {
                lineNo++;
                // 1. check for comments
                String[] comments = line.split("#", 2);
                String conf = comments[0].trim();

                // 2. get key and value
                if (conf.length() > 0) {
                    String[] kv = conf.split(": ", 2);

                    // skip line with no valid key-value pair
                    if (kv.length == 1) {
                        LOG.warn(
                                "Error while trying to split key and value in configuration file "
                                        + confFile
                                        + ":"
                                        + lineNo
                                        + ": \""
                                        + line
                                        + "\"");
                        continue;
                    }

                    String key = kv[0].trim();
                    String value = kv[1].trim();

                    // sanity check
                    if (key.length() == 0 || value.length() == 0) {
                        LOG.warn(
                                "Error after splitting key and value in configuration file "
                                        + confFile
                                        + ":"
                                        + lineNo
                                        + ": \""
                                        + line
                                        + "\"");
                        continue;
                    }

                    LOG.info("Loading configuration property: {}, {}", key, value);
                    configuration.setProperty(key, value);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Error parsing YAML configuration.", e);
        }
        return configuration;
    }

    private static String convertToString(Object o) {
        if (o.getClass() == String.class) {
            return (String) o;
        } else if (o.getClass() == Duration.class) {
            Duration duration = (Duration) o;
            return String.format("%d ns", duration.toNanos());
        } else if (o instanceof List) {
            return ((List<?>) o)
                    .stream()
                            .map(e -> escapeWithSingleQuote(convertToString(e), ";"))
                            .collect(Collectors.joining(";"));
        } else if (o instanceof Map) {
            return ((Map<?, ?>) o)
                    .entrySet().stream()
                            .map(
                                    e -> {
                                        String escapedKey =
                                                escapeWithSingleQuote(e.getKey().toString(), ":");
                                        String escapedValue =
                                                escapeWithSingleQuote(e.getValue().toString(), ":");

                                        return escapeWithSingleQuote(
                                                escapedKey + ":" + escapedValue, ",");
                                    })
                            .collect(Collectors.joining(","));
        }

        return o.toString();
    }
}
