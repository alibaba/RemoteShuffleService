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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileWriter;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/** Tests for {@link Configuration}. */
public class ConfigurationTest {

    private static final String KEY_1 = "remote-shuffle.test.key1";

    private static final String KEY_2 = "remote-shuffle.test.key2";

    private static final String KEY_3 = "remote-shuffle.test.key3";

    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testGetBoolean() {
        ConfigOption<Boolean> option1 = new ConfigOption<Boolean>(KEY_1).defaultValue(false);
        ConfigOption<Boolean> option2 = new ConfigOption<Boolean>(KEY_2).defaultValue(false);
        ConfigOption<Boolean> option3 = new ConfigOption<Boolean>(KEY_3).defaultValue(false);

        Properties properties = new Properties();
        properties.put(KEY_1, "true");
        properties.put(KEY_2, "illegal");

        Configuration configuration = new Configuration(properties);

        assertEquals(true, configuration.getBoolean(KEY_1));
        assertEquals(true, configuration.getBoolean(KEY_1, false));
        assertEquals(true, configuration.getBoolean(option1));
        assertEquals(true, configuration.getBoolean(option1, false));

        checkGetIllegalValue(() -> configuration.getBoolean(KEY_2));
        checkGetIllegalValue(() -> configuration.getBoolean(KEY_2, false));
        checkGetIllegalValue(() -> configuration.getBoolean(option2));
        checkGetIllegalValue(() -> configuration.getBoolean(option2, false));

        assertNull(configuration.getBoolean(KEY_3));
        assertEquals(false, configuration.getBoolean(KEY_3, false));
        assertEquals(true, configuration.getBoolean(KEY_3, true));
        assertEquals(option3.defaultValue(), configuration.getBoolean(option3));
        assertEquals(false, configuration.getBoolean(option3, false));
        assertEquals(true, configuration.getBoolean(option3, true));
    }

    @Test
    public void testGetByte() {
        Byte defaultValue = 'X';
        ConfigOption<Byte> option1 = new ConfigOption<Byte>(KEY_1).defaultValue(defaultValue);
        ConfigOption<Byte> option2 = new ConfigOption<Byte>(KEY_2).defaultValue(defaultValue);
        ConfigOption<Byte> option3 = new ConfigOption<Byte>(KEY_3).defaultValue(defaultValue);

        Byte value1 = 'O';
        Properties properties = new Properties();
        properties.put(KEY_1, value1.toString());
        properties.put(KEY_2, "illegal");

        Configuration configuration = new Configuration(properties);

        assertEquals(value1, configuration.getByte(KEY_1));
        assertEquals(value1, configuration.getByte(KEY_1, defaultValue));
        assertEquals(value1, configuration.getByte(option1));
        assertEquals(value1, configuration.getByte(option1, defaultValue));

        checkGetIllegalValue(() -> configuration.getByte(KEY_2));
        checkGetIllegalValue(() -> configuration.getByte(KEY_2, defaultValue));
        checkGetIllegalValue(() -> configuration.getByte(option2));
        checkGetIllegalValue(() -> configuration.getByte(option2, defaultValue));

        assertNull(configuration.getByte(KEY_3));
        assertEquals(defaultValue, configuration.getByte(KEY_3, defaultValue));
        assertEquals(option3.defaultValue(), configuration.getByte(option3));
        assertEquals(value1, configuration.getByte(option3, value1));
    }

    @Test
    public void testGetShort() {
        Short defaultValue = 1;
        ConfigOption<Short> option1 = new ConfigOption<Short>(KEY_1).defaultValue(defaultValue);
        ConfigOption<Short> option2 = new ConfigOption<Short>(KEY_2).defaultValue(defaultValue);
        ConfigOption<Short> option3 = new ConfigOption<Short>(KEY_3).defaultValue(defaultValue);

        Short value1 = 1024;
        Properties properties = new Properties();
        properties.put(KEY_1, value1.toString());
        properties.put(KEY_2, "illegal");

        Configuration configuration = new Configuration(properties);

        assertEquals(value1, configuration.getShort(KEY_1));
        assertEquals(value1, configuration.getShort(KEY_1, defaultValue));
        assertEquals(value1, configuration.getShort(option1));
        assertEquals(value1, configuration.getShort(option1, defaultValue));

        checkGetIllegalValue(() -> configuration.getShort(KEY_2));
        checkGetIllegalValue(() -> configuration.getShort(KEY_2, defaultValue));
        checkGetIllegalValue(() -> configuration.getShort(option2));
        checkGetIllegalValue(() -> configuration.getShort(option2, defaultValue));

        assertNull(configuration.getShort(KEY_3));
        assertEquals(defaultValue, configuration.getShort(KEY_3, defaultValue));
        assertEquals(option3.defaultValue(), configuration.getShort(option3));
        assertEquals(value1, configuration.getShort(option3, value1));
    }

    @Test
    public void testGetInteger() {
        Integer defaultValue = 1;
        ConfigOption<Integer> option1 = new ConfigOption<Integer>(KEY_1).defaultValue(defaultValue);
        ConfigOption<Integer> option2 = new ConfigOption<Integer>(KEY_2).defaultValue(defaultValue);
        ConfigOption<Integer> option3 = new ConfigOption<Integer>(KEY_3).defaultValue(defaultValue);

        Integer value1 = 1024;
        Properties properties = new Properties();
        properties.put(KEY_1, value1.toString());
        properties.put(KEY_2, "illegal");

        Configuration configuration = new Configuration(properties);

        assertEquals(value1, configuration.getInteger(KEY_1));
        assertEquals(value1, configuration.getInteger(KEY_1, defaultValue));
        assertEquals(value1, configuration.getInteger(option1));
        assertEquals(value1, configuration.getInteger(option1, defaultValue));

        checkGetIllegalValue(() -> configuration.getInteger(KEY_2));
        checkGetIllegalValue(() -> configuration.getInteger(KEY_2, defaultValue));
        checkGetIllegalValue(() -> configuration.getInteger(option2));
        checkGetIllegalValue(() -> configuration.getInteger(option2, defaultValue));

        assertNull(configuration.getInteger(KEY_3));
        assertEquals(defaultValue, configuration.getInteger(KEY_3, defaultValue));
        assertEquals(option3.defaultValue(), configuration.getInteger(option3));
        assertEquals(value1, configuration.getInteger(option3, value1));
    }

    @Test
    public void testGetLong() {
        Long defaultValue = 1L;
        ConfigOption<Long> option1 = new ConfigOption<Long>(KEY_1).defaultValue(defaultValue);
        ConfigOption<Long> option2 = new ConfigOption<Long>(KEY_2).defaultValue(defaultValue);
        ConfigOption<Long> option3 = new ConfigOption<Long>(KEY_3).defaultValue(defaultValue);

        Long value1 = 1024L;
        Properties properties = new Properties();
        properties.put(KEY_1, value1.toString());
        properties.put(KEY_2, "illegal");

        Configuration configuration = new Configuration(properties);

        assertEquals(value1, configuration.getLong(KEY_1));
        assertEquals(value1, configuration.getLong(KEY_1, defaultValue));
        assertEquals(value1, configuration.getLong(option1));
        assertEquals(value1, configuration.getLong(option1, defaultValue));

        checkGetIllegalValue(() -> configuration.getLong(KEY_2));
        checkGetIllegalValue(() -> configuration.getLong(KEY_2, defaultValue));
        checkGetIllegalValue(() -> configuration.getLong(option2));
        checkGetIllegalValue(() -> configuration.getLong(option2, defaultValue));

        assertNull(configuration.getLong(KEY_3));
        assertEquals(defaultValue, configuration.getLong(KEY_3, defaultValue));
        assertEquals(option3.defaultValue(), configuration.getLong(option3));
        assertEquals(value1, configuration.getLong(option3, value1));
    }

    @Test
    public void testGetFloat() {
        Float defaultValue = 1.0F;
        ConfigOption<Float> option1 = new ConfigOption<Float>(KEY_1).defaultValue(defaultValue);
        ConfigOption<Float> option2 = new ConfigOption<Float>(KEY_2).defaultValue(defaultValue);
        ConfigOption<Float> option3 = new ConfigOption<Float>(KEY_3).defaultValue(defaultValue);

        Float value1 = 1024.0F;
        Properties properties = new Properties();
        properties.put(KEY_1, value1.toString());
        properties.put(KEY_2, "illegal");

        Configuration configuration = new Configuration(properties);

        assertEquals(value1, configuration.getFloat(KEY_1));
        assertEquals(value1, configuration.getFloat(KEY_1, defaultValue));
        assertEquals(value1, configuration.getFloat(option1));
        assertEquals(value1, configuration.getFloat(option1, defaultValue));

        checkGetIllegalValue(() -> configuration.getFloat(KEY_2));
        checkGetIllegalValue(() -> configuration.getFloat(KEY_2, defaultValue));
        checkGetIllegalValue(() -> configuration.getFloat(option2));
        checkGetIllegalValue(() -> configuration.getFloat(option2, defaultValue));

        assertNull(configuration.getFloat(KEY_3));
        assertEquals(defaultValue, configuration.getFloat(KEY_3, defaultValue));
        assertEquals(option3.defaultValue(), configuration.getFloat(option3));
        assertEquals(value1, configuration.getFloat(option3, value1));
    }

    @Test
    public void testGetDouble() {
        Double defaultValue = 1.0;
        ConfigOption<Double> option1 = new ConfigOption<Double>(KEY_1).defaultValue(defaultValue);
        ConfigOption<Double> option2 = new ConfigOption<Double>(KEY_2).defaultValue(defaultValue);
        ConfigOption<Double> option3 = new ConfigOption<Double>(KEY_3).defaultValue(defaultValue);

        Double value1 = 1024.0;
        Properties properties = new Properties();
        properties.put(KEY_1, value1.toString());
        properties.put(KEY_2, "illegal");

        Configuration configuration = new Configuration(properties);

        assertEquals(value1, configuration.getDouble(KEY_1));
        assertEquals(value1, configuration.getDouble(KEY_1, defaultValue));
        assertEquals(value1, configuration.getDouble(option1));
        assertEquals(value1, configuration.getDouble(option1, defaultValue));

        checkGetIllegalValue(() -> configuration.getDouble(KEY_2));
        checkGetIllegalValue(() -> configuration.getDouble(KEY_2, defaultValue));
        checkGetIllegalValue(() -> configuration.getDouble(option2));
        checkGetIllegalValue(() -> configuration.getDouble(option2, defaultValue));

        assertNull(configuration.getDouble(KEY_3));
        assertEquals(defaultValue, configuration.getDouble(KEY_3, defaultValue));
        assertEquals(option3.defaultValue(), configuration.getDouble(option3));
        assertEquals(value1, configuration.getDouble(option3, value1));
    }

    @Test
    public void testGetString() {
        String defaultValue = "hello";
        ConfigOption<String> option1 = new ConfigOption<String>(KEY_1).defaultValue(defaultValue);
        ConfigOption<String> option2 = new ConfigOption<String>(KEY_2).defaultValue(defaultValue);

        String value1 = "world";
        Properties properties = new Properties();
        properties.put(KEY_1, value1);

        Configuration configuration = new Configuration(properties);

        assertEquals(value1, configuration.getString(KEY_1));
        assertEquals(value1, configuration.getString(KEY_1, defaultValue));
        assertEquals(value1, configuration.getString(option1));
        assertEquals(value1, configuration.getString(option1, defaultValue));

        assertNull(configuration.getString(KEY_3));
        assertEquals(defaultValue, configuration.getString(KEY_3, defaultValue));
        assertEquals(option2.defaultValue(), configuration.getString(option2));
        assertEquals(value1, configuration.getString(option2, value1));
    }

    @Test
    public void testLoadConfigurationFromFile() throws Exception {
        File confFile = temporaryFolder.newFile(Configuration.REMOTE_SHUFFLE_CONF_FILENAME);

        String value1 = "value1";
        String value2 = "value2";
        String value3 = "value3";

        try (FileWriter fileWriter = new FileWriter(confFile)) {
            fileWriter.write(KEY_1 + ": " + value1 + "\n");
            fileWriter.write(KEY_2 + ": " + value2 + "\n");
            fileWriter.write("#" + KEY_3 + ": " + value3 + "\n");
        }

        Configuration configuration = new Configuration(confFile.getParent());
        assertEquals(value1, configuration.getString(KEY_1));
        assertEquals(value2, configuration.getString(KEY_2));
        assertNull(configuration.getString(KEY_3));
    }

    @Test
    public void testDynamicConfiguration() throws Exception {
        File confFile = temporaryFolder.newFile(Configuration.REMOTE_SHUFFLE_CONF_FILENAME);

        String value1 = "value1";
        String value2 = "value2";
        String value3 = "value3";

        try (FileWriter fileWriter = new FileWriter(confFile)) {
            fileWriter.write(KEY_1 + ": " + value1 + "\n");
            fileWriter.write(KEY_2 + ": " + value2 + "\n");
        }

        Properties dynamicConfiguration = new Properties();
        dynamicConfiguration.setProperty(KEY_1, value3);

        Configuration configuration = new Configuration(confFile.getParent(), dynamicConfiguration);
        assertEquals(value3, configuration.getString(KEY_1));
        assertEquals(value2, configuration.getString(KEY_2));
    }

    private void checkGetIllegalValue(Runnable runnable) {
        try {
            runnable.run();
        } catch (ConfigurationException ignored) {
            // expected
            return;
        }
        fail("Should throw IllegalArgumentException.");
    }
}
