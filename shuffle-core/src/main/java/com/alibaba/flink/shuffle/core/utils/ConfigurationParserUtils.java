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

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.common.exception.ShuffleException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;

/** Utility class to load and parse {@link Configuration} from args and config file. */
public class ConfigurationParserUtils {

    /** The default shuffle config directory name. */
    public static final String DEFAULT_SHUFFLE_CONF_DIR = "conf";

    public static Configuration loadConfiguration(String[] args) throws IOException {
        final DefaultParser parser = new DefaultParser();
        final Options options = new Options();
        options.addOption(DYNAMIC_PROPERTY_OPTION);
        options.addOption(CONFIG_DIR_OPTION);

        final CommandLine commandLine;
        try {
            commandLine = parser.parse(options, args, true);
        } catch (ParseException e) {
            throw new ShuffleException("Failed to parse the command line arguments.", e);
        }

        final Properties dynamicProperties =
                commandLine.getOptionProperties(DYNAMIC_PROPERTY_OPTION.getOpt());
        if (commandLine.hasOption(CONFIG_DIR_OPTION.getOpt())) {
            return new Configuration(
                    commandLine.getOptionValue(CONFIG_DIR_OPTION.getOpt()), dynamicProperties);
        } else {
            return new Configuration(
                    deriveShuffleConfDirectoryFromLibDirectory(), dynamicProperties);
        }
    }

    private static String deriveShuffleConfDirectoryFromLibDirectory() {
        final String libJar =
                ConfigurationParserUtils.class
                        .getProtectionDomain()
                        .getCodeSource()
                        .getLocation()
                        .getPath();

        final File libDirectory = checkNotNull(new File(libJar).getParentFile());
        final File homeDirectory = checkNotNull(libDirectory.getParentFile());
        final File confDirectory = new File(homeDirectory, DEFAULT_SHUFFLE_CONF_DIR);

        return confDirectory.getAbsolutePath();
    }

    public static final Option CONFIG_DIR_OPTION =
            Option.builder("c")
                    .longOpt("configDir")
                    .required(false)
                    .hasArg(true)
                    .argName("configuration directory")
                    .desc(
                            "Directory which contains the configuration file "
                                    + Configuration.REMOTE_SHUFFLE_CONF_FILENAME
                                    + ".")
                    .build();

    public static final Option DYNAMIC_PROPERTY_OPTION =
            Option.builder("D")
                    .argName("property=value")
                    .numberOfArgs(2)
                    .valueSeparator('=')
                    .desc("use value for given property")
                    .build();
}
