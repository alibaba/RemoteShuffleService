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
import com.alibaba.flink.shuffle.core.config.ManagerOptions;
import com.alibaba.flink.shuffle.core.config.WorkerOptions;
import com.alibaba.flink.shuffle.core.config.memory.ShuffleManagerProcessSpec;
import com.alibaba.flink.shuffle.core.config.memory.ShuffleWorkerProcessSpec;
import com.alibaba.flink.shuffle.core.config.memory.util.ProcessMemoryUtils;

import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkArgument;
import static com.alibaba.flink.shuffle.core.utils.ConfigurationParserUtils.CONFIG_DIR_OPTION;
import static com.alibaba.flink.shuffle.core.utils.ConfigurationParserUtils.DYNAMIC_PROPERTY_OPTION;

/** Utility class for using java utilities in bash scripts. */
public class BashJavaUtils {
    private static final Logger LOG = LoggerFactory.getLogger(BashJavaUtils.class);

    public static final String EXECUTION_PREFIX = "BASH_JAVA_UTILS_EXEC_RESULT:";

    private BashJavaUtils() {}

    public static void main(String[] args) throws Exception {
        checkArgument(args.length > 0, "Command not specified.");

        Command command = Command.valueOf(args[0]);
        String[] commandArgs = Arrays.copyOfRange(args, 1, args.length);
        String outputLine = runCommand(command, commandArgs);
        System.out.println(EXECUTION_PREFIX + outputLine);
    }

    private static String runCommand(Command command, String[] commandArgs) throws Exception {
        Configuration configuration =
                ConfigurationParserUtils.loadConfiguration(filterCmdArgs(commandArgs));
        switch (command) {
            case GET_SHUFFLE_WORKER_JVM_PARAMS:
                return getShuffleWorkerJvmParams(configuration);
            case GET_SHUFFLE_MANAGER_JVM_PARAMS:
                return getShuffleManagerJvmParams(configuration);
            default:
                // unexpected, Command#valueOf should fail if a unknown command is passed in
                throw new RuntimeException("Unexpected, something is wrong.");
        }
    }

    private static String[] filterCmdArgs(String[] args) {
        final List<String> filteredArgs = new ArrayList<>();
        final Iterator<String> iter = Arrays.asList(args).iterator();
        final Options cmdOptions = getCmdOptions();

        while (iter.hasNext()) {
            String token = iter.next();
            if (cmdOptions.hasOption(token)) {
                filteredArgs.add(token);
                if (cmdOptions.getOption(token).hasArg() && iter.hasNext()) {
                    filteredArgs.add(iter.next());
                }
            } else if (token.startsWith("-D")) {
                // "-Dkey=value"
                filteredArgs.add(token);
            }
        }

        return filteredArgs.toArray(new String[0]);
    }

    private static Options getCmdOptions() {
        final Options cmdOptions = new Options();
        cmdOptions.addOption(CONFIG_DIR_OPTION);
        cmdOptions.addOption(DYNAMIC_PROPERTY_OPTION);
        return cmdOptions;
    }

    /** Generate and print JVM parameters of Shuffle Worker resources as one line. */
    private static String getShuffleWorkerJvmParams(Configuration configuration) {

        ShuffleWorkerProcessSpec shuffleWorkerProcessSpec =
                new ShuffleWorkerProcessSpec(configuration);

        logShuffleWorkerMemoryConfiguration(shuffleWorkerProcessSpec);

        return ProcessMemoryUtils.generateJvmArgsStr(
                shuffleWorkerProcessSpec, configuration.getString(WorkerOptions.JVM_OPTIONS));
    }

    /** Generate and print JVM parameters of Shuffle Manager resources as one line. */
    private static String getShuffleManagerJvmParams(Configuration configuration) {
        ShuffleManagerProcessSpec shuffleManagerProcessSpec =
                new ShuffleManagerProcessSpec(configuration);

        logShuffleManagerMemoryConfiguration(shuffleManagerProcessSpec);

        return ProcessMemoryUtils.generateJvmArgsStr(
                shuffleManagerProcessSpec, configuration.getString(ManagerOptions.JVM_OPTIONS));
    }

    private static void logShuffleManagerMemoryConfiguration(ShuffleManagerProcessSpec spec) {
        LOG.info("ShuffleManager Memory configuration:");
        LOG.info(
                "  Total Process Memory:          {}",
                spec.getTotalProcessMemorySize().toHumanReadableString());
        LOG.info(
                "    JVM Heap Memory:             {}",
                spec.getJvmHeapMemorySize().toHumanReadableString());
        LOG.info(
                "    JVM Direct Memory:           {}",
                spec.getJvmDirectMemorySize().toHumanReadableString());
        LOG.info(
                "    JVM Metaspace:               {}",
                spec.getJvmMetaspaceSize().toHumanReadableString());
        LOG.info(
                "    JVM Overhead:                {}",
                spec.getJvmOverheadSize().toHumanReadableString());
    }

    private static void logShuffleWorkerMemoryConfiguration(ShuffleWorkerProcessSpec spec) {
        LOG.info("ShuffleWorker Memory configuration:");
        LOG.info(
                "  Total Process Memory:          {}",
                spec.getTotalProcessMemorySize().toHumanReadableString());
        LOG.info(
                "    JVM Heap Memory:             {}",
                spec.getJvmHeapMemorySize().toHumanReadableString());
        LOG.info(
                "    Total JVM Direct Memory:     {}",
                spec.getJvmDirectMemorySize().toHumanReadableString());
        LOG.info(
                "      Framework:                 {}",
                spec.getFrameworkOffHeap().toHumanReadableString());
        LOG.info(
                "      Network:                   {}",
                spec.getNetworkOffHeap().toHumanReadableString());
        LOG.info(
                "    JVM Metaspace:               {}",
                spec.getJvmMetaspaceSize().toHumanReadableString());
        LOG.info(
                "    JVM Overhead:                {}",
                spec.getJvmOverheadSize().toHumanReadableString());
    }

    /** Commands that BashJavaUtils supports. */
    public enum Command {
        /** Get JVM parameters of shuffle worker. */
        GET_SHUFFLE_WORKER_JVM_PARAMS,

        /** Get JVM parameters of shuffle manager. */
        GET_SHUFFLE_MANAGER_JVM_PARAMS,
    }
}
