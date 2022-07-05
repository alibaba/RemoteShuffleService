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

package com.alibaba.flink.shuffle.core.config;

import com.alibaba.flink.shuffle.common.config.ConfigOption;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/** This class holds configuration constants used by the remote shuffle deployment. */
public class KubernetesOptions {

    // --------------------------------------------------------------------------------------------
    //  Common configurations.
    // --------------------------------------------------------------------------------------------

    /** Image to use for the remote shuffle manager and worker containers. */
    public static final ConfigOption<String> CONTAINER_IMAGE =
            new ConfigOption<String>("remote-shuffle.kubernetes.container.image")
                    .defaultValue(null)
                    .description(
                            "Image to use for the remote shuffle manager and worker containers.");

    /**
     * The Kubernetes container image pull policy (IfNotPresent or Always or Never). The default
     * policy is IfNotPresent to avoid putting pressure to image repository.
     */
    public static final ConfigOption<String> CONTAINER_IMAGE_PULL_POLICY =
            new ConfigOption<String>("remote-shuffle.kubernetes.container.image.pull-policy")
                    .defaultValue("IfNotPresent")
                    .description(
                            "The Kubernetes container image pull policy (IfNotPresent or Always or"
                                    + " Never). The default policy is IfNotPresent to avoid putting"
                                    + " pressure to image repository.");

    /** Whether to enable host network for pod. Generally, host network is faster. */
    public static final ConfigOption<Boolean> POD_HOST_NETWORK_ENABLED =
            new ConfigOption<Boolean>("remote-shuffle.kubernetes.host-network.enabled")
                    .defaultValue(true)
                    .description(
                            "Whether to enable host network for pod. Generally, host network is "
                                    + "faster.");

    // --------------------------------------------------------------------------------------------
    //  ShuffleManager configurations.
    // --------------------------------------------------------------------------------------------

    /** The number of cpu used by the shuffle manager. */
    public static final ConfigOption<Double> SHUFFLE_MANAGER_CPU =
            new ConfigOption<Double>("remote-shuffle.kubernetes.manager.cpu")
                    .defaultValue(1.0)
                    .description("The number of cpu used by the shuffle manager.");

    /**
     * Env vars for the shuffle manager. Specified as key:value pairs separated by commas. For
     * example, set timezone as TZ:Asia/Shanghai.
     */
    public static final ConfigOption<Map<String, String>> SHUFFLE_MANAGER_ENV_VARS =
            new ConfigOption<Map<String, String>>("remote-shuffle.kubernetes.manager.env-vars")
                    .defaultValue(Collections.emptyMap())
                    .description(
                            "Env vars for the shuffle manager. Specified as key:value pairs "
                                    + "separated by commas. For example, set timezone as "
                                    + "TZ:Asia/Shanghai.");

    /**
     * Specify the kubernetes EmptyDir volumes that will be mounted into shuffle manager container.
     * Following attribute can be configured:
     *
     * <p>'name', the name of the volume. For example, 'name:disk1' means the volume named disk1.
     *
     * <p>'sizeLimit', the limit size of the volume. For example, 'sizeLimit:5Gi' means the volume
     * size is limited to 5Gi.
     *
     * <p>'mountPath', the mount path in container. For example, 'mountPath:/opt/disk1' means this
     * volume will be mounted as /opt/disk1 path.
     */
    public static final ConfigOption<List<Map<String, String>>> SHUFFLE_MANAGER_EMPTY_DIR_VOLUMES =
            new ConfigOption<List<Map<String, String>>>(
                            "remote-shuffle.kubernetes.manager.volume.empty-dirs")
                    .defaultValue(Collections.emptyList())
                    .description(
                            "Specify the kubernetes empty dir volumes that will be mounted into "
                                    + "shuffle manager container. The value should be in form of "
                                    + "name:disk1,sizeLimit:5Gi,mountPath:/opt/disk1;name:disk2,"
                                    + "sizeLimit:5Gi,mountPath:/opt/disk2. More specifically, "
                                    + "'name' is the name of the volume, 'sizeLimit' is the limit "
                                    + "size of the volume and 'mountPath' is the mount path in "
                                    + "container.");

    /**
     * Specify the kubernetes HostPath volumes that will be mounted into shuffle manager container.
     * Following attribute can be configured:
     *
     * <p>'name', the name of the volume. For example, 'name:disk1' means the volume named disk1.
     *
     * <p>'path', the directory location on host. For example, 'path:/dump/1' means the directory
     * /dump/1 on host will be mounted into container.
     *
     * <p>'mountPath', the mount path in container. For example, 'mountPath:/opt/disk1' means this
     * volume will be mounted as /opt/disk1 path.
     */
    public static final ConfigOption<List<Map<String, String>>> SHUFFLE_MANAGER_HOST_PATH_VOLUMES =
            new ConfigOption<List<Map<String, String>>>(
                            "remote-shuffle.kubernetes.manager.volume.host-paths")
                    .defaultValue(Collections.emptyList())
                    .description(
                            "Specify the kubernetes HostPath volumes that will be mounted into "
                                    + "shuffle manager container. The value should be in form of "
                                    + "name:disk1,path:/dump/1,mountPath:/opt/disk1;name:disk2,"
                                    + "path:/dump/2,mountPath:/opt/disk2. More specifically, "
                                    + "'name' is the name of the volume, 'path' is the directory "
                                    + "location on host and 'mountPath' is the mount path in "
                                    + "container.");

    /**
     * The user-specified labels to be set for the shuffle manager pod. Specified as key:value pairs
     * separated by commas. For example, version:alphav1,deploy:test.
     */
    public static final ConfigOption<Map<String, String>> SHUFFLE_MANAGER_LABELS =
            new ConfigOption<Map<String, String>>("remote-shuffle.kubernetes.manager.labels")
                    .defaultValue(Collections.emptyMap())
                    .description(
                            "The user-specified labels to be set for the shuffle manager pod. "
                                    + "Specified as key:value pairs separated by commas. For "
                                    + "example, version:alphav1,deploy:test.");

    /**
     * The user-specified node selector to be set for the shuffle manager pod. Specified as
     * key:value pairs separated by commas. For example, environment:production,disk:ssd.
     */
    public static final ConfigOption<Map<String, String>> SHUFFLE_MANAGER_NODE_SELECTOR =
            new ConfigOption<Map<String, String>>("remote-shuffle.kubernetes.manager.node-selector")
                    .defaultValue(Collections.emptyMap())
                    .description(
                            "The user-specified node selector to be set for the shuffle manager "
                                    + "pod. Specified as key:value pairs separated by commas. For "
                                    + "example, environment:production,disk:ssd.");

    /**
     * The user-specified tolerations to be set to the shuffle manager pod. The value should be in
     * the form of
     * key:key1,operator:Equal,value:value1,effect:NoSchedule;key:key2,operator:Exists,effect:NoExecute,tolerationSeconds:6000.
     */
    public static final ConfigOption<List<Map<String, String>>> SHUFFLE_MANAGER_TOLERATIONS =
            new ConfigOption<List<Map<String, String>>>(
                            "remote-shuffle.kubernetes.manager.tolerations")
                    .defaultValue(Collections.emptyList())
                    .description(
                            "The user-specified tolerations to be set to the shuffle manager pod. "
                                    + "The value should be in the form of key:key1,operator:Equal,"
                                    + "value:value1,effect:NoSchedule;key:key2,operator:Exists,"
                                    + "effect:NoExecute,tolerationSeconds:6000.");

    // --------------------------------------------------------------------------------------------
    //  ShuffleWorker configurations.
    // --------------------------------------------------------------------------------------------

    /** The number of cpu used by the shuffle worker. */
    public static final ConfigOption<Double> SHUFFLE_WORKER_CPU =
            new ConfigOption<Double>("remote-shuffle.kubernetes.worker.cpu")
                    .defaultValue(1.0)
                    .description("The number of cpu used by the shuffle worker.");

    /**
     * Env vars for the shuffle worker. Specified as key:value pairs separated by commas. For
     * example, set timezone as TZ:Asia/Shanghai.
     */
    public static final ConfigOption<Map<String, String>> SHUFFLE_WORKER_ENV_VARS =
            new ConfigOption<Map<String, String>>("remote-shuffle.kubernetes.worker.env-vars")
                    .defaultValue(Collections.emptyMap())
                    .description(
                            "Env vars for the shuffle worker. Specified as key:value pairs "
                                    + "separated by commas. For example, set timezone as "
                                    + "TZ:Asia/Shanghai.");

    /**
     * Specify the kubernetes EmptyDir volumes that will be mounted into shuffle worker container.
     * Following attribute can be configured:
     *
     * <p>'name', the name of the volume. For example, 'name:disk1' means the volume named disk1.
     *
     * <p>'sizeLimit', the limit size of the volume. For example, 'sizeLimit:5Gi' means the volume
     * size is limited to 5Gi.
     *
     * <p>'mountPath', the mount path in container. For example, 'mountPath:/opt/disk1' means this
     * volume will be mounted as /opt/disk1 path.
     */
    public static final ConfigOption<List<Map<String, String>>> SHUFFLE_WORKER_EMPTY_DIR_VOLUMES =
            new ConfigOption<List<Map<String, String>>>(
                            "remote-shuffle.kubernetes.worker.volume.empty-dirs")
                    .defaultValue(Collections.emptyList())
                    .description(
                            "Specify the kubernetes empty dir volumes that will be mounted into "
                                    + "shuffle worker container. The value should be in form of "
                                    + "name:disk1,sizeLimit:5Gi,mountPath:/opt/disk1;name:disk2,"
                                    + "sizeLimit:5Gi,mountPath:/opt/disk2. More specifically, "
                                    + "'name' is the name of the volume, 'sizeLimit', the limit "
                                    + "size of the volume and 'mountPath' is the mount path in "
                                    + "container.");

    /**
     * Specify the kubernetes HostPath volumes that will be mounted into shuffle worker container.
     * Following attribute can be configured:
     *
     * <p>'name', the name of the volume. For example, 'name:disk1' means the volume named disk1.
     *
     * <p>'path', the directory location on host. For example, 'path:/dump/1' means the directory
     * /dump/1 on host will be mounted into container.
     *
     * <p>'mountPath', the mount path in container. For example, 'mountPath:/opt/disk1' means this
     * volume will be mounted as /opt/disk1 path.
     */
    public static final ConfigOption<List<Map<String, String>>> SHUFFLE_WORKER_HOST_PATH_VOLUMES =
            new ConfigOption<List<Map<String, String>>>(
                            "remote-shuffle.kubernetes.worker.volume.host-paths")
                    .defaultValue(Collections.emptyList())
                    .description(
                            "Specify the kubernetes HostPath volumes that will be mounted into "
                                    + "shuffle worker container. The value should be in form of "
                                    + "name:disk1,path:/dump/1,mountPath:/opt/disk1;name:disk2,"
                                    + "path:/dump/2,mountPath:/opt/disk2. More specifically, 'name'"
                                    + " is the name of the volume, 'path' is the directory location"
                                    + " on host and 'mountPath' is the mount path in container.");

    /**
     * The user-specified labels to be set for the shuffle worker pods. Specified as key:value pairs
     * separated by commas. For example, version:alphav1,deploy:test.
     */
    public static final ConfigOption<Map<String, String>> SHUFFLE_WORKER_LABELS =
            new ConfigOption<Map<String, String>>("remote-shuffle.kubernetes.worker.labels")
                    .defaultValue(Collections.emptyMap())
                    .description(
                            "The user-specified labels to be set for the shuffle worker pods. "
                                    + "Specified as key:value pairs separated by commas. For "
                                    + "example, version:alphav1,deploy:test.");

    /**
     * The user-specified node selector to be set for the shuffle worker pods. Specified as
     * key:value pairs separated by commas. For example, environment:production,disk:ssd.
     */
    public static final ConfigOption<Map<String, String>> SHUFFLE_WORKER_NODE_SELECTOR =
            new ConfigOption<Map<String, String>>("remote-shuffle.kubernetes.worker.node-selector")
                    .defaultValue(Collections.emptyMap())
                    .description(
                            "The user-specified node selector to be set for the shuffle worker "
                                    + "pods. Specified as key:value pairs separated by commas. For "
                                    + "example, environment:production,disk:ssd.");

    /**
     * The user-specified tolerations to be set to the shuffle worker pod. The value should be in
     * the form of
     * key:key1,operator:Equal,value:value1,effect:NoSchedule;key:key2,operator:Exists,effect:NoExecute,tolerationSeconds:6000.
     */
    public static final ConfigOption<List<Map<String, String>>> SHUFFLE_WORKER_TOLERATIONS =
            new ConfigOption<List<Map<String, String>>>(
                            "remote-shuffle.kubernetes.worker.tolerations")
                    .defaultValue(Collections.emptyList())
                    .description(
                            "The user-specified tolerations to be set to the shuffle worker pods. "
                                    + "The value should be in the form of key:key1,operator:Equal,"
                                    + "value:value1,effect:NoSchedule;key:key2,operator:Exists,"
                                    + "effect:NoExecute,tolerationSeconds:6000.");

    /**
     * The prefix of Kubernetes resource limit factor. It should not be less than 1. The resource
     * could be cpu, memory, ephemeral-storage and all other types supported by Kubernetes.
     */
    public static final String SHUFFLE_MANAGER_RESOURCE_LIMIT_FACTOR_PREFIX =
            "remote-shuffle.kubernetes.manager.limit-factor.";

    /**
     * The prefix of Kubernetes resource limit factor. It should not be less than 1. The resource
     * could be cpu, memory, ephemeral-storage and all other types supported by Kubernetes.
     */
    public static final String SHUFFLE_WORKER_RESOURCE_LIMIT_FACTOR_PREFIX =
            "remote-shuffle.kubernetes.worker.limit-factor.";

    // ------------------------------------------------------------------------

    /** Not intended to be instantiated. */
    private KubernetesOptions() {}
}
