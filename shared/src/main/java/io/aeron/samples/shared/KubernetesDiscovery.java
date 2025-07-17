/*
 * Copyright 2023 Adaptive Financial Consulting
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aeron.samples.shared;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodList;
import io.kubernetes.client.util.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kubernetes discovery utilities for Aeron applications
 */
public class KubernetesDiscovery
{
    private static final Logger LOGGER = LoggerFactory.getLogger(KubernetesDiscovery.class);

    /**
     * Checks if Kubernetes discovery is enabled via environment variable
     * @return true if K8S_DISCOVERY environment variable is set to "true"
     */
    public static boolean isKubernetesDiscoveryEnabled()
    {
        final String k8sDiscovery = System.getenv("K8S_DISCOVERY");
        return "true".equalsIgnoreCase(k8sDiscovery);
    }

    /**
     * Gets the resolver bootstrap port
     * @return resolver bootstrap port, default 8050
     */
    public static int getResolverBootstrapPort()
    {
        String portString = System.getenv("RESOLVER_BOOTSTRAP_PORT");
        if (null == portString || portString.isEmpty())
        {
            portString = System.getProperty("resolver.bootstrap.port", "8050");
        }
        return Integer.parseInt(portString);
    }

    /**
     * Gets the current pod name from environment variable
     * @return pod name or null if not found
     */
    public static String getPodName()
    {
        return System.getenv("HOSTNAME");
    }

    /**
     * Detects if the application is running inside a Kubernetes pod by checking for
     * the presence of Kubernetes service account token file.
     * @return true if running in Kubernetes, false otherwise
     */
    public static boolean isRunningInKubernetes()
    {
        final Path tokenPath = Paths.get("/var/run/secrets/kubernetes.io/serviceaccount/token");
        return Files.exists(tokenPath);
    }

    /**
     * Gets the Kubernetes namespace from environment variable or service account namespace file.
     * @return the Kubernetes namespace
     */
    public static String getKubernetesNamespace()
    {
        String namespace = System.getenv("K8S_NAMESPACE");
        if (null != namespace && !namespace.isEmpty())
        {
            return namespace;
        }

        final Path namespacePath = Paths.get("/var/run/secrets/kubernetes.io/serviceaccount/namespace");
        if (Files.exists(namespacePath))
        {
            try
            {
                namespace = Files.readString(namespacePath).trim();
                if (!namespace.isEmpty())
                {
                    return namespace;
                }
            }
            catch (final IOException e)
            {
                LOGGER.warn("Failed to read namespace from service account file", e);
            }
        }

        return "default";
    }

    /**
     * Detects if the application is running inside Kubernetes and extracts pod IP addresses
     * based on environment variables for labels and namespace using the Kubernetes Java API.
     *
     * Environment variables:
     * - K8S_DISCOVERY: Set to "true" to enable Kubernetes discovery
     * - K8S_POD_LABELS: Comma-separated key=value pairs for pod labels to match (e.g., "app=cluster,role=node")
     * - K8S_NAMESPACE: Kubernetes namespace to search in (defaults to current namespace)
     *
     * @return List of pod IP addresses matching the specified labels and namespace
     */
    public static List<String> getKubernetesPodIPs()
    {
        final List<String> podIPs = new ArrayList<>();

        if (!isKubernetesDiscoveryEnabled())
        {
            LOGGER.info("Kubernetes discovery is disabled");
            return podIPs;
        }

        if (!isRunningInKubernetes())
        {
            LOGGER.error("K8S_DISCOVERY is enabled but application is not running in Kubernetes environment");
            System.exit(-1);
        }

        String podLabels = System.getenv("K8S_POD_LABELS");
        if (null == podLabels || podLabels.isEmpty())
        {
            podLabels = "aeron.io/media-driver=true";
            LOGGER.info("K8S_POD_LABELS not set, using default: {}", podLabels);
        }

        final String namespace = getKubernetesNamespace();
        LOGGER.info("Searching for pods with labels '{}' in namespace '{}'", podLabels, namespace);

        try
        {
            final ApiClient client = Config.defaultClient();
            Configuration.setDefaultApiClient(client);

            final CoreV1Api api = new CoreV1Api();
            final V1PodList podList = api.listNamespacedPod(namespace)
                .labelSelector(podLabels)
                .fieldSelector("status.phase=Running")
                .execute();

            for (final V1Pod pod : podList.getItems())
            {
                try
                {
                    if (null != pod.getStatus() && null != pod.getStatus().getPodIP())
                    {
                        final String podIP = pod.getStatus().getPodIP();
                        if (!podIP.isEmpty() && !"<none>".equals(podIP))
                        {
                            podIPs.add(podIP);
                        }
                    }
                }
                catch (final Exception e)
                {
                    LOGGER.warn("Error processing pod {}: {}",
                        pod.getMetadata() != null ? pod.getMetadata().getName() : "unknown",
                        e.getMessage());
                }
            }

            LOGGER.info("Found {} pod IPs: {}", podIPs.size(), podIPs);
        }
        catch (final ApiException e)
        {
            LOGGER.error("Kubernetes API error: {} - {}", e.getCode(), e.getResponseBody());
        }
        catch (final IOException e)
        {
            LOGGER.error("Error configuring Kubernetes client", e);
        }

        return podIPs;
    }

    /**
     * Builds the resolver bootstrap neighbors string from Kubernetes pod IPs
     * @param port the port to use for each neighbor
     * @return comma-separated string of IP:port neighbors, or null if none found
     */
    public static String buildResolverBootstrapNeighbors(final int port)
    {
        if (!isKubernetesDiscoveryEnabled())
        {
            return null;
        }

        final List<String> podIPs = getKubernetesPodIPs();
        if (podIPs.isEmpty())
        {
            LOGGER.error("Kubernetes discovery is enabled but no pod IPs were found");
            System.exit(-1);
        }

        final StringBuilder neighbors = new StringBuilder();
        for (int i = 0; i < podIPs.size(); i++)
        {
            if (i > 0)
            {
                neighbors.append(",");
            }
            neighbors.append(podIPs.get(i)).append(":").append(port);
        }

        final String result = neighbors.toString();
        LOGGER.info("Resolver bootstrap neighbors: {}", result);
        return result;
    }

    /**
     * Configures resolver bootstrap neighbors and related settings for Kubernetes discovery.
     * This method handles the complete resolver configuration when Kubernetes discovery is enabled.
     *
     * @param configureResolver a functional interface to configure the resolver settings
     * @return true if Kubernetes discovery was enabled and configuration was applied, false otherwise
     */
    public static boolean configureResolverBootstrapNeighbors(final ResolverConfigurator configureResolver)
    {
        if (!isKubernetesDiscoveryEnabled())
        {
            LOGGER.info("Kubernetes discovery is disabled");
            return false;
        }

        final int resolverBootstrapPort = getResolverBootstrapPort();
        final String resolverBootstrapNeighbors = buildResolverBootstrapNeighbors(resolverBootstrapPort);

        if (null != resolverBootstrapNeighbors && !resolverBootstrapNeighbors.isEmpty())
        {
            final String podName = getPodName();
            configureResolver.configure(resolverBootstrapNeighbors, podName);
            LOGGER.info("Configured resolver bootstrap neighbors: {}", resolverBootstrapNeighbors);

            if (null != podName && !podName.isEmpty())
            {
                LOGGER.info("Configured resolver name: {}", podName);
            }
            return true;
        }
        else
        {
            LOGGER.info("No resolver bootstrap neighbors configured");
            return false;
        }
    }

    /**
     * Logs media driver context configuration details for debugging purposes.
     *
     * @param contextDebugger a functional interface to log context details
     */
    public static void logMediaDriverContextConfiguration(final ContextDebugger contextDebugger)
    {
        if (isKubernetesDiscoveryEnabled())
        {
            contextDebugger.logConfiguration();
        }
    }

    /**
     * Functional interface for configuring resolver settings
     */
    @FunctionalInterface
    public interface ResolverConfigurator
    {
        /**
         * Configure resolver settings
         * @param resolverBootstrapNeighbors the comma-separated list of neighbors
         * @param podName the pod name for resolver name, may be null
         */
        void configure(String resolverBootstrapNeighbors, String podName);
    }

    /**
     * Functional interface for logging context configuration details
     */
    @FunctionalInterface
    public interface ContextDebugger
    {
        /**
         * Log configuration details
         */
        void logConfiguration();
    }
}