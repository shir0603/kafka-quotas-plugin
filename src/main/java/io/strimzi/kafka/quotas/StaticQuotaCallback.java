/*
 * Copyright 2020, Red Hat Inc.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

import java.io.File;
import java.time.Clock;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;
import io.strimzi.kafka.quotas.throttle.AvailableBytesThrottleFactorPolicy;
import io.strimzi.kafka.quotas.throttle.AvailableRatioThrottleFactorPolicy;
import io.strimzi.kafka.quotas.throttle.FixedDurationExpiryPolicy;
import io.strimzi.kafka.quotas.throttle.PolicyBasedThrottle;
import io.strimzi.kafka.quotas.throttle.ThrottleFactor;
import io.strimzi.kafka.quotas.throttle.ThrottleFactorPolicy;
import io.strimzi.kafka.quotas.throttle.ThrottleFactorSource;
import io.strimzi.kafka.quotas.throttle.UnlimitedThrottleFactorSource;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Reconfigurable;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.metrics.Quota;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.quota.ClientQuotaCallback;
import org.apache.kafka.server.quota.ClientQuotaEntity;
import org.apache.kafka.server.quota.ClientQuotaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Locale.ENGLISH;

/**
 * Allows configuring generic quotas for a broker independent of users and clients.
 * <p>
 * Implements {@link Reconfigurable} to support dynamic SSL certificate reloading
 * via {@code kafka-configs.sh} without requiring a broker restart.
 */
public class StaticQuotaCallback implements ClientQuotaCallback, Reconfigurable {
    /**
     * Tag used for metrics to identify the broker which generated the observation.
     */
    public static final String REMOTE_BROKER_TAG = "remoteBrokerId";
    /**
     * Tag used for metrics to identify the logDir included in the observation.
     */
    public static final String LOG_DIR_TAG = "logDir";
    private static final Logger log = LoggerFactory.getLogger(StaticQuotaCallback.class);
    private static final String EXCLUDED_PRINCIPAL_QUOTA_KEY = "excluded-principal-quota-key";
    private final Clock clock;

    private volatile Map<ClientQuotaType, Quota> quotaMap = new HashMap<>();
    private volatile Set<String> excludedPrincipalNameList = Set.of();
    private final Set<ClientQuotaType> resetQuota = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private volatile ThrottleFactorSource throttleFactorSource = UnlimitedThrottleFactorSource.UNLIMITED_THROTTLE_FACTOR_SOURCE;
    private final VolumeSourceBuilder volumeSourceBuilder;
    private static final String SCOPE = "io.strimzi.kafka.quotas.StaticQuotaCallback";

    private final ScheduledExecutorService backgroundScheduler;

    /**
     * Tag used for metrics to identify the broker which is recording the observation.
     */
    public static final String HOST_BROKER_TAG = "observingBrokerId";

    // --- Dynamic SSL reconfiguration fields ---
    private final AtomicReference<Admin> adminClientRef = new AtomicReference<>();
    private final ExecutorService adminCleanupExecutor;
    private final Function<Map<String, Object>, Admin> adminClientFactory;
    private volatile Map<String, Object> currentConfigs = new HashMap<>();

    /**
     * Default constructor for production use.
     * <p>
     * It provides a default {@link io.strimzi.kafka.quotas.VolumeSourceBuilder#VolumeSourceBuilder()} and a scheduled executor for running background tasks on named threads.
     */
    public StaticQuotaCallback() {
        this(new VolumeSourceBuilder(),
                Executors.newScheduledThreadPool(2, r -> {
                    final Thread thread = new Thread(r, StaticQuotaCallback.class.getSimpleName() + "-taskExecutor");
                    thread.setDaemon(true);
                    return thread;
                }),
                Clock.systemUTC(),
                AdminClient::create);
    }

    /**
     * Secondary constructor visible for testing purposes
     *
     * @param volumeSourceBuilder the {@link io.strimzi.kafka.quotas.VolumeSourceBuilder#VolumeSourceBuilder()} to use
     * @param backgroundScheduler the scheduler for executing background tasks.
     * @param clock the time source to use when evaluating expiry
     */
    /*test*/ StaticQuotaCallback(VolumeSourceBuilder volumeSourceBuilder,
                                 ScheduledExecutorService backgroundScheduler,
                                 Clock clock) {
        this(volumeSourceBuilder, backgroundScheduler, clock, AdminClient::create);
    }

    /**
     * Tertiary constructor visible for testing purposes, with injectable admin client factory.
     *
     * @param volumeSourceBuilder the builder to use
     * @param backgroundScheduler the scheduler for executing background tasks.
     * @param clock the time source to use when evaluating expiry
     * @param adminClientFactory factory function for creating Admin clients
     */
    /*test*/ StaticQuotaCallback(VolumeSourceBuilder volumeSourceBuilder,
                                 ScheduledExecutorService backgroundScheduler,
                                 Clock clock,
                                 Function<Map<String, Object>, Admin> adminClientFactory) {
        this.volumeSourceBuilder = volumeSourceBuilder;
        this.backgroundScheduler = backgroundScheduler;
        Collections.addAll(resetQuota, ClientQuotaType.values());
        this.clock = clock;
        this.adminClientFactory = adminClientFactory;
        this.adminCleanupExecutor = Executors.newSingleThreadExecutor(r -> {
            final Thread thread = new Thread(r, StaticQuotaCallback.class.getSimpleName() + "-adminCleanup");
            thread.setDaemon(true);
            return thread;
        });
    }

    @Override
    public Map<String, String> quotaMetricTags(ClientQuotaType quotaType, KafkaPrincipal principal, String clientId) {
        Map<String, String> m = new HashMap<>();
        m.put("quota.type", quotaType.name());
        if (!excludedPrincipalNameList.isEmpty() && principal != null && excludedPrincipalNameList.contains(principal.getName())) {
            m.put(EXCLUDED_PRINCIPAL_QUOTA_KEY, Boolean.TRUE.toString());
        }
        return m;
    }

    @Override
    public Double quotaLimit(ClientQuotaType quotaType, Map<String, String> metricTags) {
        if (Boolean.TRUE.toString().equals(metricTags.get(EXCLUDED_PRINCIPAL_QUOTA_KEY))) {
            return Quota.upperBound(Double.MAX_VALUE).bound();
        }
        double quota = quotaMap.getOrDefault(quotaType, Quota.upperBound(Double.MAX_VALUE)).bound();
        if (ClientQuotaType.PRODUCE.equals(quotaType)) {
            ThrottleFactor factor = throttleFactorSource.currentThrottleFactor();
            quota = quota * factor.getThrottleFactor();
        }
        // returning zero would cause a divide by zero in Kafka, so we return 1 at minimum
        return Math.max(quota, 1d);
    }

    @Override
    public void updateQuota(ClientQuotaType quotaType, ClientQuotaEntity quotaEntity, double newValue) {
        // Unused: This plugin does not care about user or client id entities.
    }

    @Override
    public void removeQuota(ClientQuotaType quotaType, ClientQuotaEntity quotaEntity) {
        // Unused: This plugin does not care about user or client id entities.
    }

    @Override
    public boolean quotaResetRequired(ClientQuotaType quotaType) {
        return resetQuota.remove(quotaType);
    }

    @Override
    public boolean updateClusterMetadata(Cluster cluster) {
        return false;
    }

    @Override
    public void close() {
        try {
            closeExecutorService();
            closeAdminCleanupExecutor();
            closeAdminClient();
            volumeSourceBuilder.close();
        } finally {
            Metrics.defaultRegistry().allMetrics().keySet().stream().filter(m -> SCOPE.equals(m.getScope())).forEach(Metrics.defaultRegistry()::removeMetric);
        }
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // Store the full config for reconfiguration merging later
        @SuppressWarnings("unchecked")
        Map<String, Object> configsCopy = new HashMap<>((Map<String, Object>) configs);
        this.currentConfigs = configsCopy;

        StaticQuotaConfig config = new StaticQuotaConfig(configs, true);
        quotaMap = config.getQuotaMap();
        long storageCheckInterval = config.getStorageCheckInterval();
        if (storageCheckInterval > 0L) {
            final Optional<Long> availableBytesLimitConfig = config.getAvailableBytesLimit();
            final Optional<Double> availableRatioLimit = config.getAvailableRatioLimit();
            if (availableBytesLimitConfig.isPresent() || availableRatioLimit.isPresent()) {
                // Create initial Admin Client and store in AtomicReference
                Map<String, Object> adminConfig = config.getKafkaClientConfig().getKafkaClientConfig();
                Admin initialAdmin = adminClientFactory.apply(adminConfig);
                adminClientRef.set(initialAdmin);

                // Wire the builder with a supplier that reads from the AtomicReference
                volumeSourceBuilder.withAdminSupplier(adminClientRef::get);

                scheduleStorageCheck(config, availableBytesLimitConfig, availableRatioLimit, storageCheckInterval);
            } else {
                log.info("No volume size limits configured, storage check disabled");
            }
        } else {
            log.info("Static quota callback configured to never check usage: set {} to a positive value to enable", StaticQuotaConfig.STORAGE_CHECK_INTERVAL_PROP);
        }
        excludedPrincipalNameList = config.getSetOfExcludedPrincipals();
        if (!excludedPrincipalNameList.isEmpty()) {
            log.info("Excluded principals {}", excludedPrincipalNameList);
        }

        quotaMap.forEach((clientQuotaType, quota) -> {
            String name = clientQuotaType.name().toUpperCase(ENGLISH).charAt(0) + clientQuotaType.name().toLowerCase(ENGLISH).substring(1);
            Metrics.newGauge(metricName(StaticQuotaCallback.class, name), new ClientQuotaGauge(quota));
        });
    }

    // --- Reconfigurable interface implementation ---

    @Override
    public Set<String> reconfigurableConfigs() {
        return StaticQuotaConfig.RECONFIGURABLE_SSL_CONFIGS;
    }

    @Override
    public void validateReconfiguration(Map<String, ?> configs) throws ConfigException {
        validateFilePathIfPresent(configs, StaticQuotaConfig.ADMIN_SSL_TRUSTSTORE_LOCATION_PROP);
        validateFilePathIfPresent(configs, StaticQuotaConfig.ADMIN_SSL_KEYSTORE_LOCATION_PROP);
    }

    @Override
    public void reconfigure(Map<String, ?> configs) {
        try {
            log.info("Reconfiguring Admin Client with updated SSL certificates");

            // Merge new configs into the stored full config
            Map<String, Object> mergedConfigs = new HashMap<>(currentConfigs);
            for (Map.Entry<String, ?> entry : configs.entrySet()) {
                mergedConfigs.put(entry.getKey(), entry.getValue());
            }

            // Build Admin Client config from merged configuration
            StaticQuotaConfig newConfig = new StaticQuotaConfig(mergedConfigs, false);
            Map<String, Object> adminConfig = newConfig.getKafkaClientConfig().getKafkaClientConfig();

            // Create new Admin Client
            Admin newAdmin = adminClientFactory.apply(adminConfig);

            // Verify the new Admin Client can actually connect (forces TLS handshake).
            // AdminClient.create() is lazy — without this check, a client with invalid
            // certs would be swapped in and every subsequent VolumeSource.run() would fail.
            try {
                newAdmin.describeCluster().clusterId().get(30, TimeUnit.SECONDS);
            } catch (Exception verifyEx) {
                log.error("New Admin Client failed TLS/connection verification, keeping existing client: {}", verifyEx.getMessage(), verifyEx);
                try {
                    newAdmin.close(Duration.ofSeconds(5));
                } catch (Exception closeEx) {
                    log.warn("Error closing unverified Admin Client: {}", closeEx.getMessage(), closeEx);
                }
                return;
            }

            // Verification passed — commit the config change and swap atomically
            this.currentConfigs = mergedConfigs;
            Admin oldAdmin = adminClientRef.getAndSet(newAdmin);

            // Schedule background close of the old Admin Client to prevent resource leaks
            if (oldAdmin != null) {
                adminCleanupExecutor.submit(() -> {
                    try {
                        oldAdmin.close(Duration.ofSeconds(30));
                        log.info("Successfully closed old Admin Client after reconfiguration");
                    } catch (Exception e) {
                        log.warn("Error closing old Admin Client: {}", e.getMessage(), e);
                    }
                });
            }

            log.info("Admin Client reconfigured successfully with updated SSL certificates");
        } catch (Exception e) {
            log.error("Failed to reconfigure Admin Client, keeping existing client: {}", e.getMessage(), e);
            // Do NOT propagate the exception — keep old client active, do not crash the broker
        }
    }

    // --- Private helpers ---

    private void validateFilePathIfPresent(Map<String, ?> configs, String configKey) throws ConfigException {
        Object value = configs.get(configKey);
        if (value != null) {
            String path = value.toString();
            File file = new File(path);
            if (!file.exists() || !file.canRead()) {
                throw new ConfigException(configKey, path,
                        "The file does not exist or is not readable: " + path);
            }
        }
    }

    private void scheduleStorageCheck(StaticQuotaConfig config, Optional<Long> availableBytesLimitConfig, Optional<Double> availableRatioLimit, long storageCheckInterval) {
        LinkedHashMap<String, String> defaultTags = new LinkedHashMap<>();
        defaultTags.put(HOST_BROKER_TAG, config.getBrokerId());
        if (availableBytesLimitConfig.isPresent() && availableRatioLimit.isPresent()) {
            String props = String.join(",", StaticQuotaConfig.AVAILABLE_BYTES_PROP, StaticQuotaConfig.AVAILABLE_RATIO_PROP);
            throw new IllegalStateException("Both limit types configured, only configure one of [" + props + "]");
        }
        ThrottleFactorPolicy throttleFactorPolicy;
        if (availableBytesLimitConfig.isPresent()) {
            throttleFactorPolicy = new AvailableBytesThrottleFactorPolicy(availableBytesLimitConfig.get());
            log.info("Available bytes limit {}", availableBytesLimitConfig.get());
        } else {
            throttleFactorPolicy = new AvailableRatioThrottleFactorPolicy(availableRatioLimit.get());
            log.info("Available ratio limit {}", availableRatioLimit.get());
        }
        FixedDurationExpiryPolicy expiryPolicy = new FixedDurationExpiryPolicy(clock, config.getThrottleFactorValidityDuration());
        final PolicyBasedThrottle factorNotifier = new PolicyBasedThrottle(throttleFactorPolicy, () -> resetQuota.add(ClientQuotaType.PRODUCE), expiryPolicy, config.getFallbackThrottleFactor(), defaultTags);
        throttleFactorSource = factorNotifier;
        VolumeObserver observer = new CachingVolumeObserver(factorNotifier, clock, config.getThrottleFactorValidityDuration(), defaultTags);
        Runnable volumeSource = volumeSourceBuilder.withConfig(config).withVolumeObserver(observer).withDefaultTags(defaultTags).build();
        backgroundScheduler.scheduleWithFixedDelay(volumeSource, 0, storageCheckInterval, TimeUnit.SECONDS);
        backgroundScheduler.scheduleWithFixedDelay(factorNotifier::checkThrottleFactorValidity, 0, 10, TimeUnit.SECONDS);
        log.info("Configured quota callback with {}. Storage check interval: {}s", quotaMap, storageCheckInterval);
    }

    private void closeExecutorService() {
        try {
            backgroundScheduler.shutdownNow();
        } catch (Exception e) {
            log.warn("Encountered problem shutting down background executor: {}", e.getMessage(), e);
        }
    }

    private void closeAdminCleanupExecutor() {
        try {
            adminCleanupExecutor.shutdownNow();
        } catch (Exception e) {
            log.warn("Encountered problem shutting down admin cleanup executor: {}", e.getMessage(), e);
        }
    }

    private void closeAdminClient() {
        Admin admin = adminClientRef.getAndSet(null);
        if (admin != null) {
            try {
                admin.close(Duration.ofSeconds(10));
            } catch (Exception e) {
                log.warn("Encountered problem closing admin client: {}", e.getMessage(), e);
            }
        }
    }

    static MetricName metricName(Class<?> clazz, String name, LinkedHashMap<String, String> tags) {
        String group = clazz.getPackageName();
        String type = clazz.getSimpleName();
        return metricName(name, type, group, tags);
    }

    static MetricName metricName(Class<?> clazz, String name) {
        String group = clazz.getPackageName();
        String type = clazz.getSimpleName();
        return metricName(name, type, group);
    }

    /**
     * Generate a Yammer metric name
     *
     * @param name  the name of the Metric.
     * @param type  the type to which the Metric belongs.
     * @param group the group to which the Metric belongs type
     * @return the MetricName object derived from the arguments.
     */
    public static MetricName metricName(String name, String type, String group) {
        final String sanitisedGroup = sanitise(group);
        final String sanitisedType = sanitise(type);
        final String sanitisedName = sanitise(name);
        String mBeanName = String.format("%s:type=%s,name=%s", sanitisedGroup, sanitisedType, sanitisedName);
        return new MetricName(sanitisedGroup, sanitisedType, sanitisedName, SCOPE, mBeanName);
    }

    /**
     * Generate a Yammer metric name
     *
     * @param name  the name of the Metric.
     * @param type  the type to which the Metric belongs.
     * @param group the group to which the Metric belongs type
     * @param tags  an ordered set of key value mappings
     * @return the MetricName object derived from the arguments.
     */
    public static MetricName metricName(String name, String type, String group, LinkedHashMap<String, String> tags) {
        final String tagValues = tags.entrySet().stream().map(entry -> String.format("%s=%s", sanitise(entry.getKey()), sanitise(entry.getValue()))).collect(Collectors.joining(","));
        String mBeanName;
        final String sanitisedGroup = sanitise(group);
        final String sanitisedType = sanitise(type);
        final String sanitisedName = sanitise(name);
        if (!tagValues.isBlank()) {
            mBeanName = String.format("%s:type=%s,name=%s,%s", sanitisedGroup, sanitisedType, sanitisedName, tagValues);
        } else {
            mBeanName = String.format("%s:type=%s,name=%s", sanitisedGroup, sanitisedType, sanitisedName);
        }
        return new MetricName(sanitisedGroup, sanitisedType, sanitisedName, SCOPE, mBeanName);
    }

    /**
     * MetricNames are translated to mbean names which need to comply with the @link{<a href="https://docs.oracle.com/en/java/javase/17/docs/api/java.management/javax/management/ObjectName.html">javax.management.ObjectName</a>} rules.
     * Unfortunately the constructors we use from Yammer don't validate the metric names, so we do it ourselves.
     *
     * @param name value to be sanitised
     * @return the value with all illegal characters removed
     */
    private static String sanitise(String name) {
        // It would be more efficient to create a single pattern and replace everything once.
        // However, I think this makes things clearer and easier to understand.
        return name.replaceAll("[:?*=,]", "")
                .replaceAll("//", "") // Double slashes are reserved as a protocol specifier
                .replaceAll("\\$$", ""); // $ is only illegal as a trailing character as it is used to denote inner classes.
    }

    /**
     * Returns the current Admin Client reference, visible for testing.
     * @return the AtomicReference holding the live Admin Client
     */
    /*test*/ AtomicReference<Admin> getAdminClientRef() {
        return adminClientRef;
    }

    private static class ClientQuotaGauge extends Gauge<Double> {
        private final Quota quota;

        public ClientQuotaGauge(Quota quota) {
            this.quota = quota;
        }

        public Double value() {
            return quota.bound();
        }
    }
}
