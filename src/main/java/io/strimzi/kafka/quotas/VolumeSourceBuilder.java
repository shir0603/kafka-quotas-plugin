/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

import java.util.LinkedHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.kafka.clients.admin.Admin;

/**
 * A fluent builder which ensures the <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-827%3A+Expose+log+dirs+total+and+usable+space+via+Kafka+API">KIP-827 API</a> is available and will throw exceptions if not.
 *
 * @see <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-827%3A+Expose+log+dirs+total+and+usable+space+via+Kafka+API">KIP-827 API</a>
 */
public class VolumeSourceBuilder implements AutoCloseable {

    private Supplier<Admin> adminSupplier;
    private StaticQuotaConfig config;
    private VolumeObserver volumeObserver;
    private LinkedHashMap<String, String> defaultTags = new LinkedHashMap<>();

    /**
     * Default production constructor for production usage.
     */
    public VolumeSourceBuilder() {
    }

    /**
     * Provide the builder with a supplier of Admin clients.
     * The supplier is called on each scheduled volume check to obtain the current Admin client.
     * @param adminSupplier a supplier that returns the current Admin client instance.
     * @return this to allow fluent usage of the builder.
     */
    public VolumeSourceBuilder withAdminSupplier(Supplier<Admin> adminSupplier) {
        this.adminSupplier = adminSupplier;
        return this;
    }

    /**
     * Provide the builder with the plug-in config.
     * @param config The plug-in configuration to use.
     * @return this to allow fluent usage of the builder.
     */
    public VolumeSourceBuilder withConfig(StaticQuotaConfig config) {
        this.config = config;
        return this;
    }

    /**
     * Provide the builder with the VolumeObserver.
     * @param volumesObserver The volume consumer to register for updates.
     * @return this to allow fluent usage of the builder.
     */
    public VolumeSourceBuilder withVolumeObserver(VolumeObserver volumesObserver) {
        this.volumeObserver = volumesObserver;
        return this;
    }

    /**
     * Provide the default set of tags to add to metrics.
     * @param defaultTags A linked hash map (for deterministic order) of key value pairs to add to each metric
     * @return this to allow fluent usage of the builder.
     */
    @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "The tags are defensively copied at metric creation time to allow the defaults to be updated")
    public VolumeSourceBuilder withDefaultTags(LinkedHashMap<String, String> defaultTags) {
        this.defaultTags = defaultTags;
        return this;
    }

    VolumeSource build() {
        if (!config.isSupportsKip827()) {
            throw new IllegalStateException("KIP-827 not available, this plugin requires broker version >= 3.3");
        }
        if (adminSupplier == null) {
            throw new IllegalStateException("Admin supplier must be set before building");
        }
        //Timeout just before the next job will be scheduled to run to avoid tasks queuing on the client thread pool.
        final int timeout = config.getStorageCheckInterval() - 1;
        return new VolumeSource(adminSupplier, volumeObserver, timeout, TimeUnit.SECONDS, defaultTags);
    }

    @Override
    public void close() {
        // Admin lifecycle is now managed externally by StaticQuotaCallback
    }

}
