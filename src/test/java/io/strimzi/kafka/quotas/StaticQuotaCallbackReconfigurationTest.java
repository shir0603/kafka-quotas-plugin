/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StaticQuotaCallbackReconfigurationTest {

    private static final String BROKER_ID_PROPERTY = "broker.id";
    private static final String BROKER_ID = "1";

    @Mock(lenient = true)
    private VolumeSourceBuilder volumeSourceBuilder;

    @Mock
    private Admin initialAdmin;

    @Mock
    private Admin newAdmin;

    @TempDir
    Path tempDir;

    private StaticQuotaCallback callback;
    private ScheduledExecutorService backgroundScheduler;

    @BeforeEach
    void setUp() {
        backgroundScheduler = Executors.newSingleThreadScheduledExecutor();
        when(volumeSourceBuilder.withConfig(any())).thenReturn(volumeSourceBuilder);
        when(volumeSourceBuilder.withVolumeObserver(any())).thenReturn(volumeSourceBuilder);
        when(volumeSourceBuilder.withDefaultTags(any())).thenReturn(volumeSourceBuilder);
        when(volumeSourceBuilder.withAdminSupplier(any())).thenReturn(volumeSourceBuilder);
        when(volumeSourceBuilder.build()).thenReturn(mock(VolumeSource.class));

        callback = new StaticQuotaCallback(
                volumeSourceBuilder,
                backgroundScheduler,
                Clock.systemUTC(),
                configs -> initialAdmin
        );

        // Configure with minimum config that enables storage check (triggers Admin creation)
        callback.configure(Map.of(
                StaticQuotaConfig.STORAGE_CHECK_INTERVAL_PROP, "10",
                StaticQuotaConfig.ADMIN_BOOTSTRAP_SERVER_PROP, "localhost:9092",
                StaticQuotaConfig.AVAILABLE_BYTES_PROP, "2",
                BROKER_ID_PROPERTY, BROKER_ID
        ));
    }

    @AfterEach
    void tearDown() {
        if (callback != null) {
            callback.close();
        }
    }

    /**
     * Stubs the describeCluster verification call so that reconfigure() can
     * complete the TLS handshake check on the given mock Admin.
     */
    private static void stubDescribeCluster(Admin adminMock) {
        var describeClusterResult = mock(org.apache.kafka.clients.admin.DescribeClusterResult.class);
        when(adminMock.describeCluster()).thenReturn(describeClusterResult);
        when(describeClusterResult.clusterId()).thenReturn(org.apache.kafka.common.KafkaFuture.completedFuture("test-cluster-id"));
    }

    @Test
    void reconfigurableConfigsReturnsAllSslKeys() {
        Set<String> configs = callback.reconfigurableConfigs();

        assertThat(configs).containsExactlyInAnyOrder(
                StaticQuotaConfig.ADMIN_SSL_TRUSTSTORE_LOCATION_PROP,
                StaticQuotaConfig.ADMIN_SSL_TRUSTSTORE_PASSWORD_PROP,
                StaticQuotaConfig.ADMIN_SSL_KEYSTORE_LOCATION_PROP,
                StaticQuotaConfig.ADMIN_SSL_KEYSTORE_PASSWORD_PROP,
                StaticQuotaConfig.ADMIN_SSL_KEY_PASSWORD_PROP
        );
    }

    @Test
    void validateReconfigurationWithValidPathsSucceeds() throws IOException {
        File truststoreFile = Files.createFile(tempDir.resolve("truststore.jks")).toFile();
        File keystoreFile = Files.createFile(tempDir.resolve("keystore.jks")).toFile();

        Map<String, String> newConfigs = Map.of(
                StaticQuotaConfig.ADMIN_SSL_TRUSTSTORE_LOCATION_PROP, truststoreFile.getAbsolutePath(),
                StaticQuotaConfig.ADMIN_SSL_KEYSTORE_LOCATION_PROP, keystoreFile.getAbsolutePath()
        );

        assertDoesNotThrow(() -> callback.validateReconfiguration(newConfigs));
    }

    @Test
    void validateReconfigurationWithInvalidPathThrowsConfigException() {
        Map<String, String> newConfigs = Map.of(
                StaticQuotaConfig.ADMIN_SSL_TRUSTSTORE_LOCATION_PROP, "/nonexistent/path/truststore.jks"
        );

        assertThrows(ConfigException.class, () -> callback.validateReconfiguration(newConfigs));
    }

    @Test
    void reconfigureSwapsAdminClientAtomically() throws IOException {
        // Given
        File truststoreFile = Files.createFile(tempDir.resolve("new-truststore.jks")).toFile();
        Admin replacementAdmin = mock(Admin.class);
        stubDescribeCluster(replacementAdmin);

        // Create a callback with a factory that returns the replacement admin on second call
        StaticQuotaCallback testCallback = new StaticQuotaCallback(
                volumeSourceBuilder,
                Executors.newSingleThreadScheduledExecutor(),
                Clock.systemUTC(),
                configs -> replacementAdmin
        );
        testCallback.configure(Map.of(
                StaticQuotaConfig.STORAGE_CHECK_INTERVAL_PROP, "10",
                StaticQuotaConfig.ADMIN_BOOTSTRAP_SERVER_PROP, "localhost:9092",
                StaticQuotaConfig.AVAILABLE_BYTES_PROP, "2",
                BROKER_ID_PROPERTY, BROKER_ID
        ));

        // When
        Map<String, Object> newConfigs = new HashMap<>();
        newConfigs.put(StaticQuotaConfig.ADMIN_SSL_TRUSTSTORE_LOCATION_PROP, truststoreFile.getAbsolutePath());
        testCallback.reconfigure(newConfigs);

        // Then — AtomicReference should now hold the replacement admin
        assertThat(testCallback.getAdminClientRef().get()).isSameAs(replacementAdmin);
        testCallback.close();
    }

    @Test
    void reconfigureClosesOldAdminClientInBackground() throws Exception {
        // Given
        Admin oldAdmin = mock(Admin.class);
        CountDownLatch closeLatch = new CountDownLatch(1);
        Mockito.doAnswer(invocation -> {
            closeLatch.countDown();
            return null;
        }).when(oldAdmin).close(any(Duration.class));

        Admin replacementAdmin = mock(Admin.class);
        stubDescribeCluster(replacementAdmin);
        Function<Map<String, Object>, Admin> sequentialFactory = new Function<>() {
            private boolean firstCall = true;
            @Override
            public Admin apply(Map<String, Object> configs) {
                if (firstCall) {
                    firstCall = false;
                    return oldAdmin;
                }
                return replacementAdmin;
            }
        };

        StaticQuotaCallback testCallback = new StaticQuotaCallback(
                volumeSourceBuilder,
                Executors.newSingleThreadScheduledExecutor(),
                Clock.systemUTC(),
                sequentialFactory
        );
        testCallback.configure(Map.of(
                StaticQuotaConfig.STORAGE_CHECK_INTERVAL_PROP, "10",
                StaticQuotaConfig.ADMIN_BOOTSTRAP_SERVER_PROP, "localhost:9092",
                StaticQuotaConfig.AVAILABLE_BYTES_PROP, "2",
                BROKER_ID_PROPERTY, BROKER_ID
        ));

        // When
        Map<String, Object> newConfigs = new HashMap<>();
        newConfigs.put(StaticQuotaConfig.ADMIN_SSL_TRUSTSTORE_PASSWORD_PROP, "newpassword");
        testCallback.reconfigure(newConfigs);

        // Then — old admin's close() should be called in background
        boolean closed = closeLatch.await(5, TimeUnit.SECONDS);
        assertThat(closed).isTrue();
        verify(oldAdmin).close(any(Duration.class));
        testCallback.close();
    }

    @Test
    void reconfigureFailsGracefullyKeepsOldClient() {
        // Given — factory that throws on second call
        Function<Map<String, Object>, Admin> failingFactory = new Function<>() {
            private boolean firstCall = true;
            @Override
            public Admin apply(Map<String, Object> configs) {
                if (firstCall) {
                    firstCall = false;
                    return initialAdmin;
                }
                throw new RuntimeException("TLS handshake failed");
            }
        };

        StaticQuotaCallback testCallback = new StaticQuotaCallback(
                volumeSourceBuilder,
                Executors.newSingleThreadScheduledExecutor(),
                Clock.systemUTC(),
                failingFactory
        );
        testCallback.configure(Map.of(
                StaticQuotaConfig.STORAGE_CHECK_INTERVAL_PROP, "10",
                StaticQuotaConfig.ADMIN_BOOTSTRAP_SERVER_PROP, "localhost:9092",
                StaticQuotaConfig.AVAILABLE_BYTES_PROP, "2",
                BROKER_ID_PROPERTY, BROKER_ID
        ));

        // When — reconfigure should NOT throw
        Map<String, Object> newConfigs = new HashMap<>();
        newConfigs.put(StaticQuotaConfig.ADMIN_SSL_TRUSTSTORE_PASSWORD_PROP, "newpassword");
        assertDoesNotThrow(() -> testCallback.reconfigure(newConfigs));

        // Then — old client should still be active
        assertThat(testCallback.getAdminClientRef().get()).isSameAs(initialAdmin);
        testCallback.close();
    }

    @Test
    void reconfigureDoesNotThrowExceptionOnFailure() {
        // Given — factory that always throws on second call
        Function<Map<String, Object>, Admin> failingFactory = new Function<>() {
            private boolean firstCall = true;
            @Override
            public Admin apply(Map<String, Object> configs) {
                if (firstCall) {
                    firstCall = false;
                    return initialAdmin;
                }
                throw new RuntimeException("Connection refused");
            }
        };

        StaticQuotaCallback testCallback = new StaticQuotaCallback(
                volumeSourceBuilder,
                Executors.newSingleThreadScheduledExecutor(),
                Clock.systemUTC(),
                failingFactory
        );
        testCallback.configure(Map.of(
                StaticQuotaConfig.STORAGE_CHECK_INTERVAL_PROP, "10",
                StaticQuotaConfig.ADMIN_BOOTSTRAP_SERVER_PROP, "localhost:9092",
                StaticQuotaConfig.AVAILABLE_BYTES_PROP, "2",
                BROKER_ID_PROPERTY, BROKER_ID
        ));

        // When + Then — should NOT propagate exception to broker
        Map<String, Object> newConfigs = new HashMap<>();
        newConfigs.put(StaticQuotaConfig.ADMIN_SSL_KEY_PASSWORD_PROP, "newkeypassword");
        assertDoesNotThrow(() -> testCallback.reconfigure(newConfigs));
        testCallback.close();
    }

    @Test
    void reconfigureKeepsOldClientWhenTlsVerificationFails() {
        // Given — factory creates a new admin, but its TLS verification fails
        Admin brokenAdmin = mock(Admin.class);
        var failingDescribeClusterResult = mock(org.apache.kafka.clients.admin.DescribeClusterResult.class);
        when(brokenAdmin.describeCluster()).thenReturn(failingDescribeClusterResult);
        org.apache.kafka.common.internals.KafkaFutureImpl<String> failingFuture = new org.apache.kafka.common.internals.KafkaFutureImpl<>();
        failingFuture.completeExceptionally(new javax.net.ssl.SSLHandshakeException("unknown certificate"));
        when(failingDescribeClusterResult.clusterId()).thenReturn(failingFuture);

        Function<Map<String, Object>, Admin> factoryReturningBrokenAdmin = new Function<>() {
            private boolean firstCall = true;
            @Override
            public Admin apply(Map<String, Object> configs) {
                if (firstCall) {
                    firstCall = false;
                    return initialAdmin;
                }
                return brokenAdmin;
            }
        };

        StaticQuotaCallback testCallback = new StaticQuotaCallback(
                volumeSourceBuilder,
                Executors.newSingleThreadScheduledExecutor(),
                Clock.systemUTC(),
                factoryReturningBrokenAdmin
        );
        testCallback.configure(Map.of(
                StaticQuotaConfig.STORAGE_CHECK_INTERVAL_PROP, "10",
                StaticQuotaConfig.ADMIN_BOOTSTRAP_SERVER_PROP, "localhost:9092",
                StaticQuotaConfig.AVAILABLE_BYTES_PROP, "2",
                BROKER_ID_PROPERTY, BROKER_ID
        ));

        // When
        Map<String, Object> newConfigs = new HashMap<>();
        newConfigs.put(StaticQuotaConfig.ADMIN_SSL_KEY_PASSWORD_PROP, "newkeypassword");
        assertDoesNotThrow(() -> testCallback.reconfigure(newConfigs));

        // Then — old client should remain, broken client should be closed
        assertThat(testCallback.getAdminClientRef().get()).isSameAs(initialAdmin);
        verify(brokenAdmin).close(any(Duration.class));
        verify(brokenAdmin, never()).close();
        testCallback.close();
    }
}
