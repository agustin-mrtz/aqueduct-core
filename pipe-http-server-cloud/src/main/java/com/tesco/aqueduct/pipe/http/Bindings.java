package com.tesco.aqueduct.pipe.http;

import com.tesco.aqueduct.pipe.api.LocationService;
import com.tesco.aqueduct.pipe.api.TokenProvider;
import com.tesco.aqueduct.pipe.identity.issuer.IdentityIssueTokenClient;
import com.tesco.aqueduct.pipe.identity.issuer.IdentityIssueTokenProvider;
import com.tesco.aqueduct.pipe.location.CloudLocationService;
import com.tesco.aqueduct.pipe.location.LocationServiceClient;
import com.tesco.aqueduct.pipe.metrics.Measure;
import com.tesco.aqueduct.pipe.storage.ClusterStorage;
import com.tesco.aqueduct.pipe.storage.GlobalLatestOffsetCache;
import com.tesco.aqueduct.pipe.storage.PostgresqlStorage;
import com.tesco.aqueduct.registry.model.NodeRegistry;
import com.tesco.aqueduct.registry.model.NodeRequestStorage;
import com.tesco.aqueduct.registry.postgres.PostgreSQLNodeRegistry;
import com.tesco.aqueduct.registry.postgres.PostgreSQLNodeRequestStorage;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Value;

import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
import javax.sql.DataSource;
import java.net.URL;
import java.time.Duration;

@Factory
@Singleton
public class Bindings {

    // This provides Reader as it implements it
    @Singleton
    @Named("local")
    PostgresqlStorage bindPostgreSQL(
        @Property(name = "persistence.read.limit") final int limit,
        @Property(name = "persistence.read.retry-after") final int retryAfter,
        @Property(name = "persistence.read.max-batch-size") final int maxBatchSize,
        @Value("${persistence.read.expected-node-count}") final int expectedNodeCount,
        @Value("${persistence.read.cluster-db-pool-size}") final long clusterDBPoolSize,
        @Value("${persistence.read.work-mem-mb:4}") final int workMemMb,
        @Named("pipe") final DataSource pipeDataSource,
        final GlobalLatestOffsetCache globalLatestOffsetCache,
        ClusterStorage clusterStorage,
        @Named("compaction") final DataSource compactionDataSource
    ) {
        return new PostgresqlStorage(
            pipeDataSource, compactionDataSource, limit, retryAfter, maxBatchSize, globalLatestOffsetCache, expectedNodeCount, clusterDBPoolSize, workMemMb, clusterStorage
        );
    }

    @Singleton
    ClusterStorage clusterStorage(
        @Named("pipe") final DataSource dataSource,
        @Value("${location.clusters.cache.expire-after-write}") final Duration expireAfter,
        final LocationService locationService
    ) {
        return new ClusterStorage(locationService, expireAfter);
    }

    @Singleton
    @Measure
    NodeRegistry bindNodeRegistry(
        @Named("registry") final DataSource dataSource,
        @Property(name = "pipe.server.url") final URL selfUrl,
        @Value("${registry.mark-offline-after:1m}") final Duration markAsOffline,
        @Value("${registry.remove-offline-after:1m}") final Duration removeOffline
    ) {
        return new PostgreSQLNodeRegistry(dataSource, selfUrl, markAsOffline, removeOffline);
    }

    @Singleton
    @Measure
    NodeRequestStorage bindNodeRequestStorage(@Named("registry") final DataSource dataSource) {
        return new PostgreSQLNodeRequestStorage(dataSource);
    }

    @Singleton
    TokenProvider bindTokenProvider(
        final Provider<IdentityIssueTokenClient> identityIssueTokenClient,
        @Property(name = "authentication.identity.client.id") String identityClientId,
        @Property(name = "authentication.identity.client.secret") String identityClientSecret
    ) {
        return new IdentityIssueTokenProvider(identityIssueTokenClient, identityClientId, identityClientSecret);
    }

    @Singleton
    LocationService locationService(final Provider<LocationServiceClient> locationServiceClientProvider) {
        return new CloudLocationService(locationServiceClientProvider);
    }

}
