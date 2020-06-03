package com.tesco.aqueduct.pipe.location;

import com.tesco.aqueduct.pipe.api.LocationResolver;
import com.tesco.aqueduct.pipe.logger.PipeLogger;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class CloudLocationResolver implements LocationResolver {

    private static final PipeLogger LOG = new PipeLogger(LoggerFactory.getLogger(CloudLocationResolver.class));

    private final LocationServiceClient locationServiceClient;

    public CloudLocationResolver(@NotNull LocationServiceClient locationServiceClient) {
        this.locationServiceClient = locationServiceClient;
    }

    @Override
    public List<String> resolve(@NotNull String locationId) {
        final String traceId = UUID.randomUUID().toString();
        try {
            return locationServiceClient.getClusters(traceId, locationId)
                .getBody()
                .map(LocationServiceClusterResponse::getClusters)
                .orElseThrow(() -> new LocationServiceException("Unexpected response body, please check location service contract for this endpoint."));

        } catch (final HttpClientResponseException exception) {
            LOG.error("resolve", "trace_id: " + traceId, exception);
            if (exception.getStatus().getCode() > 499) {
                throw new LocationServiceException("Unexpected error from location service with status - " + exception.getStatus());
            } else {
                throw exception;
            }
        }
    }
}