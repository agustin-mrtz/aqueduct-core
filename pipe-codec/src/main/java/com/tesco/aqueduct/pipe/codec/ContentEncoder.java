package com.tesco.aqueduct.pipe.codec;

import io.micronaut.context.annotation.Property;
import io.micronaut.http.HttpRequest;
import jakarta.inject.Inject;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

import static com.tesco.aqueduct.pipe.api.HttpHeaders.X_CONTENT_ENCODING;
import static io.micronaut.http.HttpHeaders.ACCEPT_ENCODING;
import static io.micronaut.http.HttpHeaders.CONTENT_ENCODING;

public class ContentEncoder {

    private final int compressionThreshold;
    private final BrotliCodec brotliCodec;
    private final GzipCodec gzipCodec;

    @Inject
    public ContentEncoder(
        @Property(name = "compression.threshold-in-bytes") int compressionThreshold,
        BrotliCodec brotliCodec,
        GzipCodec gzipCodec
    ) {
        this.compressionThreshold = compressionThreshold;
        this.brotliCodec = brotliCodec;
        this.gzipCodec = gzipCodec;
    }

    public EncodedResponse encodeResponse(HttpRequest<?> request, byte[] responseBytes) {
        Map<CharSequence, CharSequence> headers = new HashMap<>();
        byte[] responseBody = responseBytes;
        if (needsCompression(request, responseBytes)){
            if (request.getHeaders().get(ACCEPT_ENCODING).contains("br") && brotliCodec.isAvailable()) {
                responseBody = brotliCodec.encode(responseBytes);
                headers.put(X_CONTENT_ENCODING, brotliCodec.getHeaderType());
            } else if (request.getHeaders().get(ACCEPT_ENCODING).contains("gzip")) {
                responseBody = gzipCodec.encode(responseBytes);
                headers.put(X_CONTENT_ENCODING, gzipCodec.getHeaderType());
                headers.put(CONTENT_ENCODING, gzipCodec.getHeaderType());
            }
        }
        return new EncodedResponse(responseBody, headers);
    }

    private boolean needsCompression(HttpRequest<?> request, byte[] responseBytes) {
        return responseBytes.length > compressionThreshold && request.getHeaders().contains(ACCEPT_ENCODING);
    }

    @Getter
    public class EncodedResponse {
        byte[] encodedBody;
        Map<CharSequence, CharSequence> headers;

        public EncodedResponse(byte[] encodedBody, Map<CharSequence, CharSequence> headers) {
            this.encodedBody = encodedBody;
            this.headers = headers;
        }
    }
}
