/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.kafkapulsar.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * End-to-end smoke test: proves that the NAR was discovered by a real Pulsar broker running on KIND,
 * that the Kafka listener is bound, and that it speaks the wire protocol. It performs a raw
 * {@code ApiVersions} exchange (a full {@code KafkaConsumer} needs Metadata/Fetch, which land in
 * Phase 1). The bootstrap address comes from {@code KAFKA_BOOTSTRAP_SERVERS} (default
 * {@code 127.0.0.1:9092}); CI port-forwards the in-cluster broker to local 9092 — the local port
 * must equal the advertised port.
 */
@Tag("e2e")
class ApiVersionsE2eTest {

    private static final int READ_TIMEOUT_MS = 15_000;

    @Test
    void brokerAnswersApiVersionsOverTheKafkaListener() throws Exception {
        String bootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092");
        int lastColon = bootstrap.lastIndexOf(':');
        String host = bootstrap.substring(0, lastColon);
        int port = Integer.parseInt(bootstrap.substring(lastColon + 1));

        short version = ApiKeys.API_VERSIONS.latestVersion();
        int correlationId = 99;
        ApiVersionsRequest request = new ApiVersionsRequest.Builder(version).build(version);
        RequestHeader header = new RequestHeader(ApiKeys.API_VERSIONS, version, "e2e-test", correlationId);
        ByteBuffer body = request.serializeWithHeader(header);

        ByteBuffer framed = ByteBuffer.allocate(4 + body.remaining());
        framed.putInt(body.remaining()).put(body).flip();

        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), READ_TIMEOUT_MS);
            socket.setSoTimeout(READ_TIMEOUT_MS);
            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            out.write(framed.array());
            out.flush();

            int length = ByteBuffer.wrap(in.readNBytes(4)).getInt();
            assertThat(length).as("response frame length").isPositive();
            ByteBuffer response = ByteBuffer.wrap(in.readNBytes(length));

            short responseHeaderVersion = ApiKeys.API_VERSIONS.responseHeaderVersion(version);
            ResponseHeader responseHeader = ResponseHeader.parse(response, responseHeaderVersion);
            assertThat(responseHeader.correlationId()).isEqualTo(correlationId);

            ApiVersionsResponse parsed =
                    ApiVersionsResponse.parse(new ByteBufferAccessor(response), version);
            assertThat(parsed.data().errorCode()).isEqualTo(Errors.NONE.code());
            assertThat(parsed.data().apiKeys().find(ApiKeys.API_VERSIONS.id))
                    .as("broker must advertise the ApiVersions API")
                    .isNotNull();
        }
    }
}
