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
package io.github.kafkapulsar;

import static org.assertj.core.api.Assertions.assertThat;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import java.nio.ByteBuffer;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import org.junit.jupiter.api.Test;

/**
 * Drives the exact Netty pipeline the broker builds (frame decode/encode + handler) with real
 * kafka-clients request bytes, and asserts the response parses back correctly. No broker or cluster
 * required, so this runs on every PR and locks the KIP-511 header-version behaviour.
 */
class KafkaApiVersionsHandlerTest {

    private static EmbeddedChannel newChannel() {
        return new EmbeddedChannel(
                new LengthFieldBasedFrameDecoder(KafkaChannelInitializer.MAX_FRAME_LENGTH, 0, 4, 0, 4),
                new LengthFieldPrepender(4),
                new KafkaRequestHandler());
    }

    @Test
    void answersApiVersionsWithNoErrorAndPreservesCorrelationId() {
        short version = ApiKeys.API_VERSIONS.latestVersion();
        int correlationId = 7;
        ApiVersionsRequest request = new ApiVersionsRequest.Builder(version).build(version);
        RequestHeader header = new RequestHeader(ApiKeys.API_VERSIONS, version, "unit-test", correlationId);

        ByteBuffer requestBytes = request.serializeWithHeader(header);
        ByteBuf inbound = Unpooled.buffer().writeInt(requestBytes.remaining()).writeBytes(requestBytes);

        EmbeddedChannel channel = newChannel();
        // The handler consumes the inbound frame and emits an outbound response, so nothing is left
        // readable on the inbound side (writeInbound would return false) — assert on the response.
        channel.writeInbound(inbound);

        // LengthFieldPrepender emits the length prefix and payload as separate outbound buffers;
        // aggregate them into the single framed stream a socket peer would see.
        ByteBuf framed = Unpooled.buffer();
        for (ByteBuf part = channel.readOutbound(); part != null; part = channel.readOutbound()) {
            framed.writeBytes(part);
            part.release();
        }
        assertThat(framed.readableBytes()).as("a response frame should be produced").isGreaterThan(4);

        int frameLength = framed.readInt();
        ByteBuffer responseBytes = framed.nioBuffer();
        assertThat(responseBytes.remaining()).isEqualTo(frameLength);

        short responseHeaderVersion = ApiKeys.API_VERSIONS.responseHeaderVersion(version);
        ResponseHeader responseHeader = ResponseHeader.parse(responseBytes, responseHeaderVersion);
        assertThat(responseHeader.correlationId()).isEqualTo(correlationId);

        ApiVersionsResponse response = ApiVersionsResponse.parse(new ByteBufferAccessor(responseBytes), version);
        assertThat(response.data().errorCode()).isEqualTo(Errors.NONE.code());
        assertThat(response.data().apiKeys().find(ApiKeys.API_VERSIONS.id)).isNotNull();

        assertThat(channel.finish()).isFalse();
    }

    @Test
    void closesConnectionForUnsupportedApi() {
        short version = ApiKeys.METADATA.latestVersion();
        RequestHeader header = new RequestHeader(ApiKeys.METADATA, version, "unit-test", 1);
        // A bare header with no body is enough: the handler rejects the API before reading the body.
        ByteBuffer headerBytes = serializeHeader(header);
        ByteBuf inbound = Unpooled.buffer().writeInt(headerBytes.remaining()).writeBytes(headerBytes);

        EmbeddedChannel channel = newChannel();
        channel.writeInbound(inbound);

        assertThat(channel.isOpen()).isFalse();
        assertThat((ByteBuf) channel.readOutbound()).isNull();
    }

    private static ByteBuffer serializeHeader(RequestHeader header) {
        var cache = new org.apache.kafka.common.protocol.ObjectSerializationCache();
        ByteBuffer buffer = ByteBuffer.allocate(header.data().size(cache, header.headerVersion()));
        header.data().write(new ByteBufferAccessor(buffer), cache, header.headerVersion());
        buffer.flip();
        return buffer;
    }
}
