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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import java.nio.ByteBuffer;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersion;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersionCollection;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decodes one Kafka request per inbound frame and writes the matching response. The walking skeleton
 * only answers {@code ApiVersions}; any other API closes the connection.
 *
 * <p>Wire subtlety encoded here from day one (KIP-511): the response header version is derived from
 * {@link ApiKeys#responseHeaderVersion(short)}, which returns 0 for {@code ApiVersions} regardless of
 * the negotiated body version. Getting this wrong breaks every client at the handshake.
 */
public class KafkaRequestHandler extends SimpleChannelInboundHandler<ByteBuf> {

    private static final Logger log = LoggerFactory.getLogger(KafkaRequestHandler.class);

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf frame) {
        ByteBuffer buffer = frame.nioBuffer();
        RequestHeader header = RequestHeader.parse(buffer);
        ApiKeys apiKey = header.apiKey();

        if (apiKey == ApiKeys.API_VERSIONS) {
            // Consume the request body so malformed requests are rejected early.
            AbstractRequest.parseRequest(apiKey, header.apiVersion(), new ByteBufferAccessor(buffer));
            ctx.writeAndFlush(encode(header, apiVersionsResponse()));
        } else {
            log.debug("Unsupported API {} (v{}); closing connection", apiKey, header.apiVersion());
            ctx.close();
        }
    }

    private static ApiVersionsResponseData apiVersionsResponse() {
        ApiVersionCollection apis = new ApiVersionCollection();
        apis.add(new ApiVersion()
                .setApiKey(ApiKeys.API_VERSIONS.id)
                .setMinVersion((short) 0)
                .setMaxVersion(ApiKeys.API_VERSIONS.latestVersion()));
        return new ApiVersionsResponseData()
                .setErrorCode(Errors.NONE.code())
                .setApiKeys(apis);
    }

    private static ByteBuf encode(RequestHeader requestHeader, ApiMessage body) {
        short apiVersion = requestHeader.apiVersion();
        short headerVersion = requestHeader.apiKey().responseHeaderVersion(apiVersion);
        ResponseHeaderData responseHeader =
                new ResponseHeaderData().setCorrelationId(requestHeader.correlationId());

        ObjectSerializationCache cache = new ObjectSerializationCache();
        int size = responseHeader.size(cache, headerVersion) + body.size(cache, apiVersion);
        ByteBuffer buffer = ByteBuffer.allocate(size);
        ByteBufferAccessor accessor = new ByteBufferAccessor(buffer);
        responseHeader.write(accessor, cache, headerVersion);
        body.write(accessor, cache, apiVersion);
        buffer.flip();
        return Unpooled.wrappedBuffer(buffer);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("Closing Kafka connection after error", cause);
        ctx.close();
    }
}
