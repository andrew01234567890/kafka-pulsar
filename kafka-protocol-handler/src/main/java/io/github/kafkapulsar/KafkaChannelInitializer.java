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

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

/**
 * Builds the per-connection Netty pipeline for the Kafka listener. Kafka frames each request/response
 * with a 4-byte big-endian length prefix, so inbound frames are split by that length and outbound
 * buffers are prefixed with it.
 */
public class KafkaChannelInitializer extends ChannelInitializer<SocketChannel> {

    /** Kafka's default {@code socket.request.max.bytes} is 100 MiB. */
    public static final int MAX_FRAME_LENGTH = 100 * 1024 * 1024;

    @Override
    protected void initChannel(SocketChannel ch) {
        ch.pipeline()
                .addLast("frameDecoder", new LengthFieldBasedFrameDecoder(MAX_FRAME_LENGTH, 0, 4, 0, 4))
                .addLast("frameEncoder", new LengthFieldPrepender(4))
                .addLast("handler", new KafkaRequestHandler());
    }
}
