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
import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.protocol.ProtocolHandler;
import org.apache.pulsar.broker.service.BrokerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point loaded by the Pulsar broker from the NAR when {@code messagingProtocols=kafka}.
 *
 * <p>This is the walking-skeleton implementation: it binds the configured Kafka listener(s) and
 * answers {@code ApiVersions} so the packaging, NAR discovery and listener wiring can be verified
 * end-to-end. The real protocol surface (Metadata/Produce/Fetch/consumer groups/...) is layered on
 * top in later phases.
 */
public class KafkaProtocolHandler implements ProtocolHandler {

    private static final Logger log = LoggerFactory.getLogger(KafkaProtocolHandler.class);

    public static final String PROTOCOL_NAME = "kafka";
    static final String LISTENERS_KEY = "kafkaListeners";
    static final String DEFAULT_LISTENERS = "PLAINTEXT://0.0.0.0:9092";

    private volatile String listeners;

    @Override
    public String protocolName() {
        return PROTOCOL_NAME;
    }

    @Override
    public boolean accept(String protocol) {
        return PROTOCOL_NAME.equalsIgnoreCase(protocol);
    }

    @Override
    public void initialize(ServiceConfiguration conf) {
        Object configured = conf.getProperties().get(LISTENERS_KEY);
        this.listeners = (configured == null || configured.toString().isBlank())
                ? DEFAULT_LISTENERS
                : configured.toString();
        log.info("Kafka protocol handler initialized; listeners={}", listeners);
    }

    @Override
    public String getProtocolDataToAdvertise() {
        return listeners;
    }

    @Override
    public void start(BrokerService service) {
        log.info("Kafka protocol handler started");
    }

    @Override
    public Map<InetSocketAddress, ChannelInitializer<SocketChannel>> newChannelInitializers() {
        Map<InetSocketAddress, ChannelInitializer<SocketChannel>> initializers = new LinkedHashMap<>();
        for (String listener : listeners.split(",")) {
            String trimmed = listener.trim();
            if (!trimmed.isEmpty()) {
                initializers.put(parseListener(trimmed), new KafkaChannelInitializer());
            }
        }
        return initializers;
    }

    @Override
    public void close() {
        // No resources to release in the walking skeleton.
    }

    static InetSocketAddress parseListener(String listener) {
        String hostPort = listener;
        int schemeIdx = hostPort.indexOf("://");
        if (schemeIdx >= 0) {
            hostPort = hostPort.substring(schemeIdx + 3);
        }
        int colon = hostPort.lastIndexOf(':');
        if (colon < 0) {
            throw new IllegalArgumentException("Invalid Kafka listener (missing port): " + listener);
        }
        String host = hostPort.substring(0, colon);
        int port = Integer.parseInt(hostPort.substring(colon + 1));
        return new InetSocketAddress(host, port);
    }
}
