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
package io.github.kafkapulsar.common;

import java.util.Objects;

/**
 * Translates between Kafka topic names (flat, e.g. {@code orders}) and Pulsar topic names
 * (e.g. {@code persistent://public/default/orders}). A Kafka topic maps to a single Pulsar
 * topic under a fixed tenant/namespace; a name that already carries a Pulsar scheme is passed
 * through unchanged.
 */
public final class KafkaTopicMapper {

    private static final String PERSISTENT_SCHEME = "persistent://";
    private static final String NON_PERSISTENT_SCHEME = "non-persistent://";

    private final String tenant;
    private final String namespace;

    public KafkaTopicMapper(String tenant, String namespace) {
        this.tenant = Objects.requireNonNull(tenant, "tenant");
        this.namespace = Objects.requireNonNull(namespace, "namespace");
    }

    public static KafkaTopicMapper defaults() {
        return new KafkaTopicMapper("public", "default");
    }

    public String toPulsarTopic(String kafkaTopic) {
        Objects.requireNonNull(kafkaTopic, "kafkaTopic");
        if (kafkaTopic.startsWith(PERSISTENT_SCHEME) || kafkaTopic.startsWith(NON_PERSISTENT_SCHEME)) {
            return kafkaTopic;
        }
        return PERSISTENT_SCHEME + tenant + "/" + namespace + "/" + kafkaTopic;
    }

    public String toKafkaTopic(String pulsarTopic) {
        Objects.requireNonNull(pulsarTopic, "pulsarTopic");
        String withoutScheme = pulsarTopic;
        int scheme = withoutScheme.indexOf("://");
        if (scheme >= 0) {
            withoutScheme = withoutScheme.substring(scheme + 3);
        }
        int lastSlash = withoutScheme.lastIndexOf('/');
        return lastSlash >= 0 ? withoutScheme.substring(lastSlash + 1) : withoutScheme;
    }

    public String tenant() {
        return tenant;
    }

    public String namespace() {
        return namespace;
    }
}
