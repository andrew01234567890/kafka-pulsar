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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import org.junit.jupiter.api.Test;

class KafkaTopicMapperTest {

    private final KafkaTopicMapper mapper = KafkaTopicMapper.defaults();

    @Test
    void mapsFlatKafkaTopicToPulsarTopicUnderDefaultTenantAndNamespace() {
        assertThat(mapper.toPulsarTopic("orders"))
                .isEqualTo("persistent://public/default/orders");
    }

    @Test
    void passesThroughNamesThatAlreadyCarryAPulsarScheme() {
        assertThat(mapper.toPulsarTopic("persistent://t/ns/orders"))
                .isEqualTo("persistent://t/ns/orders");
        assertThat(mapper.toPulsarTopic("non-persistent://t/ns/orders"))
                .isEqualTo("non-persistent://t/ns/orders");
    }

    @Test
    void extractsTheKafkaTopicNameFromAFullyQualifiedPulsarTopic() {
        assertThat(mapper.toKafkaTopic("persistent://public/default/orders")).isEqualTo("orders");
    }

    @Test
    void roundTripsAFlatTopicName() {
        String kafkaTopic = "events";
        assertThat(mapper.toKafkaTopic(mapper.toPulsarTopic(kafkaTopic))).isEqualTo(kafkaTopic);
    }

    @Test
    void honoursACustomTenantAndNamespace() {
        KafkaTopicMapper custom = new KafkaTopicMapper("acme", "prod");
        assertThat(custom.toPulsarTopic("orders")).isEqualTo("persistent://acme/prod/orders");
        assertThat(custom.tenant()).isEqualTo("acme");
        assertThat(custom.namespace()).isEqualTo("prod");
    }

    @Test
    void rejectsNullArguments() {
        assertThatNullPointerException().isThrownBy(() -> mapper.toPulsarTopic(null));
        assertThatNullPointerException().isThrownBy(() -> mapper.toKafkaTopic(null));
    }
}
