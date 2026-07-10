package com.arshad.config;

import com.arshad.model.LibraryEvent;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Spring Kafka producer configuration.
 *
 * <p>Manually wires a {@link ProducerFactory} and {@link KafkaTemplate} with explicit
 * producer settings sourced from application properties, rather than relying on
 * Spring Boot's auto-configuration. This gives full control over serialization,
 * idempotence, acknowledgement mode, and batching behaviour.
 *
 * <p>Key settings:
 * <ul>
 *   <li>Key serializer: {@link org.apache.kafka.common.serialization.LongSerializer}</li>
 *   <li>Value serializer: {@link org.springframework.kafka.support.serializer.JsonSerializer}</li>
 *   <li>Acknowledgement: configurable via {@code spring.kafka.producer.acks}</li>
 *   <li>Idempotence: configurable via {@code spring.kafka.producer.properties.enable.idempotence}</li>
 * </ul>
 */
@Configuration
public class KafkaProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.producer.acks}")
    private String acks;

    @Value("${spring.kafka.producer.retries}")
    private Integer retries;

    @Value("${spring.kafka.producer.properties.enable.idempotence}")
    private Boolean enableIdempotence;

    @Value("${spring.kafka.producer.properties.linger.ms}")
    private Integer lingerMs;

    /**
     * Creates the Kafka {@link ProducerFactory} with all producer settings.
     *
     * @return a configured {@link DefaultKafkaProducerFactory}
     */
    @Bean
    public ProducerFactory<Long, LibraryEvent> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        configProps.put(ProducerConfig.ACKS_CONFIG, acks);
        configProps.put(ProducerConfig.RETRIES_CONFIG, retries);
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, enableIdempotence);
        configProps.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs);

        return new DefaultKafkaProducerFactory<>(configProps);
    }

    /**
     * Creates the {@link KafkaTemplate} used by producers to send messages.
     *
     * @return a {@link KafkaTemplate} backed by the configured {@link #producerFactory()}
     */
    @Bean
    public KafkaTemplate<Long, LibraryEvent> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}
