package com.learnkafka.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;

@Configuration
@EnableKafka
public class LibraryEventsConsumerConfig {

    @Bean
    ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    @Bean
    KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, String>>
            kafkaListenerContainerFactory(ConsumerFactory<Integer, String> consumerFactory) {

        var factory = new ConcurrentKafkaListenerContainerFactory<Integer, String>();
        factory.setConsumerFactory(consumerFactory);

        //factory.setConcurrency(3); // Set concurrency to 3 — allows processing messages from 3 partitions concurrently.

        // Set acknowledgement mode to MANUAL — offsets are committed to Kafka on the next scheduled
        // interval only after acknowledgment.acknowledge() is explicitly called in the listener.
        // Default AckMode is BATCH — offsets are auto-committed after each poll batch is fully processed,
        // without requiring any explicit acknowledgment call in the listener.
        //factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
    }
}
