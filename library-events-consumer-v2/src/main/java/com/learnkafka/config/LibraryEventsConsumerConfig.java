package com.learnkafka.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.util.backoff.ExponentialBackOff;

@Configuration
@EnableKafka
public class LibraryEventsConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(LibraryEventsConsumerConfig.class);

    @Bean
    ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    @Bean
    KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, String>>
            kafkaListenerContainerFactory(ConsumerFactory<Integer, String> consumerFactory,
                                          DefaultErrorHandler defaultErrorHandler) {

        var factory = new ConcurrentKafkaListenerContainerFactory<Integer, String>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(defaultErrorHandler);

        //factory.setConcurrency(3); // Set concurrency to 3 — allows processing messages from 3 partitions concurrently.

        // Set acknowledgement mode to MANUAL — offsets are committed to Kafka on the next scheduled
        // interval only after acknowledgment.acknowledge() is explicitly called in the listener.
        // Default AckMode is BATCH — offsets are auto-committed after each poll batch is fully processed,
        // without requiring any explicit acknowledgment call in the listener.
        //factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
    }

    /**
     * Configures exponential backoff retry behaviour for the Kafka listener error handler.
     *
     * <p><b>Spring Kafka 4.x note:</b> {@code ExponentialBackOffWithMaxRetries} was removed as part
     * of a major simplification of the retry architecture in Spring Kafka 4.x. The framework dropped
     * its dependency on {@code spring-retry} entirely in favour of core Spring Framework 7 mechanisms,
     * and custom convenience subclasses like {@code ExponentialBackOffWithMaxRetries} were cleaned up
     * along with it.
     *
     * <p>The replacement is the standard {@link ExponentialBackOff} from {@code spring-core}.
     * By default it retries indefinitely; use {@code setMaxAttempts} (added in Spring Framework 6.1)
     * to bound retries directly — which is equivalent to the maximum number of retries
     * <em>excluding</em> the original invocation.
     *
     * <p>Retry schedule (initialInterval=1s, multiplier=2.0, maxAttempts=2):
     * <pre>
     *   Attempt │ Wait
     *   ────────┼──────
     *   Retry 1 │  1 s
     *   Retry 2 │  2 s
     *   Stop    │  record skipped
     * </pre>
     */
    @Bean
    public DefaultErrorHandler defaultErrorHandler() {
        var backOff = new ExponentialBackOff(1000L, 2.0);
        backOff.setMaxAttempts(2);

        var errorHandler = new DefaultErrorHandler(backOff);
        // Previous lambda-based RetryListener (kept for reference):
        // errorHandler.setRetryListeners((record, ex, deliveryAttempt) -> {
        //     var message = String.format(
        //             "Retry attempt %d failed for topic=%s partition=%d offset=%d: %s",
        //             deliveryAttempt,
        //             record.topic(),
        //             record.partition(),
        //             record.offset(),
        //             ex.getMessage());
        //     log.warn(message, ex);
        // });
        errorHandler.setRetryListeners(retryListener());
        return errorHandler;
    }

    @Bean
    public RetryListener retryListener() {
        return new RetryListener() {
            @Override
            public void failedDelivery(ConsumerRecord<?, ?> record, Exception ex, int deliveryAttempt) {
                log.warn("Retry attempt {} failed — topic: {}, partition: {}, offset: {}, error: {}",
                        deliveryAttempt, record.topic(), record.partition(), record.offset(), ex.getMessage(), ex);
            }
        };
    }
}

