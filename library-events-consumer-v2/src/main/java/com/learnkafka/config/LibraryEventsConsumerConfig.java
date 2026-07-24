package com.learnkafka.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.entity.LibraryEventConsumerFailure;
import com.learnkafka.repository.LibraryEventConsumerFailureRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.*;
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

    private static final String DLT_TOPIC = "library-events.DLT";

    /**
     * Configures exponential backoff retry behaviour for the Kafka listener error handler.
     *
     * <p>When {@code app.kafka.recovery.mode=dlt}, exhausted records are forwarded to
     * {@value DLT_TOPIC} via a {@link DeadLetterPublishingRecoverer}. Otherwise the record
     * is simply skipped after retries are exhausted.
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
     *   Stop    │  record forwarded to DLT (if mode=dlt) or skipped
     * </pre>
     */
    @Bean
    public DefaultErrorHandler defaultErrorHandler(
            @Value("${app.kafka.recovery.mode:none}") String recoveryMode,
            KafkaTemplate<Object, Object> kafkaTemplate,
            LibraryEventConsumerFailureRepository libraryEventFailureRepository) {

        var backOff = new ExponentialBackOff(1000L, 2.0);
        backOff.setMaxAttempts(2);

        ConsumerRecordRecoverer recoverer = switch (recoveryMode.toLowerCase()) {
            case "dlt" -> {
                log.info("Recovery mode=dlt — exhausted records will be forwarded to {}", DLT_TOPIC);
                yield new DeadLetterPublishingRecoverer(kafkaTemplate,
                        (record, ex) -> new TopicPartition(DLT_TOPIC, record.partition()));
            }
            /**
             * BOTH recovery mode:
             *
             * After all retry attempts are exhausted, a custom ConsumerRecordRecoverer
             * performs two recovery actions for the failed record:
             *
             * 1. Delegates to DeadLetterPublishingRecoverer to publish the record to the DLT.
             * 2. Persists the failed record and exception details to the failure table.
             *
             * The lambda itself acts as the ConsumerRecordRecoverer passed to
             * DefaultErrorHandler. When recovery is triggered, DefaultErrorHandler invokes
             * this recoverer's accept(record, exception) method.
             *
             * Note: Publishing to Kafka and saving to the database are two independent
             * operations and are not atomic in this implementation.
             */
            case "both" -> {
                log.info("Recovery mode=both — exhausted records will be forwarded to {} AND persisted to library_event_consumer_failure table", DLT_TOPIC);
                var dltRecoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                        (record, ex) -> new TopicPartition(DLT_TOPIC, record.partition()));
                yield (record, ex) -> {
                    dltRecoverer.accept(record, ex);
                    log.info("DeadLetterPublishingRecoverer completed — now persisting to library_event_consumer_failure table");
                    var failure = new LibraryEventConsumerFailure(
                            record.topic(),
                            record.partition(),
                            record.offset(),
                            record.key() != null ? record.key().toString() : null,
                            record.value() != null ? record.value().toString() : null,
                            ex.getClass().getName(),
                            ex.getMessage()
                    );
                    libraryEventFailureRepository.save(failure);
                    log.info("Persisted consumer failure record: {}", failure);
                };
            }
            case "failure-table" -> {
                log.info("Recovery mode=failure-table — exhausted records will be persisted to library_event_consumer_failure table");
                yield (record, ex) -> {
                    log.error("Persisting failed record to library_event_consumer_failure table. Topic={}, Partition={}, Offset={}",
                            record.topic(), record.partition(), record.offset(), ex);
                    var failure = new LibraryEventConsumerFailure(
                            record.topic(),
                            record.partition(),
                            record.offset(),
                            record.key() != null ? record.key().toString() : null,
                            record.value() != null ? record.value().toString() : null,
                            ex.getClass().getName(),
                            ex.getMessage()
                    );
                    libraryEventFailureRepository.save(failure);
                    log.info("Persisted consumer failure record: {}", failure);
                };
            }
            case "log_skip" -> {
                log.info("Recovery mode=log_skip — exhausted records will be logged and skipped");
                yield (ConsumerRecordRecoverer) (record, ex) ->
                        log.error("Recovery: skipping failed record. Topic={}, Partition={}, Offset={}, Exception={}",
                                record.topic(), record.partition(), record.offset(), ex.getMessage());
            }
            default -> {
                log.info("Recovery mode={} — exhausted records will be skipped (no recovery)", recoveryMode);
                yield null;
            }
        };

        var errorHandler = recoverer != null
                ? new DefaultErrorHandler(recoverer, backOff)
                : new DefaultErrorHandler(backOff);

        errorHandler.setRetryListeners(retryListener());
        errorHandler.addNotRetryableExceptions(
                IllegalArgumentException.class,
                NullPointerException.class,
                DataIntegrityViolationException.class
        );
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

