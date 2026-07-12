package com.arshad.producer;

import com.arshad.exception.LibraryEventPublishException;
import com.arshad.model.LibraryEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Kafka producer responsible for publishing {@link LibraryEvent} messages to the configured topic.
 *
 * <p>Provides two publishing strategies:
 * <ul>
 *   <li><b>Asynchronous</b> ({@link #sendLibraryEvent}) — returns a {@code CompletableFuture}
 *       that callers can chain via {@code thenApply} / {@code exceptionally}; the calling thread
 *       is never blocked waiting for broker acknowledgement.</li>
 *   <li><b>Synchronous</b> ({@link #sendLibraryEventSynchronous}) — blocks the calling thread
 *       until the broker acknowledges the message (or a timeout / error occurs).</li>
 * </ul>
 *
 * <p>The target topic is resolved from the {@code spring.kafka.topic.name} application property,
 * defaulting to {@code library-events} if not set.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class LibraryEventProducer {

    private final KafkaTemplate<Long, LibraryEvent> kafkaTemplate;

    @Value("${spring.kafka.topic.name:library-events}")
    private String topicName;

    /**
     * Publishes a {@link LibraryEvent} to Kafka asynchronously.
     *
     * <p>The message is enqueued immediately. The returned {@code CompletableFuture} completes
     * with the broker {@link SendResult} on acknowledgement, or completes exceptionally if the
     * broker rejects the record. Callers should chain further work via {@code thenApply} and
     * handle failures via {@code exceptionally}.
     *
     * @param event the library event to publish; must not be {@code null}, and must have a
     *              non-null {@code libraryEventId} and {@code eventType}
     * @return a {@code CompletableFuture} that completes with the {@link SendResult} on success,
     *         or completes exceptionally if the broker rejects the message
     * @throws IllegalArgumentException     if {@code event}, its ID, or its type is {@code null}
     * @throws LibraryEventPublishException if the message cannot be enqueued synchronously
     *                                      (e.g. serialization failure before it reaches the broker)
     */
    public CompletableFuture<SendResult<Long, LibraryEvent>> sendLibraryEvent(LibraryEvent event) {

        log.info("Publishing {} event for libraryEventId: {}", event.getEventType(), event.getLibraryEventId());

        Message<LibraryEvent> message = MessageBuilder
                .withPayload(event)
                .setHeader(KafkaHeaders.TOPIC, topicName)
                .setHeader("kafka_messageKey", event.getLibraryEventId())
                .setHeader("eventType", event.getEventType().name())
                .build();

        CompletableFuture<SendResult<Long, LibraryEvent>> publishFuture;
        try {
            publishFuture = kafkaTemplate.send(message);
        } catch (RuntimeException ex) {
            log.error("Failed to enqueue {} event for libraryEventId: {}",
                    event.getEventType(), event.getLibraryEventId(), ex);
            throw new LibraryEventPublishException(
                    String.format("Failed to enqueue %s event for libraryEventId: %d", event.getEventType(), event.getLibraryEventId()),
                    ex
            );
        }

        return publishFuture.whenComplete((result, throwable) -> {
            if (throwable != null) {
                log.error("Failed to publish {} event for libraryEventId: {}", event.getEventType(), event.getLibraryEventId(), throwable);
                return;
            }
            log.info("Successfully published {} event for libraryEventId: {}", event.getEventType(), event.getLibraryEventId());
        });
    }

    /**
     * Publishes a {@link LibraryEvent} to Kafka synchronously, blocking until the broker
     * acknowledges the message or a timeout / error occurs.
     *
     * <p>The call blocks for up to 3 seconds waiting for broker acknowledgement. On success,
     * the partition and offset are logged for traceability.
     *
     * @param event the library event to publish; must not be {@code null}, and must have a
     *              non-null {@code libraryEventId} and {@code eventType}
     * @return the {@link SendResult} containing broker metadata (topic, partition, offset)
     * @throws IllegalArgumentException     if {@code event}, its ID, or its type is {@code null}
     * @throws LibraryEventPublishException if the broker rejects the message, the thread is
     *                                      interrupted, or the 3-second timeout elapses
     */
    public SendResult<Long, LibraryEvent> sendLibraryEventSynchronous(LibraryEvent event) {
        if (event == null) {
            throw new IllegalArgumentException("Event cannot be null");
        }
        if (event.getLibraryEventId() == null) {
            throw new IllegalArgumentException("Library event ID cannot be null");
        }
        if (event.getEventType() == null) {
            throw new IllegalArgumentException("Event type cannot be null");
        }

        log.info("Publishing {} event synchronously for libraryEventId: {}", event.getEventType(), event.getLibraryEventId());

        Message<LibraryEvent> message = MessageBuilder
                .withPayload(event)
                .setHeader(KafkaHeaders.TOPIC, topicName)
                .setHeader("kafka_messageKey", event.getLibraryEventId())
                .setHeader("eventType", event.getEventType().name())
                .build();

        try {
            SendResult<Long, LibraryEvent> result = kafkaTemplate.send(message).get(3, TimeUnit.SECONDS);
            log.info("Successfully published {} event synchronously for libraryEventId: {} - partition: {}, offset: {}",
                    event.getEventType(),
                    event.getLibraryEventId(),
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset());
            return result;
        } catch (ExecutionException ex) {
            log.error("Broker rejected or failed to process {} event for libraryEventId: {}",
                    event.getEventType(), event.getLibraryEventId(), ex.getCause());
            throw new LibraryEventPublishException(
                    String.format("Failed to publish %s event synchronously for libraryEventId: %d", event.getEventType(), event.getLibraryEventId()),
                    ex.getCause()
            );
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            log.error("Thread interrupted while waiting for broker acknowledgement for {} event for libraryEventId: {}",
                    event.getEventType(), event.getLibraryEventId(), ex);
            throw new LibraryEventPublishException(
                    String.format("Interrupted while publishing %s event synchronously for libraryEventId: %d", event.getEventType(), event.getLibraryEventId()),
                    ex
            );
        } catch (TimeoutException ex) {
            log.error("Timed out waiting for broker acknowledgement while publishing {} event for libraryEventId: {}",
                    event.getEventType(), event.getLibraryEventId(), ex);
            throw new LibraryEventPublishException(
                    String.format("Timed out publishing %s event synchronously for libraryEventId: %d", event.getEventType(), event.getLibraryEventId()),
                    ex
            );
        }
    }
}
