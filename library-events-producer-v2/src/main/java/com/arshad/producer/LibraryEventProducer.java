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

@Component
@RequiredArgsConstructor
@Slf4j
public class LibraryEventProducer {

    private final KafkaTemplate<Long, LibraryEvent> kafkaTemplate;

    @Value("${kafka.topic.library-events:library-events}")
    private String topicName;

    public void sendLibraryEvent(LibraryEvent event) {
        if (event == null) {
            throw new IllegalArgumentException("Event cannot be null");
        }
        if (event.getLibraryEventId() == null) {
            throw new IllegalArgumentException("Library event ID cannot be null");
        }
        if (event.getEventType() == null) {
            throw new IllegalArgumentException("Event type cannot be null");
        }

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
            throw new LibraryEventPublishException(
                    String.format("Failed to enqueue %s event for libraryEventId: %d", event.getEventType(), event.getLibraryEventId()),
                    ex
            );
        }

        publishFuture.whenComplete((result, throwable) -> {
            if (throwable != null) {
                log.error("Failed to publish {} event for libraryEventId: {}", event.getEventType(), event.getLibraryEventId(), throwable);
                return;
            }
            log.info("Successfully published {} event for libraryEventId: {}", event.getEventType(), event.getLibraryEventId());
        });
    }
}
