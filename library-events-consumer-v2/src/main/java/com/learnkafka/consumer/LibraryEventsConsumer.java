package com.learnkafka.consumer;

import com.learnkafka.model.LibraryEventDto;
import com.learnkafka.service.LibraryEventService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class LibraryEventsConsumer {

    private static final Logger log = LoggerFactory.getLogger(LibraryEventsConsumer.class);

    private final LibraryEventService libraryEventService;

    public LibraryEventsConsumer(LibraryEventService libraryEventService) {
        this.libraryEventService = libraryEventService;
    }

    // Test -> Event to Kafka topic "library-events" -> onMessage -> libraryEventService.processEvent -> DB
    // Check the DB and the event is persisted
    // Test for ADD, UPDATE
    // Test for Invalid Events
    // Pass the book as null
    // Update Event Type with null libraryEventId

    @KafkaListener(topics = "library-events")
    public void onMessage(ConsumerRecord<Long, LibraryEventDto> consumerRecord) {
        //public void onMessage(ConsumerRecord<Long, String> consumerRecord, Acknowledgment acknowledgment) {
        log.info("Event received: topic={}, partition={}, offset={}, key={}, value={}",
                consumerRecord.topic(),
                consumerRecord.partition(),
                consumerRecord.offset(),
                consumerRecord.key(),
                consumerRecord.value());
        libraryEventService.processEvent(consumerRecord);
        // Manually commit the offset to Kafka after successful processing.
        // With AckMode.MANUAL, the commit is batched and sent on the next scheduled interval.
        // Default AckMode is BATCH — offsets are committed automatically after each poll batch is processed.
        //acknowledgment.acknowledge();
    }
}
