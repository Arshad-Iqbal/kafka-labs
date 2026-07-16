package com.learnkafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.model.LibraryEventDto;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class LibraryEventService {

    private static final Logger log = LoggerFactory.getLogger(LibraryEventService.class);

    private final ObjectMapper objectMapper;

    public LibraryEventService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public void processEvent(ConsumerRecord<Long, String> consumerRecord) {
        try {
            LibraryEventDto libraryEventDto = objectMapper.readValue(consumerRecord.value(), LibraryEventDto.class);
            log.info("Deserialized libraryEventDto={}", libraryEventDto);
        } catch (JsonProcessingException e) {
            log.error("Failed to deserialize ConsumerRecord value: {}, error: {}", consumerRecord.value(), e.getMessage(), e);
        }
    }
}
