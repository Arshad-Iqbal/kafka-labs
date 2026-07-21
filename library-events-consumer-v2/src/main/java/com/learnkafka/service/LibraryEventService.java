package com.learnkafka.service;

import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.mapper.LibraryEventMapper;
import com.learnkafka.model.EventType;
import com.learnkafka.model.LibraryEventDto;
import com.learnkafka.repository.LibraryEventRepository;
import jakarta.transaction.Transactional;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Set;
import java.util.stream.Collectors;

@Service
public class LibraryEventService {

    private static final Logger log = LoggerFactory.getLogger(LibraryEventService.class);

    private final LibraryEventRepository libraryEventRepository;
    private final LibraryEventMapper libraryEventMapper;
    private final Validator validator;

    public LibraryEventService(LibraryEventRepository libraryEventRepository,
                                LibraryEventMapper libraryEventMapper,
                                Validator validator) {
        this.libraryEventRepository = libraryEventRepository;
        this.libraryEventMapper = libraryEventMapper;
        this.validator = validator;
    }

    @Transactional
    public void processEvent(ConsumerRecord<Long, LibraryEventDto> consumerRecord) {
        LibraryEventDto dto = consumerRecord.value();
        log.info("Processing {} event: {}", dto.getEventType(), dto);

        validateDto(dto);
        validateConditionalRules(dto);

        switch (dto.getEventType()) {
            case ADD -> handleAdd(dto);
            case UPDATE -> handleUpdate(dto);
            default -> {
                log.warn("Unsupported event type: {}", dto.getEventType());
                throw new IllegalArgumentException("Unsupported event type: " + dto.getEventType());
            }
        }
    }

    private void handleAdd(LibraryEventDto dto) {
        LibraryEvent entity = libraryEventMapper.toEntity(dto);
        libraryEventRepository.save(entity);
        log.info("Saved new LibraryEvent with id={}", entity.getLibraryEventId());
    }

    private void handleUpdate(LibraryEventDto dto) {
        if (dto.getLibraryEventId() == null) {
            throw new IllegalArgumentException("libraryEventId is required for UPDATE event");
        }

        LibraryEvent existing = libraryEventRepository.findById(dto.getLibraryEventId().intValue())
                .orElseThrow(() -> new IllegalArgumentException(
                        "LibraryEvent not found for id: " + dto.getLibraryEventId()));

        libraryEventMapper.updateEntity(dto, existing);
        libraryEventRepository.save(existing);
        log.info("Updated LibraryEvent with id={}", existing.getLibraryEventId());
    }

    private void validateConditionalRules(LibraryEventDto dto) {
        if (dto.getBook() == null) {
            log.error("Conditional validation failed: book is null. dto={}", dto);
            throw new IllegalArgumentException("book is required");
        }
        if (dto.getEventType() == EventType.UPDATE && dto.getLibraryEventId() == null) {
            log.error("Conditional validation failed: UPDATE event missing libraryEventId. dto={}", dto);
            throw new IllegalArgumentException("libraryEventId is required for UPDATE event");
        }
    }

    private void validateDto(LibraryEventDto dto) {
        Set<ConstraintViolation<LibraryEventDto>> violations = validator.validate(dto);
        if (!violations.isEmpty()) {
            String message = violations.stream()
                    .map(v -> v.getPropertyPath() + " " + v.getMessage())
                    .collect(Collectors.joining(", "));
            log.error("Bean validation failed for library event. dto={}, errors={}", dto, message);
            throw new IllegalArgumentException("Validation failed: " + message);
        }
    }
}
