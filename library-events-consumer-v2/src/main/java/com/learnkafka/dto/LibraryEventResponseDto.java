package com.learnkafka.dto;

import com.learnkafka.entity.LibraryEventType;

import java.time.LocalDateTime;

public record LibraryEventResponseDto(
        Integer libraryEventId,
        LibraryEventType eventType,
        Integer bookId,
        LocalDateTime createdAt,
        LocalDateTime updatedAt
) {
}
