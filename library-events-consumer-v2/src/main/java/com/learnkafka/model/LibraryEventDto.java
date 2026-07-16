package com.learnkafka.model;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Represents a library event to be published to Kafka.
 *
 * <p>Each event carries a unique identifier, an {@link EventType} indicating whether the
 * event is a new entry ({@code ADD}) or a modification ({@code UPDATE}), and the associated
 * {@link BookDto} details.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class LibraryEventDto {

    /** Unique identifier for this library event. Must be a positive number. */
    @NotNull(message = "libraryEventId cannot be null")
    @Positive(message = "libraryEventId must be a positive number")
    private Long libraryEventId;

    /** Indicates whether this event represents a new book entry or an update. */
    @NotNull(message = "eventType cannot be null")
    private EventType eventType;

    /** The book associated with this library event. */
    @NotNull(message = "book cannot be null")
    @Valid
    private BookDto book;
}
