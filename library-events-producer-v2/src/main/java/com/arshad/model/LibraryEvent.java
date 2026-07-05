package com.arshad.model;

import com.arshad.validator.ValidEventType;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class LibraryEvent {
    
    @NotNull(message = "libraryEventId cannot be null")
    @Positive(message = "libraryEventId must be a positive number")
    private Long libraryEventId;
    
    @NotNull(message = "eventType cannot be null")
    @ValidEventType(message = "eventType must be a valid enum value (ADD or UPDATE)")
    private EventType eventType;
    
    @NotNull(message = "book cannot be null")
    @Valid
    private Book book;
}
