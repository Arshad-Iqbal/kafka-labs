package com.arshad.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class LibraryEvent {
    private Integer libraryEventId;
    private EventType eventType;
    private Book book;
}
