package com.learnkafka.mapper;

import com.learnkafka.entity.Book;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.entity.LibraryEventType;
import com.learnkafka.model.LibraryEventDto;
import org.springframework.stereotype.Component;

@Component
public class LibraryEventMapper {

    public LibraryEvent toEntity(LibraryEventDto dto) {
        LibraryEvent event = LibraryEvent.builder()
                .libraryEventId(dto.getLibraryEventId().intValue())
                .eventType(LibraryEventType.valueOf(dto.getEventType().name()))
                .build();

        Book book = Book.builder()
                .bookId(dto.getBook().getBookId().intValue())
                .bookName(dto.getBook().getBookName())
                .bookAuthor(dto.getBook().getBookAuthor())
                .libraryEvent(event)
                .build();

        event.setBook(book);
        return event;
    }

    public void updateEntity(LibraryEventDto dto, LibraryEvent existing) {
        existing.setEventType(LibraryEventType.valueOf(dto.getEventType().name()));
        existing.getBook().setBookName(dto.getBook().getBookName());
        existing.getBook().setBookAuthor(dto.getBook().getBookAuthor());
    }
}
