package com.learnkafka.mapper;

import com.learnkafka.dto.BookDto;
import com.learnkafka.dto.BookResponseDto;
import com.learnkafka.dto.LibraryEventResponseDto;
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

    public static Book toBookEntity(BookDto dto) {
        return Book.builder()
                .bookId(dto.bookId())
                .bookName(dto.bookName())
                .bookAuthor(dto.bookAuthor())
                .build();
    }

    public static BookResponseDto toBookResponseDto(Book book) {
        Integer libraryEventId = book.getLibraryEvent() != null
                ? book.getLibraryEvent().getLibraryEventId()
                : null;
        return new BookResponseDto(
                book.getBookId(),
                book.getBookName(),
                book.getBookAuthor(),
                libraryEventId,
                book.getCreatedAt(),
                book.getUpdatedAt()
        );
    }

    public static LibraryEventResponseDto toLibraryEventResponseDto(LibraryEvent event) {
        Integer bookId = event.getBook() != null ? event.getBook().getBookId() : null;
        return new LibraryEventResponseDto(
                event.getLibraryEventId(),
                event.getEventType(),
                bookId,
                event.getCreatedAt(),
                event.getUpdatedAt()
        );
    }
}
