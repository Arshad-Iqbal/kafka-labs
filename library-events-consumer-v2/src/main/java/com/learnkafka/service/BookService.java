package com.learnkafka.service;

import com.learnkafka.dto.BookDto;
import com.learnkafka.dto.BookResponseDto;
import com.learnkafka.entity.Book;
import com.learnkafka.mapper.LibraryEventMapper;
import com.learnkafka.repository.BookRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

@Service
public class BookService {

    private static final Logger log = LoggerFactory.getLogger(BookService.class);

    private final BookRepository bookRepository;

    public BookService(BookRepository bookRepository) {
        this.bookRepository = bookRepository;
    }

    public List<BookResponseDto> findAll() {
        log.info("Fetching all books");
        return bookRepository.findAll()
                .stream()
                .map(LibraryEventMapper::toBookResponseDto)
                .toList();
    }

    public Optional<BookResponseDto> findById(Integer bookId) {
        log.info("Fetching book with id: {}", bookId);
        return bookRepository.findById(bookId)
                .map(LibraryEventMapper::toBookResponseDto);
    }

    public BookResponseDto create(BookDto dto) {
        log.info("Creating book: {}", dto);
        Book entity = LibraryEventMapper.toBookEntity(dto);
        Book saved = bookRepository.save(entity);
        log.info("Successfully created book with id: {}", saved.getBookId());
        return LibraryEventMapper.toBookResponseDto(saved);
    }

    @Transactional
    public Optional<BookResponseDto> update(Integer bookId, BookDto dto) {
        log.info("Updating book with id: {}", bookId);
        return bookRepository.findById(bookId)
                .map(existing -> {
                    existing.setBookName(dto.bookName());
                    existing.setBookAuthor(dto.bookAuthor());
                    Book updated = bookRepository.save(existing);
                    log.info("Successfully updated book with id: {}", updated.getBookId());
                    return LibraryEventMapper.toBookResponseDto(updated);
                });
    }

    @Transactional
    public boolean delete(Integer bookId) {
        log.info("Deleting book with id: {}", bookId);
        return bookRepository.findById(bookId)
                .map(book -> {
                    if (book.getLibraryEvent() != null) {
                        book.getLibraryEvent().setBook(null);
                    }
                    bookRepository.delete(book);
                    log.info("Successfully deleted book with id: {}", bookId);
                    return true;
                })
                .orElse(false);
    }
}
