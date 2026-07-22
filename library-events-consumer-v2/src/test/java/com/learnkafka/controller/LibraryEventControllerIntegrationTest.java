package com.learnkafka.controller;

import com.learnkafka.TestcontainersConfiguration;
import com.learnkafka.entity.Book;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.entity.LibraryEventType;
import com.learnkafka.repository.BookRepository;
import com.learnkafka.repository.LibraryEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.context.ImportTestcontainers;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@ImportTestcontainers(TestcontainersConfiguration.class)
class LibraryEventControllerIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private LibraryEventRepository libraryEventRepository;

    @Autowired
    private BookRepository bookRepository;

    @BeforeEach
    void setUp() {
        bookRepository.deleteAll();
        libraryEventRepository.deleteAll();
    }

    // ── GET all ──────────────────────────────────────────────────────────────

    @Test
    void getAllLibraryEvents_shouldReturnEmptyList() throws Exception {
        mockMvc.perform(get("/v1/library-events"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.length()").value(0));
    }

    @Test
    void getAllLibraryEvents_shouldReturnAllLibraryEvents() throws Exception {
        persistLibraryEventWithBook(1, 101, "Clean Code", "Robert C. Martin");
        persistLibraryEventWithBook(2, 102, "The Pragmatic Programmer", "David Thomas");

        mockMvc.perform(get("/v1/library-events"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(2));
    }

    // ── GET by ID ─────────────────────────────────────────────────────────────

    @Test
    void getLibraryEventById_shouldReturnLibraryEvent() throws Exception {
        persistLibraryEventWithBook(1, 101, "Clean Code", "Robert C. Martin");

        mockMvc.perform(get("/v1/library-events/1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.libraryEventId").value(1))
                .andExpect(jsonPath("$.eventType").value("ADD"))
                .andExpect(jsonPath("$.bookId").value(101))
                .andExpect(jsonPath("$.createdAt").isNotEmpty())
                .andExpect(jsonPath("$.updatedAt").isNotEmpty());
    }

    @Test
    void getLibraryEventById_notFound_shouldReturn404() throws Exception {
        mockMvc.perform(get("/v1/library-events/999"))
                .andExpect(status().isNotFound());
    }

    // ── Helper ────────────────────────────────────────────────────────────────

    private void persistLibraryEventWithBook(Integer libraryEventId, Integer bookId,
                                             String bookName, String bookAuthor) {
        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(libraryEventId)
                .eventType(LibraryEventType.ADD)
                .build();
        LibraryEvent savedEvent = libraryEventRepository.save(libraryEvent);

        Book book = Book.builder()
                .bookId(bookId)
                .bookName(bookName)
                .bookAuthor(bookAuthor)
                .libraryEvent(savedEvent)
                .build();
        bookRepository.save(book);
    }
}
