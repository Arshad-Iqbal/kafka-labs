package com.learnkafka.controller;

import com.learnkafka.TestcontainersConfiguration;
import com.learnkafka.dto.BookDto;
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
import tools.jackson.databind.ObjectMapper;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@ImportTestcontainers(TestcontainersConfiguration.class)
class BookControllerIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private BookRepository bookRepository;

    @Autowired
    private LibraryEventRepository libraryEventRepository;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        bookRepository.deleteAll();
        libraryEventRepository.deleteAll();
    }

    // ── GET all ──────────────────────────────────────────────────────────────

    @Test
    void getAllBooks_shouldReturnEmptyList() throws Exception {
        mockMvc.perform(get("/v1/books"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.length()").value(0));
    }

    @Test
    void getAllBooks_shouldReturnAllBooks() throws Exception {
        persistBookWithLibraryEvent(101, 1, "Clean Code", "Robert C. Martin");
        persistBookWithLibraryEvent(102, 2, "The Pragmatic Programmer", "David Thomas");

        mockMvc.perform(get("/v1/books"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(2));
    }

    // ── GET by ID ─────────────────────────────────────────────────────────────

    @Test
    void getBookById_shouldReturnBook() throws Exception {
        persistBookWithLibraryEvent(101, 1, "Clean Code", "Robert C. Martin");

        mockMvc.perform(get("/v1/books/1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.bookId").value(1))
                .andExpect(jsonPath("$.bookName").value("Clean Code"))
                .andExpect(jsonPath("$.bookAuthor").value("Robert C. Martin"))
                .andExpect(jsonPath("$.libraryEventId").isNotEmpty())
                .andExpect(jsonPath("$.createdAt").isNotEmpty())
                .andExpect(jsonPath("$.updatedAt").isNotEmpty());
    }

    @Test
    void getBookById_notFound_shouldReturn404() throws Exception {
        mockMvc.perform(get("/v1/books/999"))
                .andExpect(status().isNotFound());
    }

    // ── POST (create) ─────────────────────────────────────────────────────────

    @Test
    void createBook_shouldPersistAndReturn201() throws Exception {
        BookDto dto = new BookDto(10, "Domain-Driven Design", "Eric Evans");

        mockMvc.perform(post("/v1/books")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(dto)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.bookId").value(10))
                .andExpect(jsonPath("$.bookName").value("Domain-Driven Design"))
                .andExpect(jsonPath("$.bookAuthor").value("Eric Evans"))
                .andExpect(jsonPath("$.libraryEventId").isEmpty())
                .andExpect(jsonPath("$.createdAt").isNotEmpty())
                .andExpect(jsonPath("$.updatedAt").isNotEmpty());
    }

    @Test
    void createBook_invalidPayload_shouldReturn400() throws Exception {
        BookDto dto = new BookDto(null, "", "");

        mockMvc.perform(post("/v1/books")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(dto)))
                .andExpect(status().isBadRequest());
    }

    // ── PUT (update) ──────────────────────────────────────────────────────────

    @Test
    void updateBook_shouldUpdateAndReturn200() throws Exception {
        persistBookWithLibraryEvent(101, 1, "Clean Code", "Robert C. Martin");
        BookDto updateDto = new BookDto(1, "Clean Code (2nd Ed)", "Robert C. Martin");

        mockMvc.perform(put("/v1/books/1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(updateDto)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.bookName").value("Clean Code (2nd Ed)"))
                .andExpect(jsonPath("$.bookAuthor").value("Robert C. Martin"));
    }

    @Test
    void updateBook_notFound_shouldReturn404() throws Exception {
        BookDto updateDto = new BookDto(999, "Unknown", "Unknown Author");

        mockMvc.perform(put("/v1/books/999")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(updateDto)))
                .andExpect(status().isNotFound());
    }

    // ── DELETE ────────────────────────────────────────────────────────────────

    @Test
    void deleteBook_shouldDeleteAndReturn204() throws Exception {
        persistBookWithLibraryEvent(101, 1, "Clean Code", "Robert C. Martin");

        mockMvc.perform(delete("/v1/books/1"))
                .andExpect(status().isNoContent());
    }

    @Test
    void deleteBook_notFound_shouldReturn404() throws Exception {
        mockMvc.perform(delete("/v1/books/999"))
                .andExpect(status().isNotFound());
    }

    // ── Helper ────────────────────────────────────────────────────────────────

    private void persistBookWithLibraryEvent(Integer libraryEventId, Integer bookId, String bookName, String bookAuthor) {
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
