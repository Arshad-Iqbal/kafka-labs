package com.arshad.controller;

import com.arshad.exception.LibraryEventPublishException;
import com.arshad.model.Book;
import com.arshad.model.EventType;
import com.arshad.model.LibraryEvent;
import com.arshad.service.LibraryEventService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import tools.jackson.databind.ObjectMapper;

import java.util.concurrent.CompletableFuture;

import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * Web-layer slice tests for {@link LibraryEventController} using {@code @WebMvcTest}.
 *
 * <p>In Spring Boot 4.x, {@code @WebMvcTest} moved to a new artifact
 * ({@code spring-boot-webmvc-test}) and package
 * ({@code org.springframework.boot.webmvc.test.autoconfigure}).
 * It still loads only the web layer — controllers, {@code @ControllerAdvice}, filters,
 * {@code WebMvcConfigurer}, and validation — without Kafka or any other infrastructure.
 *
 * <p>{@code @MockitoBean} (Spring Framework 7.x) replaces the old {@code @MockBean}
 * (Spring Boot 3.x).
 *
 * <p>Because the controller returns {@link CompletableFuture}, success-path and service-error
 * tests follow the two-step async-dispatch pattern:
 * <ol>
 *   <li>Perform the request and assert that async processing started.</li>
 *   <li>Dispatch the async result and assert the final HTTP response.</li>
 * </ol>
 * Validation and event-type mismatch tests throw exceptions synchronously (before any future
 * is returned), so they use a single {@code perform()} call.
 */
@WebMvcTest(LibraryEventController.class)
@DisplayName("LibraryEventController Slice Tests (@WebMvcTest)")
class LibraryEventControllerSliceTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockitoBean
    private LibraryEventService libraryEventService;

    // -------------------------------------------------------------------------
    // POST /v1/library-events
    // -------------------------------------------------------------------------

    @Nested
    @DisplayName("POST /v1/library-events")
    class CreateLibraryEvent {

        @Test
        @DisplayName("valid ADD event returns 201 Created with Location header and event body")
        void givenValidAddEvent_thenReturns201WithLocationAndBody() throws Exception {
            LibraryEvent event = validAddEvent();
            when(libraryEventService.createLibraryEvent(any()))
                    .thenReturn(CompletableFuture.completedFuture(event));

            MvcResult async = mockMvc.perform(post("/v1/library-events")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(event)))
                    .andExpect(request().asyncStarted())
                    .andReturn();

            mockMvc.perform(asyncDispatch(async))
                    .andExpect(status().isCreated())
                    .andExpect(header().string("Location", "/v1/library-events/1"))
                    .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                    .andExpect(jsonPath("$.libraryEventId").value(1))
                    .andExpect(jsonPath("$.eventType").value("ADD"))
                    .andExpect(jsonPath("$.book.bookId").value(10))
                    .andExpect(jsonPath("$.book.bookName").value("Kafka: The Definitive Guide"))
                    .andExpect(jsonPath("$.book.bookAuthor").value("Neha Narkhede"));
        }

        @Test
        @DisplayName("valid ADD event delegates to libraryEventService.createLibraryEvent")
        void givenValidAddEvent_thenDelegatesToService() throws Exception {
            LibraryEvent event = validAddEvent();
            when(libraryEventService.createLibraryEvent(any()))
                    .thenReturn(CompletableFuture.completedFuture(event));

            MvcResult async = mockMvc.perform(post("/v1/library-events")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(event)))
                    .andExpect(request().asyncStarted())
                    .andReturn();

            mockMvc.perform(asyncDispatch(async)).andExpect(status().isCreated());
            verify(libraryEventService, times(1)).createLibraryEvent(any());
        }

        @Test
        @DisplayName("UPDATE event type on POST endpoint returns 400 Bad Request")
        void givenUpdateEventTypeOnPost_thenReturns400() throws Exception {
            LibraryEvent event = new LibraryEvent(1L, EventType.UPDATE, validBook());

            mockMvc.perform(post("/v1/library-events")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(event)))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.status").value(400))
                    .andExpect(jsonPath("$.errors[0]", containsString("POST endpoint only accepts ADD events")));

            verify(libraryEventService, never()).createLibraryEvent(any());
        }

        @Test
        @DisplayName("null libraryEventId returns 400 with validation error")
        void givenNullLibraryEventId_thenReturns400() throws Exception {
            LibraryEvent event = new LibraryEvent(null, EventType.ADD, validBook());

            mockMvc.perform(post("/v1/library-events")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(event)))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.status").value(400))
                    .andExpect(jsonPath("$.errors", hasItem(containsString("libraryEventId"))));

            verify(libraryEventService, never()).createLibraryEvent(any());
        }

        @Test
        @DisplayName("negative libraryEventId returns 400 with validation error")
        void givenNegativeLibraryEventId_thenReturns400() throws Exception {
            LibraryEvent event = new LibraryEvent(-1L, EventType.ADD, validBook());

            mockMvc.perform(post("/v1/library-events")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(event)))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.status").value(400))
                    .andExpect(jsonPath("$.errors", hasItem(containsString("libraryEventId"))));

            verify(libraryEventService, never()).createLibraryEvent(any());
        }

        @Test
        @DisplayName("null book returns 400 with validation error")
        void givenNullBook_thenReturns400() throws Exception {
            LibraryEvent event = new LibraryEvent(1L, EventType.ADD, null);

            mockMvc.perform(post("/v1/library-events")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(event)))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.status").value(400))
                    .andExpect(jsonPath("$.errors", hasItem(containsString("book"))));

            verify(libraryEventService, never()).createLibraryEvent(any());
        }

        @Test
        @DisplayName("empty bookName returns 400 with validation error")
        void givenEmptyBookName_thenReturns400() throws Exception {
            LibraryEvent event = new LibraryEvent(1L, EventType.ADD,
                    new Book(10L, "", "Neha Narkhede"));

            mockMvc.perform(post("/v1/library-events")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(event)))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.status").value(400))
                    .andExpect(jsonPath("$.errors", hasItem(containsString("bookName"))));

            verify(libraryEventService, never()).createLibraryEvent(any());
        }

        @Test
        @DisplayName("empty bookAuthor returns 400 with validation error")
        void givenEmptyBookAuthor_thenReturns400() throws Exception {
            LibraryEvent event = new LibraryEvent(1L, EventType.ADD,
                    new Book(10L, "Kafka: The Definitive Guide", ""));

            mockMvc.perform(post("/v1/library-events")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(event)))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.status").value(400))
                    .andExpect(jsonPath("$.errors", hasItem(containsString("bookAuthor"))));

            verify(libraryEventService, never()).createLibraryEvent(any());
        }

        @Test
        @DisplayName("bookName exceeding 255 characters returns 400")
        void givenBookNameExceeds255Chars_thenReturns400() throws Exception {
            LibraryEvent event = new LibraryEvent(1L, EventType.ADD,
                    new Book(10L, "A".repeat(256), "Neha Narkhede"));

            mockMvc.perform(post("/v1/library-events")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(event)))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.status").value(400))
                    .andExpect(jsonPath("$.errors", hasItem(containsString("bookName"))));

            verify(libraryEventService, never()).createLibraryEvent(any());
        }

        @Test
        @DisplayName("multiple validation violations are returned together in a single response")
        void givenMultipleInvalidFields_thenReturnsAllErrorsAtOnce() throws Exception {
            LibraryEvent event = new LibraryEvent(null, EventType.ADD,
                    new Book(null, "", ""));

            mockMvc.perform(post("/v1/library-events")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(event)))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.errors", hasSize(greaterThan(1))));

            verify(libraryEventService, never()).createLibraryEvent(any());
        }

        @Test
        @DisplayName("malformed JSON body returns 400 Bad Request")
        void givenMalformedJson_thenReturns400() throws Exception {
            mockMvc.perform(post("/v1/library-events")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content("{invalid-json}"))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.status").value(400));

            verify(libraryEventService, never()).createLibraryEvent(any());
        }

        @Test
        @DisplayName("empty request body returns 400 Bad Request")
        void givenEmptyBody_thenReturns400() throws Exception {
            mockMvc.perform(post("/v1/library-events")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(""))
                    .andExpect(status().isBadRequest());

            verify(libraryEventService, never()).createLibraryEvent(any());
        }

        @Test
        @DisplayName("service publish failure returns 500 Internal Server Error")
        void givenServicePublishFailure_thenReturns500() throws Exception {
            LibraryEvent event = validAddEvent();
            when(libraryEventService.createLibraryEvent(any()))
                    .thenReturn(CompletableFuture.failedFuture(
                            new LibraryEventPublishException("Kafka broker unavailable", new RuntimeException())));

            MvcResult async = mockMvc.perform(post("/v1/library-events")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(event)))
                    .andExpect(request().asyncStarted())
                    .andReturn();

            mockMvc.perform(asyncDispatch(async))
                    .andExpect(status().isInternalServerError())
                    .andExpect(jsonPath("$.status").value(500))
                    .andExpect(jsonPath("$.errors[0]", containsString("Kafka broker unavailable")));
        }

        @Test
        @DisplayName("service throws synchronously returns 500 Internal Server Error")
        void givenServiceThrowsSynchronously_thenReturns500() throws Exception {
            LibraryEvent event = validAddEvent();
            when(libraryEventService.createLibraryEvent(any()))
                    .thenThrow(new RuntimeException("Unexpected service failure"));

            mockMvc.perform(post("/v1/library-events")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(event)))
                    .andExpect(status().isInternalServerError())
                    .andExpect(jsonPath("$.status").value(500));
        }

        @Test
        @DisplayName("service returns failed future with unexpected exception returns 500")
        void givenServiceFailedFutureWithUnexpectedException_thenReturns500() throws Exception {
            LibraryEvent event = validAddEvent();
            when(libraryEventService.createLibraryEvent(any()))
                    .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Unexpected async failure")));

            MvcResult async = mockMvc.perform(post("/v1/library-events")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(event)))
                    .andExpect(request().asyncStarted())
                    .andReturn();

            mockMvc.perform(asyncDispatch(async))
                    .andExpect(status().isInternalServerError())
                    .andExpect(jsonPath("$.status").value(500));
        }
    }

    // -------------------------------------------------------------------------
    // PUT /v1/library-events
    // -------------------------------------------------------------------------

    @Nested
    @DisplayName("PUT /v1/library-events")
    class UpdateLibraryEvent {

        @Test
        @DisplayName("valid UPDATE event returns 200 OK with event body")
        void givenValidUpdateEvent_thenReturns200WithBody() throws Exception {
            LibraryEvent event = validUpdateEvent();
            when(libraryEventService.updateLibraryEvent(any()))
                    .thenReturn(CompletableFuture.completedFuture(event));

            MvcResult async = mockMvc.perform(put("/v1/library-events")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(event)))
                    .andExpect(request().asyncStarted())
                    .andReturn();

            mockMvc.perform(asyncDispatch(async))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.libraryEventId").value(1))
                    .andExpect(jsonPath("$.eventType").value("UPDATE"))
                    .andExpect(jsonPath("$.book.bookId").value(10));
        }

        @Test
        @DisplayName("valid UPDATE event delegates to libraryEventService.updateLibraryEvent")
        void givenValidUpdateEvent_thenDelegatesToService() throws Exception {
            LibraryEvent event = validUpdateEvent();
            when(libraryEventService.updateLibraryEvent(any()))
                    .thenReturn(CompletableFuture.completedFuture(event));

            MvcResult async = mockMvc.perform(put("/v1/library-events")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(event)))
                    .andExpect(request().asyncStarted())
                    .andReturn();

            mockMvc.perform(asyncDispatch(async)).andExpect(status().isOk());
            verify(libraryEventService, times(1)).updateLibraryEvent(any());
        }

        @Test
        @DisplayName("ADD event type on PUT endpoint returns 400 Bad Request")
        void givenAddEventTypeOnPut_thenReturns400() throws Exception {
            LibraryEvent event = new LibraryEvent(1L, EventType.ADD, validBook());

            mockMvc.perform(put("/v1/library-events")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(event)))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.status").value(400))
                    .andExpect(jsonPath("$.errors[0]", containsString("PUT endpoint only accepts UPDATE events")));

            verify(libraryEventService, never()).updateLibraryEvent(any());
        }

        @Test
        @DisplayName("null libraryEventId returns 400 with validation error")
        void givenNullLibraryEventId_thenReturns400() throws Exception {
            LibraryEvent event = new LibraryEvent(null, EventType.UPDATE, validBook());

            mockMvc.perform(put("/v1/library-events")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(event)))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.errors", hasItem(containsString("libraryEventId"))));

            verify(libraryEventService, never()).updateLibraryEvent(any());
        }

        @Test
        @DisplayName("null book returns 400 with validation error")
        void givenNullBook_thenReturns400() throws Exception {
            LibraryEvent event = new LibraryEvent(1L, EventType.UPDATE, null);

            mockMvc.perform(put("/v1/library-events")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(event)))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.errors", hasItem(containsString("book"))));

            verify(libraryEventService, never()).updateLibraryEvent(any());
        }

        @Test
        @DisplayName("malformed JSON body returns 400 Bad Request")
        void givenMalformedJson_thenReturns400() throws Exception {
            mockMvc.perform(put("/v1/library-events")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content("{invalid-json}"))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.status").value(400));

            verify(libraryEventService, never()).updateLibraryEvent(any());
        }

        @Test
        @DisplayName("service publish failure returns 500 Internal Server Error")
        void givenServicePublishFailure_thenReturns500() throws Exception {
            LibraryEvent event = validUpdateEvent();
            when(libraryEventService.updateLibraryEvent(any()))
                    .thenReturn(CompletableFuture.failedFuture(
                            new LibraryEventPublishException("Kafka broker unavailable", new RuntimeException())));

            MvcResult async = mockMvc.perform(put("/v1/library-events")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(event)))
                    .andExpect(request().asyncStarted())
                    .andReturn();

            mockMvc.perform(asyncDispatch(async))
                    .andExpect(status().isInternalServerError())
                    .andExpect(jsonPath("$.status").value(500))
                    .andExpect(jsonPath("$.errors[0]", containsString("Kafka broker unavailable")));
        }

        @Test
        @DisplayName("service throws synchronously returns 500 Internal Server Error")
        void givenServiceThrowsSynchronously_thenReturns500() throws Exception {
            LibraryEvent event = validUpdateEvent();
            when(libraryEventService.updateLibraryEvent(any()))
                    .thenThrow(new RuntimeException("Unexpected service failure"));

            mockMvc.perform(put("/v1/library-events")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(event)))
                    .andExpect(status().isInternalServerError())
                    .andExpect(jsonPath("$.status").value(500));
        }

        @Test
        @DisplayName("service returns failed future with unexpected exception returns 500")
        void givenServiceFailedFutureWithUnexpectedException_thenReturns500() throws Exception {
            LibraryEvent event = validUpdateEvent();
            when(libraryEventService.updateLibraryEvent(any()))
                    .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Unexpected async failure")));

            MvcResult async = mockMvc.perform(put("/v1/library-events")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(event)))
                    .andExpect(request().asyncStarted())
                    .andReturn();

            mockMvc.perform(asyncDispatch(async))
                    .andExpect(status().isInternalServerError())
                    .andExpect(jsonPath("$.status").value(500));
        }
    }

    // -------------------------------------------------------------------------
    // HTTP method and routing
    // -------------------------------------------------------------------------

    @Nested
    @DisplayName("HTTP method and routing")
    class RoutingTests {

        @Test
        @DisplayName("GET /v1/library-events returns 405 Method Not Allowed")
        void givenGetMethod_thenReturns405() throws Exception {
            mockMvc.perform(get("/v1/library-events"))
                    .andExpect(status().isMethodNotAllowed());
        }

        @Test
        @DisplayName("DELETE /v1/library-events returns 405 Method Not Allowed")
        void givenDeleteMethod_thenReturns405() throws Exception {
            mockMvc.perform(delete("/v1/library-events"))
                    .andExpect(status().isMethodNotAllowed());
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private Book validBook() {
        return new Book(10L, "Kafka: The Definitive Guide", "Neha Narkhede");
    }

    private LibraryEvent validAddEvent() {
        return new LibraryEvent(1L, EventType.ADD, validBook());
    }

    private LibraryEvent validUpdateEvent() {
        return new LibraryEvent(1L, EventType.UPDATE, validBook());
    }
}
