package com.arshad.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class LibraryEventSerializationTest {

    @Test
    void testBookCreationWithLombok() {
        Book book = new Book(1L, "The Great Gatsby", "F. Scott Fitzgerald");
        
        assertNotNull(book);
        assertEquals(1L, book.getBookId());
        assertEquals("The Great Gatsby", book.getBookName());
        assertEquals("F. Scott Fitzgerald", book.getBookAuthor());
    }

    @Test
    void testLibraryEventCreation() {
        Book book = new Book(1L, "Clean Code", "Robert C. Martin");
        LibraryEvent event = new LibraryEvent(101L, EventType.ADD, book);
        
        assertNotNull(event);
        assertEquals(101L, event.getLibraryEventId());
        assertEquals(EventType.ADD, event.getEventType());
        assertNotNull(event.getBook());
        assertEquals("Clean Code", event.getBook().getBookName());
    }

    @Test
    void testEventTypeEnum() {
        assertEquals(2, EventType.values().length);
        assertTrue(EventType.ADD.name().equals("ADD"));
        assertTrue(EventType.UPDATE.name().equals("UPDATE"));
    }

    @Test
    void testJsonSerialization() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        
        Book book = new Book(5L, "Design Patterns", "Gang of Four");
        LibraryEvent event = new LibraryEvent(201L, EventType.UPDATE, book);
        
        String json = mapper.writeValueAsString(event);
        
        assertNotNull(json);
        assertTrue(json.contains("\"libraryEventId\":201"));
        assertTrue(json.contains("\"eventType\":\"UPDATE\""));
        assertTrue(json.contains("\"bookName\":\"Design Patterns\""));
    }

    @Test
    void testJsonDeserialization() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        
        String json = "{\"libraryEventId\":301,\"eventType\":\"ADD\",\"book\":{\"bookId\":10,\"bookName\":\"Refactoring\",\"bookAuthor\":\"Martin Fowler\"}}";
        
        LibraryEvent event = mapper.readValue(json, LibraryEvent.class);
        
        assertEquals(301L, event.getLibraryEventId());
        assertEquals(EventType.ADD, event.getEventType());
        assertEquals(10, event.getBook().getBookId());
        assertEquals("Refactoring", event.getBook().getBookName());
        assertEquals("Martin Fowler", event.getBook().getBookAuthor());
    }

    @Test
    void testNoArgsConstructor() {
        Book book = new Book();
        LibraryEvent event = new LibraryEvent();
        
        assertNotNull(book);
        assertNotNull(event);
        assertNull(event.getLibraryEventId());
        assertNull(event.getEventType());
        assertNull(event.getBook());
    }

    @Test
    void testAllArgsConstructor() {
        Book book = new Book(7L, "Spring in Action", "Craig Walls");
        LibraryEvent event = new LibraryEvent(401L, EventType.ADD, book);
        
        assertEquals(401L, event.getLibraryEventId());
        assertEquals(EventType.ADD, event.getEventType());
        assertEquals(book, event.getBook());
    }

    @Test
    void testLombokEqualsAndHashCode() {
        Book book1 = new Book(1L, "Book A", "Author A");
        Book book2 = new Book(1L, "Book A", "Author A");
        Book book3 = new Book(2L, "Book B", "Author B");
        
        assertEquals(book1, book2);
        assertNotEquals(book1, book3);
    }

    @Test
    void testLombokToString() {
        Book book = new Book(1L, "Test Book", "Test Author");
        String toString = book.toString();
        
        assertNotNull(toString);
        assertTrue(toString.contains("Book"));
        assertTrue(toString.contains("bookId"));
        assertTrue(toString.contains("bookName"));
    }
}
