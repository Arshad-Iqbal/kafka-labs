package com.arshad.model;

import com.arshad.validator.EventTypeValidator;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class LibraryEventValidationTest {

    private Validator validator;

    @BeforeEach
    void setUp() {
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        validator = factory.getValidator();
    }

    // ==================== Book Validation Tests ====================

    @Test
    void testValidBook() {
        Book book = new Book(1L, "The Great Gatsby", "F. Scott Fitzgerald");
        Set<ConstraintViolation<Book>> violations = validator.validate(book);
        assertTrue(violations.isEmpty(), "Valid book should have no violations");
    }

    @Test
    void testBookIdNull() {
        Book book = new Book(null, "The Great Gatsby", "F. Scott Fitzgerald");
        Set<ConstraintViolation<Book>> violations = validator.validate(book);
        assertFalse(violations.isEmpty(), "Book with null id should have violations");
        assertTrue(violations.stream().anyMatch(v -> v.getMessage().contains("bookId cannot be null")), 
                   "Should contain bookId null message");
    }

    @Test
    void testBookIdNegative() {
        Book book = new Book(-1L, "The Great Gatsby", "F. Scott Fitzgerald");
        Set<ConstraintViolation<Book>> violations = validator.validate(book);
        assertFalse(violations.isEmpty(), "Book with negative id should have violations");
        assertTrue(violations.stream().anyMatch(v -> v.getMessage().contains("must be a positive number")), 
                   "Should contain positive number message");
    }

    @Test
    void testBookIdZero() {
        Book book = new Book(0L, "The Great Gatsby", "F. Scott Fitzgerald");
        Set<ConstraintViolation<Book>> violations = validator.validate(book);
        assertFalse(violations.isEmpty(), "Book with id 0 should have violations");
    }

    @Test
    void testBookNameNull() {
        Book book = new Book(1L, null, "F. Scott Fitzgerald");
        Set<ConstraintViolation<Book>> violations = validator.validate(book);
        assertFalse(violations.isEmpty(), "Book with null name should have violations");
        assertTrue(violations.stream().anyMatch(v -> v.getMessage().contains("bookName cannot be null")), 
                   "Should contain bookName null message");
    }

    @Test
    void testBookNameEmpty() {
        Book book = new Book(1L, "", "F. Scott Fitzgerald");
        Set<ConstraintViolation<Book>> violations = validator.validate(book);
        assertFalse(violations.isEmpty(), "Book with empty name should have violations");
        assertTrue(violations.stream().anyMatch(v -> v.getMessage().contains("bookName cannot be empty")), 
                   "Should contain bookName empty message");
    }

    @Test
    void testBookNameTooLong() {
        String longName = "a".repeat(256);
        Book book = new Book(1L, longName, "Author");
        Set<ConstraintViolation<Book>> violations = validator.validate(book);
        assertFalse(violations.isEmpty(), "Book with name > 255 chars should have violations");
        assertTrue(violations.stream().anyMatch(v -> v.getMessage().contains("must not exceed 255 characters")), 
                   "Should contain size message");
    }

    @Test
    void testBookNameMaxLength() {
        String maxName = "a".repeat(255);
        Book book = new Book(1L, maxName, "Author");
        Set<ConstraintViolation<Book>> violations = validator.validate(book);
        assertTrue(violations.isEmpty(), "Book with name of 255 chars should be valid");
    }

    @Test
    void testBookAuthorNull() {
        Book book = new Book(1L, "The Great Gatsby", null);
        Set<ConstraintViolation<Book>> violations = validator.validate(book);
        assertFalse(violations.isEmpty(), "Book with null author should have violations");
        assertTrue(violations.stream().anyMatch(v -> v.getMessage().contains("bookAuthor cannot be null")), 
                   "Should contain bookAuthor null message");
    }

    @Test
    void testBookAuthorEmpty() {
        Book book = new Book(1L, "The Great Gatsby", "");
        Set<ConstraintViolation<Book>> violations = validator.validate(book);
        assertFalse(violations.isEmpty(), "Book with empty author should have violations");
        assertTrue(violations.stream().anyMatch(v -> v.getMessage().contains("bookAuthor cannot be empty")), 
                   "Should contain bookAuthor empty message");
    }

    @Test
    void testBookAuthorTooLong() {
        String longAuthor = "a".repeat(256);
        Book book = new Book(1L, "Book", longAuthor);
        Set<ConstraintViolation<Book>> violations = validator.validate(book);
        assertFalse(violations.isEmpty(), "Book with author > 255 chars should have violations");
    }

    @Test
    void testBookAuthorMaxLength() {
        String maxAuthor = "a".repeat(255);
        Book book = new Book(1L, "Book", maxAuthor);
        Set<ConstraintViolation<Book>> violations = validator.validate(book);
        assertTrue(violations.isEmpty(), "Book with author of 255 chars should be valid");
    }

    // ==================== LibraryEvent Validation Tests ====================

    @Test
    void testValidLibraryEvent() {
        Book book = new Book(1L, "The Great Gatsby", "F. Scott Fitzgerald");
        LibraryEvent event = new LibraryEvent(101L, EventType.ADD, book);
        Set<ConstraintViolation<LibraryEvent>> violations = validator.validate(event);
        assertTrue(violations.isEmpty(), "Valid library event should have no violations");
    }

    @Test
    void testValidLibraryEventWithUpdate() {
        Book book = new Book(2L, "Clean Code", "Robert C. Martin");
        LibraryEvent event = new LibraryEvent(102L, EventType.UPDATE, book);
        Set<ConstraintViolation<LibraryEvent>> violations = validator.validate(event);
        assertTrue(violations.isEmpty(), "Valid library event with UPDATE should have no violations");
    }

    @Test
    void testLibraryEventIdNull() {
        Book book = new Book(1L, "Book", "Author");
        LibraryEvent event = new LibraryEvent(null, EventType.ADD, book);
        Set<ConstraintViolation<LibraryEvent>> violations = validator.validate(event);
        assertFalse(violations.isEmpty(), "LibraryEvent with null id should have violations");
        assertTrue(violations.stream().anyMatch(v -> v.getMessage().contains("libraryEventId cannot be null")), 
                   "Should contain libraryEventId null message");
    }

    @Test
    void testLibraryEventIdNegative() {
        Book book = new Book(1L, "Book", "Author");
        LibraryEvent event = new LibraryEvent(-1L, EventType.ADD, book);
        Set<ConstraintViolation<LibraryEvent>> violations = validator.validate(event);
        assertFalse(violations.isEmpty(), "LibraryEvent with negative id should have violations");
        assertTrue(violations.stream().anyMatch(v -> v.getMessage().contains("must be a positive number")), 
                   "Should contain positive number message");
    }

    @Test
    void testLibraryEventIdZero() {
        Book book = new Book(1L, "Book", "Author");
        LibraryEvent event = new LibraryEvent(0L, EventType.ADD, book);
        Set<ConstraintViolation<LibraryEvent>> violations = validator.validate(event);
        assertFalse(violations.isEmpty(), "LibraryEvent with id 0 should have violations");
    }

    @Test
    void testEventTypeNull() {
        Book book = new Book(1L, "Book", "Author");
        LibraryEvent event = new LibraryEvent(101L, null, book);
        Set<ConstraintViolation<LibraryEvent>> violations = validator.validate(event);
        assertFalse(violations.isEmpty(), "LibraryEvent with null eventType should have violations");
        assertTrue(violations.stream().anyMatch(v -> v.getMessage().contains("eventType cannot be null")), 
                   "Should contain eventType null message");
    }

    @Test
    void testBookNull() {
        LibraryEvent event = new LibraryEvent(101L, EventType.ADD, null);
        Set<ConstraintViolation<LibraryEvent>> violations = validator.validate(event);
        assertFalse(violations.isEmpty(), "LibraryEvent with null book should have violations");
        assertTrue(violations.stream().anyMatch(v -> v.getMessage().contains("book cannot be null")), 
                   "Should contain book null message");
    }

    @Test
    void testCascadeValidationWithInvalidBook() {
        Book invalidBook = new Book(-1L, "", null);
        LibraryEvent event = new LibraryEvent(101L, EventType.ADD, invalidBook);
        Set<ConstraintViolation<LibraryEvent>> violations = validator.validate(event);
        assertFalse(violations.isEmpty(), "LibraryEvent with invalid book should have violations");
        assertTrue(violations.size() >= 3, "Should have at least 3 violations from the invalid book");
    }

    @Test
    void testCascadeValidationWithValidBook() {
        Book validBook = new Book(1L, "Valid Book", "Valid Author");
        LibraryEvent event = new LibraryEvent(101L, EventType.ADD, validBook);
        Set<ConstraintViolation<LibraryEvent>> violations = validator.validate(event);
        assertTrue(violations.isEmpty(), "LibraryEvent with valid book should have no violations");
    }

    // ==================== EventTypeValidator Tests ====================

    @Test
    void testEventTypeValidatorWithADD() {
        EventTypeValidator validator = new EventTypeValidator();
        assertTrue(validator.isValid(EventType.ADD, null), "ADD should be valid");
    }

    @Test
    void testEventTypeValidatorWithUPDATE() {
        EventTypeValidator validator = new EventTypeValidator();
        assertTrue(validator.isValid(EventType.UPDATE, null), "UPDATE should be valid");
    }

    @Test
    void testEventTypeValidatorWithNull() {
        EventTypeValidator validator = new EventTypeValidator();
        assertFalse(validator.isValid(null, null), "null should be invalid");
    }

    // ==================== Multiple Violations Tests ====================

    @Test
    void testMultipleViolationsInBook() {
        Book book = new Book(-1L, "", "");
        Set<ConstraintViolation<Book>> violations = validator.validate(book);
        assertEquals(3, violations.size(), "Should have 3 violations");
    }

    @Test
    void testMultipleViolationsInLibraryEvent() {
        Book invalidBook = new Book(-5L, null, "");
        LibraryEvent event = new LibraryEvent(-1L, null, invalidBook);
        Set<ConstraintViolation<LibraryEvent>> violations = validator.validate(event);
        assertFalse(violations.isEmpty(), "Should have multiple violations");
        assertTrue(violations.size() >= 3, "Should have at least 3 violations");
    }

    // ==================== Edge Cases ====================

    @Test
    void testBookNameWithSpaces() {
        Book book = new Book(1L, "  The Great Gatsby  ", "F. Scott Fitzgerald");
        Set<ConstraintViolation<Book>> violations = validator.validate(book);
        assertTrue(violations.isEmpty(), "Book with spaces in name should be valid");
    }

    @Test
    void testBookWithSpecialCharacters() {
        Book book = new Book(1L, "The Great Gatsby: A Novel™", "F. Scott Fitzgerald™");
        Set<ConstraintViolation<Book>> violations = validator.validate(book);
        assertTrue(violations.isEmpty(), "Book with special characters should be valid");
    }

    @Test
    void testLargePositiveBookId() {
        Book book = new Book(Long.MAX_VALUE, "Book", "Author");
        Set<ConstraintViolation<Book>> violations = validator.validate(book);
        assertTrue(violations.isEmpty(), "Book with MAX_VALUE id should be valid");
    }

    @Test
    void testLargePositiveLibraryEventId() {
        Book book = new Book(1L, "Book", "Author");
        LibraryEvent event = new LibraryEvent(Long.MAX_VALUE, EventType.ADD, book);
        Set<ConstraintViolation<LibraryEvent>> violations = validator.validate(event);
        assertTrue(violations.isEmpty(), "LibraryEvent with MAX_VALUE id should be valid");
    }
}
