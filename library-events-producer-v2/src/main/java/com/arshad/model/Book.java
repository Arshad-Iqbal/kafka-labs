package com.arshad.model;

import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Represents a book associated with a {@link LibraryEvent}.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Book {

    /** Unique identifier for the book. Must be a positive number. */
    @NotNull(message = "bookId cannot be null")
    @Positive(message = "bookId must be a positive number")
    private Long bookId;

    /** Title of the book. Must not be null or empty, and must not exceed 255 characters. */
    @NotNull(message = "bookName cannot be null")
    @NotEmpty(message = "bookName cannot be empty")
    @Size(max = 255, message = "bookName must not exceed 255 characters")
    private String bookName;

    /** Author of the book. Must not be null or empty, and must not exceed 255 characters. */
    @NotNull(message = "bookAuthor cannot be null")
    @NotEmpty(message = "bookAuthor cannot be empty")
    @Size(max = 255, message = "bookAuthor must not exceed 255 characters")
    private String bookAuthor;
}
