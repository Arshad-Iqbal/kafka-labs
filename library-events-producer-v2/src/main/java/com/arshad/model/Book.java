package com.arshad.model;

import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Book {
    
    @NotNull(message = "bookId cannot be null")
    @Positive(message = "bookId must be a positive number")
    private Long bookId;
    
    @NotNull(message = "bookName cannot be null")
    @NotEmpty(message = "bookName cannot be empty")
    @Size(max = 255, message = "bookName must not exceed 255 characters")
    private String bookName;
    
    @NotNull(message = "bookAuthor cannot be null")
    @NotEmpty(message = "bookAuthor cannot be empty")
    @Size(max = 255, message = "bookAuthor must not exceed 255 characters")
    private String bookAuthor;
}
