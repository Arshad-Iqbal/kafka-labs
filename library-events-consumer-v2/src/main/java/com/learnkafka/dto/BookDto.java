package com.learnkafka.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

public record BookDto(
        @NotNull(message = "bookId cannot be null")
        @Positive(message = "bookId must be a positive number")
        Integer bookId,

        @NotBlank(message = "bookName cannot be blank")
        String bookName,

        @NotBlank(message = "bookAuthor cannot be blank")
        String bookAuthor
) {
}
