package com.arshad.exception;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Structured error response body returned by
 * {@link LibraryEventsControllerAdvice} for all handled exceptions.
 *
 * <p>Contains the HTTP status code and a list of human-readable error messages,
 * allowing clients to display every violation in a single response.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ErrorResponse {

    /** HTTP status code of the error (e.g. 400, 500). */
    private int status;

    /** One or more messages describing what went wrong. */
    private List<String> errors;
}
