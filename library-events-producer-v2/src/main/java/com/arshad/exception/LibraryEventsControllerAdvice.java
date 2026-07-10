package com.arshad.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.List;

/**
 * Global exception handler for all controllers in the library events producer application.
 *
 * <p>Catches and maps domain and framework exceptions to structured {@link ErrorResponse} bodies,
 * keeping error-handling logic out of individual controllers.
 */
@RestControllerAdvice
@Slf4j
public class LibraryEventsControllerAdvice {

    /**
     * Handles bean-validation failures (@Valid on request body).
     * Collects all field-level error messages and returns them together
     * so the caller sees every violation in a single response.
     */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleValidationErrors(MethodArgumentNotValidException ex) {
        List<String> errors = ex.getBindingResult()
                .getFieldErrors()
                .stream()
                .map(fe -> fe.getField() + ": " + fe.getDefaultMessage())
                .sorted()
                .toList();

        log.warn("Validation failed: {}", errors);
        return ResponseEntity
                .status(HttpStatus.BAD_REQUEST)
                .body(new ErrorResponse(HttpStatus.BAD_REQUEST.value(), errors));
    }

    /**
     * Handles completely malformed / unparseable JSON bodies.
     */
    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ErrorResponse> handleNotReadable(HttpMessageNotReadableException ex) {
        List<String> errors = List.of("Request body is missing or malformed: " + ex.getMostSpecificCause().getMessage());

        log.warn("Malformed request body: {}", ex.getMostSpecificCause().getMessage());
        return ResponseEntity
                .status(HttpStatus.BAD_REQUEST)
                .body(new ErrorResponse(HttpStatus.BAD_REQUEST.value(), errors));
    }

    /**
     * Handles failures when publishing a library event to Kafka.
     */
    @ExceptionHandler(LibraryEventPublishException.class)
    public ResponseEntity<ErrorResponse> handlePublishException(LibraryEventPublishException ex) {
        List<String> errors = List.of(ex.getMessage());

        log.error("Failed to publish library event: {}", ex.getMessage(), ex);
        return ResponseEntity
                .status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new ErrorResponse(HttpStatus.INTERNAL_SERVER_ERROR.value(), errors));
    }
}
