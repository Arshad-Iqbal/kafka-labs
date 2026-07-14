---
name: controller-skill
description: Write Spring MVC REST controllers with Kafka (CompletableFuture), DB (ResponseEntity), Bean Validation, and @RestControllerAdvice error handling. Use when creating or updating controllers.
license: Apache-2.0
metadata:
  author: Arshad-Iqbal
  version: "1.0"
---

# Controller Skill

This skill describes how to write REST controllers in this Spring Boot application.
It covers the two integration patterns (Kafka vs DB), payload validation, and centralised
error handling — all consistent with the conventions established in `LibraryEventController`
and `LibraryEventsControllerAdvice`.

---

## 1. Class-Level Annotations

```java
@RestController
@RequestMapping("/v1/your-resource")
@RequiredArgsConstructor
@Slf4j
public class YourController {

    private final YourKafkaService kafkaService;   // Kafka-backed service
    private final YourDbService    dbService;      // DB-backed service
}
```

| Annotation | Purpose |
|-----------|---------|
| `@RestController` | Combines `@Controller` + `@ResponseBody`; all handler methods serialise the return value to JSON. |
| `@RequestMapping("/v1/...")` | Version-prefixed base path; all handler method paths are relative to this. |
| `@RequiredArgsConstructor` | Lombok constructor injection — no explicit `@Autowired` needed. |
| `@Slf4j` | Lombok logger (`log`) — use `log.info` for successful operations, `log.error` for failures, always include the resource ID and operation in the message. |

---

## 2. Kafka Integration — Return `CompletableFuture`

When the handler delegates to a **Kafka-backed service**, the method **must** return
`CompletableFuture<ResponseEntity<T>>`. This releases the Servlet thread immediately and
writes the HTTP response only after the broker acknowledges the message.

```java
// POST — create / publish a new event (ADD)
@PostMapping
public CompletableFuture<ResponseEntity<LibraryEvent>> createLibraryEvent(
        @Valid @RequestBody LibraryEvent event) {

    log.info("POST /v1/library-events — creating event: {}", event);

    if (event.getEventType() != EventType.ADD) {
        throw new IllegalArgumentException(
                "POST endpoint only accepts ADD events, but received: " + event.getEventType());
    }

    return kafkaService.createLibraryEvent(event)
            .thenApply(published -> {
                URI location = URI.create("/v1/library-events/" + published.getLibraryEventId());
                log.info("Event created, id={}", published.getLibraryEventId());
                return ResponseEntity.created(location).body(published);
            });
}

// PUT — update / publish an update event (UPDATE)
@PutMapping
public CompletableFuture<ResponseEntity<LibraryEvent>> updateLibraryEvent(
        @Valid @RequestBody LibraryEvent event) {

    log.info("PUT /v1/library-events — updating event: {}", event);

    if (event.getEventType() != EventType.UPDATE) {
        throw new IllegalArgumentException(
                "PUT endpoint only accepts UPDATE events, but received: " + event.getEventType());
    }

    return kafkaService.updateLibraryEvent(event)
            .thenApply(published -> {
                log.info("Event updated, id={}", published.getLibraryEventId());
                return ResponseEntity.ok(published);
            });
}
```

### Rules
- **Always** return `CompletableFuture<ResponseEntity<T>>` — never block with `.get()` inside the controller.
- Chain response-building logic via `.thenApply()`; handle errors in `@RestControllerAdvice`, not via `.exceptionally()` in the controller.
- POST → `ResponseEntity.created(location).body(...)` with a `Location` header pointing to the created resource.
- PUT → `ResponseEntity.ok(...)`.

---

## 3. DB Integration — Return `ResponseEntity<T>` Directly

When the handler delegates to a **DB-backed service** (e.g. JPA, JDBC), return the result
synchronously, either as a plain type or wrapped in `ResponseEntity`.

```java
// GET — fetch a single resource by ID
@GetMapping("/{id}")
public ResponseEntity<Book> getBook(@PathVariable Long id) {
    log.info("GET /v1/books/{}", id);
    Book book = dbService.findById(id);           // throws ResourceNotFoundException if absent
    return ResponseEntity.ok(book);
}

// GET — fetch all resources
@GetMapping
public ResponseEntity<List<Book>> getAllBooks() {
    log.info("GET /v1/books");
    return ResponseEntity.ok(dbService.findAll());
}

// POST — create a resource in the DB
@PostMapping
public ResponseEntity<Book> createBook(@Valid @RequestBody Book book) {
    log.info("POST /v1/books — creating book: {}", book);
    Book saved = dbService.save(book);
    URI location = URI.create("/v1/books/" + saved.getBookId());
    return ResponseEntity.created(location).body(saved);
}

// PUT — update an existing resource
@PutMapping("/{id}")
public ResponseEntity<Book> updateBook(@PathVariable Long id,
                                       @Valid @RequestBody Book book) {
    log.info("PUT /v1/books/{}", id);
    Book updated = dbService.update(id, book);    // throws ResourceNotFoundException if absent
    return ResponseEntity.ok(updated);
}

// DELETE — remove a resource
@DeleteMapping("/{id}")
public ResponseEntity<Void> deleteBook(@PathVariable Long id) {
    log.info("DELETE /v1/books/{}", id);
    dbService.delete(id);
    return ResponseEntity.noContent().build();
}
```

### Rules
- Return `ResponseEntity<T>` (not the raw type) so the HTTP status is explicit.
- Use `ResponseEntity.ok()`, `.created()`, `.noContent()` — never hardcode numeric status codes in the controller.
- Let the service throw domain exceptions (e.g. `ResourceNotFoundException`); map them to HTTP status codes in `@RestControllerAdvice`.

---

## 4. Payload Validation

### 4.1 Standard Bean Validation on Models

Annotate model fields directly. Apply `@Valid` on every `@RequestBody` parameter.

```java
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
```

For nested objects, annotate the field with `@Valid` to cascade validation:

```java
@NotNull(message = "book cannot be null")
@Valid
private Book book;
```

### 4.2 Custom Constraint Annotation

When standard annotations are insufficient (e.g. validating an enum value), create a
custom constraint.

**Annotation:**

```java
@Documented
@Constraint(validatedBy = EventTypeValidator.class)
@Target({ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
public @interface ValidEventType {
    String message() default "eventType must be a valid enum value";
    Class<?>[] groups() default {};
    Class<? extends Payload>[] payload() default {};
}
```

**Validator:**

```java
public class EventTypeValidator implements ConstraintValidator<ValidEventType, EventType> {

    @Override
    public boolean isValid(EventType value, ConstraintValidatorContext context) {
        if (value == null) return false;
        return value == EventType.ADD || value == EventType.UPDATE;
    }
}
```

**Usage on model field:**

```java
@NotNull(message = "eventType cannot be null")
@ValidEventType(message = "eventType must be ADD or UPDATE")
private EventType eventType;
```

### 4.3 Business-Rule Validation in the Controller

For validations that depend on *which endpoint* is called (not just field correctness),
throw `IllegalArgumentException` directly in the controller method — it is mapped to
`400 Bad Request` by the controller advice.

```java
if (event.getEventType() != EventType.ADD) {
    throw new IllegalArgumentException(
            "POST endpoint only accepts ADD events, but received: " + event.getEventType());
}
```

---

## 5. Error Handling — `@RestControllerAdvice`

All exception-to-HTTP-status mapping lives in a **single** `@RestControllerAdvice` class.
Controllers **never** catch exceptions themselves — they throw and let the advice handle it.

### 5.1 ErrorResponse Shape

```java
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ErrorResponse {
    private int status;          // HTTP status code
    private List<String> errors; // one message per violation
}
```

### 5.2 Advice Class Template

```java
@RestControllerAdvice
@Slf4j
public class AppControllerAdvice {

    /** Bean Validation failures — collects ALL field errors in one response. */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleValidation(MethodArgumentNotValidException ex) {
        List<String> errors = ex.getBindingResult().getFieldErrors().stream()
                .map(fe -> fe.getField() + ": " + fe.getDefaultMessage())
                .sorted()
                .toList();
        log.warn("Validation failed: {}", errors);
        return ResponseEntity.badRequest()
                .body(new ErrorResponse(400, errors));
    }

    /** Malformed / unreadable JSON body. */
    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ErrorResponse> handleNotReadable(HttpMessageNotReadableException ex) {
        List<String> errors = List.of("Request body is missing or malformed: "
                + ex.getMostSpecificCause().getMessage());
        log.warn("Malformed body: {}", ex.getMostSpecificCause().getMessage());
        return ResponseEntity.badRequest()
                .body(new ErrorResponse(400, errors));
    }

    /** Business-rule violations thrown from the controller. */
    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ErrorResponse> handleIllegalArgument(IllegalArgumentException ex) {
        log.warn("Invalid argument: {}", ex.getMessage());
        return ResponseEntity.badRequest()
                .body(new ErrorResponse(400, List.of(ex.getMessage())));
    }

    /** Resource not found (DB integration). */
    @ExceptionHandler(ResourceNotFoundException.class)
    public ResponseEntity<ErrorResponse> handleNotFound(ResourceNotFoundException ex) {
        log.warn("Resource not found: {}", ex.getMessage());
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(new ErrorResponse(404, List.of(ex.getMessage())));
    }

    /** Kafka publish failure. */
    @ExceptionHandler(LibraryEventPublishException.class)
    public ResponseEntity<ErrorResponse> handlePublish(LibraryEventPublishException ex) {
        log.error("Kafka publish failed: {}", ex.getMessage(), ex);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new ErrorResponse(500, List.of(ex.getMessage())));
    }

    /** Wrong HTTP method. */
    @ExceptionHandler(HttpRequestMethodNotSupportedException.class)
    public ResponseEntity<ErrorResponse> handleMethodNotSupported(HttpRequestMethodNotSupportedException ex) {
        log.warn("Method not supported: {}", ex.getMethod());
        return ResponseEntity.status(HttpStatus.METHOD_NOT_ALLOWED)
                .body(new ErrorResponse(405, List.of("HTTP method not supported: " + ex.getMethod())));
    }

    /** Catch-all — hides internal details from the caller. */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleUnexpected(Exception ex) {
        log.error("Unexpected error: {}", ex.getMessage(), ex);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new ErrorResponse(500, List.of("An unexpected error occurred. Please try again later.")));
    }
}
```

### 5.3 Handler Ordering Rules
- Most-specific handlers must be declared **before** more-general ones.
- The catch-all `Exception.class` handler must always be **last**.
- Never add duplicate handlers for the same exception type — Spring picks only the first match.

---

## 6. Return Type Decision Tree

```
Does the handler integrate with Kafka?
    YES → return CompletableFuture<ResponseEntity<T>>
    NO  → Does it need a non-200 status or headers?
              YES → return ResponseEntity<T>
              NO  → return T directly (Spring wraps it in 200 OK)
```

---

## 7. HTTP Status Conventions

| Operation | Integration | Status |
|-----------|------------|--------|
| Create resource | Kafka or DB | `201 Created` + `Location` header |
| Update resource | Kafka or DB | `200 OK` |
| Fetch resource  | DB | `200 OK` |
| Delete resource | DB | `204 No Content` |
| Validation error | — | `400 Bad Request` |
| Business-rule error | — | `400 Bad Request` |
| Resource not found | DB | `404 Not Found` |
| Wrong HTTP method | — | `405 Method Not Allowed` |
| Kafka publish failure | Kafka | `500 Internal Server Error` |
| Unexpected error | — | `500 Internal Server Error` |

---

## 8. Key Pitfalls

| Pitfall | Fix |
|---------|-----|
| Blocking on `CompletableFuture.get()` inside controller | Never block — return the future directly and chain logic via `.thenApply()` |
| Catching exceptions inside the controller method | Remove the try/catch; throw and let `@RestControllerAdvice` handle it |
| Missing `@Valid` on `@RequestBody` | Without it, Bean Validation constraints on the model are silently ignored |
| Forgetting `@Valid` on nested objects | Add `@Valid` on the nested field in the parent model to cascade validation |
| Hardcoding status codes (`new ResponseEntity<>(body, 400)`) | Use `ResponseEntity.badRequest()`, `.ok()`, `.status(HttpStatus.X)` for readability |
| Returning the raw type from a DB handler without `ResponseEntity` | Always wrap in `ResponseEntity` to make the HTTP status explicit and allow future header additions |
| Duplicate `@ExceptionHandler` for the same type | Only one handler per exception type per advice class; consolidate if needed |
