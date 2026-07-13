# Testing the Web Layer: @WebMvcTest, MockMvc, and @MockitoBean

## Why Test the Controller Separately?

A Spring MVC controller sits at the **boundary between the outside world and your application**.
It is responsible for:

- Mapping HTTP requests to handler methods
- Deserialising the request body (JSON → Java object)
- Triggering Bean Validation (`@Valid`)
- Delegating to a service
- Serialising the response (Java object → JSON)
- Returning the correct HTTP status code and headers
- Mapping exceptions to error responses (via `@ControllerAdvice`)

None of this is exercised by a plain unit test that calls the controller method directly —
because all of it lives **in the HTTP layer**, not in the method body.

```
                  ┌─────────────────────────────────────────────────────┐
                  │               HTTP Request                           │
                  │  POST /v1/library-events  { "libraryEventId": 1 }   │
                  └───────────────────────┬─────────────────────────────┘
                                          │
                              ┌───────────▼───────────┐
                              │   JSON Deserialisation │  ← tested by @WebMvcTest
                              │   Bean Validation      │  ← tested by @WebMvcTest
                              │   LibraryEventController│
                              │   @ControllerAdvice    │  ← tested by @WebMvcTest
                              └───────────┬───────────┘
                                          │
                              ┌───────────▼───────────┐
                              │   LibraryEventService  │  ← mocked away
                              └───────────────────────┘
```

---

## What is `@WebMvcTest`?

`@WebMvcTest` is a **slice test** annotation — it loads only the web layer of the Spring
context, leaving out everything else (Kafka, databases, services, etc.).

### What it loads

| Component | Loaded? |
|---|---|
| `@RestController` / `@Controller` | ✅ |
| `@ControllerAdvice` | ✅ |
| `Filter` | ✅ |
| `WebMvcConfigurer` | ✅ |
| Bean Validation (`@Valid`) | ✅ |
| `MockMvc` (auto-configured) | ✅ |
| `@Service` / `@Repository` | ❌ |
| Kafka auto-configuration | ❌ |
| Database / JPA | ❌ |

Because only the web slice is loaded, the test context **starts fast** and stays
**focused** — you cannot accidentally test Kafka behaviour here.

### `@ControllerAdvice` is auto-detected — no explicit import needed

`@WebMvcTest` scans and registers **all** `@ControllerAdvice` / `@RestControllerAdvice`
classes automatically. You do **not** need to reference `LibraryEventsControllerAdvice`
anywhere in `LibraryEventControllerSliceTest` — Spring includes it in the slice context
just by finding the annotation on the class.

```java
// ✅ No @Import or explicit reference required in the test class
@WebMvcTest(LibraryEventController.class)
class LibraryEventControllerSliceTest { ... }
// LibraryEventsControllerAdvice is still active and maps exceptions to error responses.
```

The only time you would need to register a `@ControllerAdvice` explicitly is when using
`MockMvcBuilders.standaloneSetup(controller)` in a plain unit test — there is no Spring
context in that setup, so you must call `.setControllerAdvice(new LibraryEventsControllerAdvice())`
manually.

### Spring Boot 4.x: Package change

In Spring Boot 3.x, `@WebMvcTest` was in `spring-boot-test-autoconfigure`.
In Spring Boot 4.x it moved to a dedicated artifact and package:

```groovy
// build.gradle — required in Spring Boot 4.x
testImplementation 'org.springframework.boot:spring-boot-webmvc-test'
```

```java
// Spring Boot 3.x
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;

// Spring Boot 4.x
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
```

---

## What is `MockMvc`?

`MockMvc` is Spring's test utility that **simulates HTTP requests without starting a real
server**. It drives the full Spring MVC dispatch pipeline — handler mapping, argument
resolution, message conversion, exception handling — in-process.

```java
@Autowired
private MockMvc mockMvc;
```

### Basic usage

```java
mockMvc.perform(post("/v1/library-events")
        .contentType(MediaType.APPLICATION_JSON)
        .content("""{"libraryEventId":1,"eventType":"ADD","book":{...}}"""))
    .andExpect(status().isCreated())
    .andExpect(header().string("Location", "/v1/library-events/1"))
    .andExpect(jsonPath("$.eventType").value("ADD"));
```

### Why MockMvc and not a plain method call?

```java
// ❌ Unit test — bypasses the HTTP layer entirely
ResponseEntity<LibraryEvent> response =
    controller.createLibraryEvent(event).join();
// Bean Validation never runs.
// @ControllerAdvice never runs.
// JSON deserialisation never runs.

// ✅ MockMvc — exercises the full HTTP stack
mockMvc.perform(post("/v1/library-events").content(...))
    .andExpect(status().isBadRequest()); // ← validation fires, advice maps it to 400
```

---

## Async Controllers and the Two-Step Dispatch

When a controller returns `CompletableFuture`, the Servlet thread is released immediately
and the response is written later. MockMvc mirrors this with a two-step pattern:

```java
// Step 1 — fire the request, assert async started, capture the pending result
MvcResult async = mockMvc.perform(post("/v1/library-events")
        .contentType(MediaType.APPLICATION_JSON)
        .content(objectMapper.writeValueAsString(event)))
    .andExpect(request().asyncStarted())  // the future was returned
    .andReturn();                          // grab the in-flight result

// Step 2 — resolve the future and assert the final HTTP response
mockMvc.perform(asyncDispatch(async))
    .andExpect(status().isCreated())
    .andExpect(header().string("Location", "/v1/library-events/1"));
```

Synchronous exceptions (validation failures, event-type mismatches) are thrown
**before** the future is returned, so they need only a single `perform()` call:

```java
// No async — validation fires synchronously
mockMvc.perform(post("/v1/library-events").content(invalidJson))
    .andExpect(status().isBadRequest());
```

---

## What is `@MockitoBean`?

`@MockitoBean` is a Spring Framework 7 annotation (replacing the old Spring Boot
`@MockBean`) that **registers a Mockito mock as a Spring bean** in the test application
context.

```java
@MockitoBean
private LibraryEventService libraryEventService;
```

This tells Spring: *"instead of creating a real `LibraryEventService`, put a Mockito mock
in the context so it can be injected into the controller."*

### `@MockitoBean` vs `@Mock`

| | `@Mock` (Mockito) | `@MockitoBean` (Spring) |
|---|---|---|
| **Registers in Spring context** | ❌ | ✅ |
| **Use with** | `@ExtendWith(MockitoExtension.class)` | `@WebMvcTest` / `@SpringBootTest` |
| **Injected into Spring beans** | ❌ (manual wiring only) | ✅ (autowired automatically) |
| **Resets between tests** | Yes (new mock per test) | Configurable via `MockReset` |

### Is `@MockitoBean` used in plain unit tests?

**No** — in plain unit tests you use `@Mock` from Mockito directly:

```java
// Plain unit test — no Spring context
@ExtendWith(MockitoExtension.class)
class LibraryEventControllerTest {

    @Mock                          // ← Mockito only, no Spring involved
    private LibraryEventService libraryEventService;

    @InjectMocks
    private LibraryEventController controller;
}
```

`@MockitoBean` only makes sense when a **Spring context is running** and you need to
replace a real bean with a mock. In `@WebMvcTest`, the controller is a real Spring bean
that gets `LibraryEventService` autowired — so you need `@MockitoBean` to provide a mock
that Spring can inject.

### Why not use a real `LibraryEventService` in a `@WebMvcTest`?

Because `LibraryEventService` depends on a Kafka producer, which would require a running
Kafka broker. `@WebMvcTest` deliberately excludes infrastructure like Kafka, so you mock
the service to keep the test fast and self-contained.

---

## What `@WebMvcTest` Tests vs What It Doesn't

### It tests ✅

| Scenario | Example in this project |
|---|---|
| Valid request → correct status + body | `POST` ADD event → `201 Created` |
| Invalid request → validation error | null `libraryEventId` → `400` |
| Wrong event type → `400` | UPDATE on POST → `@ControllerAdvice` maps `IllegalArgumentException` → `400` |
| Malformed JSON → `400` | Unparseable body → `HttpMessageNotReadableException` → `400` |
| Service failure → `500` | `LibraryEventPublishException` → `@ControllerAdvice` maps → `500` |
| Location header set correctly | `POST` → `Location: /v1/library-events/1` |
| Unsupported HTTP method → `405` | `GET /v1/library-events` → `405` |

### It does not test ❌

| Scenario | Tested where instead |
|---|---|
| Kafka message actually published | Integration / `@SpringBootTest` |
| Message reaches correct topic | Integration test with embedded Kafka |
| Service business logic | `LibraryEventServiceTest` (unit test) |
| Full application wiring | `@SpringBootTest` |

---

## Summary

```
@WebMvcTest          → loads only the web slice (controller + advice + validation)
MockMvc              → simulates HTTP requests in-process, no real server needed
@MockitoBean         → replaces a Spring bean with a Mockito mock in the context
@Mock                → creates a Mockito mock outside of any Spring context (unit tests)
asyncDispatch(...)   → resolves CompletableFuture responses in MockMvc tests
```

The combination of `@WebMvcTest` + `MockMvc` + `@MockitoBean` gives you **fast, focused
tests** that verify the HTTP contract of your controller without needing Kafka, a database,
or any other infrastructure running.

---

## Interview Questions

---

**Q1. What is the difference between a `@WebMvcTest` and a plain unit test using
`@ExtendWith(MockitoExtension.class)` for testing a Spring MVC controller?**

> A plain unit test calls the controller method directly and bypasses the entire HTTP
> layer — JSON deserialisation, Bean Validation (`@Valid`), `@ControllerAdvice` exception
> mapping, and HTTP status code handling never run. A `@WebMvcTest` loads the Spring web
> slice and uses `MockMvc` to send simulated HTTP requests through the full dispatch
> pipeline, so all of those behaviours are exercised. For example, sending a request with
> a null required field to a `@WebMvcTest` will correctly return a `400 Bad Request` via
> the `@ControllerAdvice`; the same call in a unit test would either pass validation
> silently or throw an exception with no HTTP mapping.

---

**Q2. In a `@WebMvcTest`, why do you use `@MockitoBean` for the service layer instead of
wiring a real implementation?**

> `@WebMvcTest` deliberately loads only the web slice — it excludes `@Service`,
> `@Repository`, and infrastructure auto-configurations such as Kafka or JPA. This means
> a real `LibraryEventService` that depends on a Kafka producer cannot be instantiated
> in this context. `@MockitoBean` registers a Mockito mock as a Spring bean so it can be
> autowired into the controller, keeping the test fast, self-contained, and free from any
> external dependency. The goal of the slice test is to verify the controller's HTTP
> contract, not the service's behaviour — so mocking the service is both necessary and
> correct.

---

**Q3. Your controller returns `CompletableFuture<ResponseEntity<LibraryEvent>>`. How does
this affect the way you write `MockMvc` tests, and why?**

> Because the controller returns a `CompletableFuture`, Spring MVC releases the Servlet
> thread immediately and writes the HTTP response asynchronously once the future completes.
> MockMvc mirrors this with a two-step pattern: the first `perform()` call fires the
> request and asserts that async processing started (`.andExpect(request().asyncStarted())`),
> returning an `MvcResult` that is still pending. A second
> `mockMvc.perform(asyncDispatch(mvcResult))` then resolves the future and lets you assert
> the final status code, headers, and response body. Skipping the async dispatch and
> asserting on the first `perform()` would assert against an incomplete response and give
> misleading results.

---

**Q4. What is the difference between `@Mock` and `@MockitoBean`? Can you use
`@MockitoBean` in a plain unit test?**

> `@Mock` (from Mockito) creates a mock object outside of any Spring context and is used
> with `@ExtendWith(MockitoExtension.class)`. It must be manually injected via
> `@InjectMocks` or constructor injection. `@MockitoBean` (from Spring Framework 7,
> replacing Spring Boot's `@MockBean`) registers a Mockito mock directly in the Spring
> application context so Spring can autowire it into real beans. You cannot meaningfully
> use `@MockitoBean` in a plain unit test — there is no Spring context running to register
> the mock in, so it would have no effect. Use `@Mock` for unit tests and `@MockitoBean`
> for slice tests (`@WebMvcTest`) or integration tests (`@SpringBootTest`).

---

**Q5. A colleague argues that since all validation errors are handled by
`LibraryEventsControllerAdvice`, you should test that class in isolation rather than
through `@WebMvcTest`. Do you agree?**

> Partially. Testing `LibraryEventsControllerAdvice` in isolation can verify that the
> error-mapping logic itself is correct. However, it cannot verify that Spring MVC actually
> **routes** a given exception to the right handler method, or that the exception is even
> raised in the first place from a real HTTP request. `@WebMvcTest` closes this gap — it
> proves that when a request with an invalid field arrives, the full chain works: validation
> fires, the exception is thrown, the advice intercepts it, and a `400` with the correct
> body is returned. Both levels of testing have value; the `@WebMvcTest` tests the
> integration of the controller and advice together as the HTTP contract demands.
