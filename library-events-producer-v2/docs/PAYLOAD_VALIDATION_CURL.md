# Payload Validation — cURL Test Cases

---

## Table of Contents

- [Quick Reference](#quick-reference)
- [CREATE — POST /v1/library-events](#create--post-v1library-events)
  - [1.1 Happy Path — valid ADD event (201 Created)](#11-happy-path--valid-add-event-201-created)
  - [1.2 Wrong event type — UPDATE sent to POST (400)](#12-wrong-event-type--update-sent-to-post-400)
  - [1.3 Malformed / unparseable body (400)](#13-malformed--unparseable-body-400)
  - [1.4 `libraryEventId` validation failures (400)](#14-libraryeventid-validation-failures-400)
  - [1.5 `eventType` validation failures (400)](#15-eventtype-validation-failures-400)
  - [1.6 `book` object validation failures (400)](#16-book-object-validation-failures-400)
  - [1.7 Multiple validation failures at once (400)](#17-multiple-validation-failures-at-once-400)
- [UPDATE — PUT /v1/library-events](#update--put-v1library-events)
  - [2.1 Happy Path — valid UPDATE event (200 OK)](#21-happy-path--valid-update-event-200-ok)
  - [2.2 Wrong event type — ADD sent to PUT (400)](#22-wrong-event-type--add-sent-to-put-400)
  - [2.3 Malformed / unparseable body (400)](#23-malformed--unparseable-body-400)
  - [2.4 `libraryEventId` validation failures (400)](#24-libraryeventid-validation-failures-400)
  - [2.5 `eventType` validation failures (400)](#25-eventtype-validation-failures-400)
  - [2.6 `book` object validation failures (400)](#26-book-object-validation-failures-400)
  - [2.7 Multiple validation failures at once (400)](#27-multiple-validation-failures-at-once-400)

---

## Quick Reference

| Method | Endpoint | eventType | Success | Error |
|--------|----------|-----------|---------|-------|
| POST | `/v1/library-events` | `ADD` | `201 Created` + `Location` header | `400` / `500` |
| PUT | `/v1/library-events` | `UPDATE` | `200 OK` | `400` / `500` |

---

## CREATE — POST /v1/library-events

### 1.1 Happy Path — valid ADD event (201 Created)

```bash
curl -i -X POST "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 1001,
    "eventType": "ADD",
    "book": {
      "bookId": 10,
      "bookName": "Kafka Fundamentals",
      "bookAuthor": "Arshad Iqbal"
    }
  }'
```

### 1.2 Wrong event type — UPDATE sent to POST (400)

```bash
curl -i -X POST "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 1001,
    "eventType": "UPDATE",
    "book": {
      "bookId": 10,
      "bookName": "Kafka Fundamentals",
      "bookAuthor": "Arshad Iqbal"
    }
  }'
```

### 1.3 Malformed / unparseable body (400)

#### 1.3.1 Missing closing brace — malformed JSON

```bash
curl -i -X POST "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 1001,
    "eventType": "ADD",
    "book": {
      "bookId": 10,
      "bookName": "Kafka Fundamentals",
      "bookAuthor": "Arshad Iqbal"
    }
  '
```

#### 1.3.2 Completely empty body

```bash
curl -i -X POST "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d ''
```

#### 1.3.3 Plain text instead of JSON

```bash
curl -i -X POST "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d 'this is not json'
```

#### 1.3.4 Wrong Content-Type header

```bash
curl -i -X POST "http://localhost:8080/v1/library-events" \
  -H "Content-Type: text/plain" \
  -d '{
    "libraryEventId": 1001,
    "eventType": "ADD",
    "book": {
      "bookId": 10,
      "bookName": "Kafka Fundamentals",
      "bookAuthor": "Arshad Iqbal"
    }
  }'
```

### 1.4 `libraryEventId` validation failures (400)

#### 1.4.1 `libraryEventId` is null

```bash
curl -i -X POST "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": null,
    "eventType": "ADD",
    "book": {
      "bookId": 10,
      "bookName": "Kafka Fundamentals",
      "bookAuthor": "Arshad Iqbal"
    }
  }'
```

#### 1.4.2 `libraryEventId` is zero (not positive)

```bash
curl -i -X POST "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 0,
    "eventType": "ADD",
    "book": {
      "bookId": 10,
      "bookName": "Kafka Fundamentals",
      "bookAuthor": "Arshad Iqbal"
    }
  }'
```

#### 1.4.3 `libraryEventId` is negative

```bash
curl -i -X POST "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": -5,
    "eventType": "ADD",
    "book": {
      "bookId": 10,
      "bookName": "Kafka Fundamentals",
      "bookAuthor": "Arshad Iqbal"
    }
  }'
```

### 1.5 `eventType` validation failures (400)

#### 1.5.1 `eventType` is absent (null)

```bash
curl -i -X POST "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 1001,
    "book": {
      "bookId": 10,
      "bookName": "Kafka Fundamentals",
      "bookAuthor": "Arshad Iqbal"
    }
  }'
```

#### 1.5.2 `eventType` is an invalid enum value

```bash
curl -i -X POST "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 1001,
    "eventType": "DELETE",
    "book": {
      "bookId": 10,
      "bookName": "Kafka Fundamentals",
      "bookAuthor": "Arshad Iqbal"
    }
  }'
```

### 1.6 `book` object validation failures (400)

#### 1.6.1 `book` is null

```bash
curl -i -X POST "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 1001,
    "eventType": "ADD",
    "book": null
  }'
```

#### 1.6.2 `bookId` is null

```bash
curl -i -X POST "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 1001,
    "eventType": "ADD",
    "book": {
      "bookId": null,
      "bookName": "Kafka Fundamentals",
      "bookAuthor": "Arshad Iqbal"
    }
  }'
```

#### 1.6.3 `bookId` is negative

```bash
curl -i -X POST "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 1001,
    "eventType": "ADD",
    "book": {
      "bookId": -1,
      "bookName": "Kafka Fundamentals",
      "bookAuthor": "Arshad Iqbal"
    }
  }'
```

#### 1.6.4 `bookName` is empty string

```bash
curl -i -X POST "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 1001,
    "eventType": "ADD",
    "book": {
      "bookId": 10,
      "bookName": "",
      "bookAuthor": "Arshad Iqbal"
    }
  }'
```

#### 1.6.5 `bookAuthor` is empty string

```bash
curl -i -X POST "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 1001,
    "eventType": "ADD",
    "book": {
      "bookId": 10,
      "bookName": "Kafka Fundamentals",
      "bookAuthor": ""
    }
  }'
```

### 1.7 Multiple validation failures at once (400)

#### 1.7.1 `libraryEventId` null + `eventType` absent + `book` null

```bash
curl -i -X POST "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": null,
    "book": null
  }'
```

#### 1.7.2 Invalid `libraryEventId` + invalid `eventType` + invalid `book` fields

```bash
curl -i -X POST "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": -1,
    "eventType": "DELETE",
    "book": {
      "bookId": -99,
      "bookName": "",
      "bookAuthor": ""
    }
  }'
```

#### 1.7.3 All `book` fields invalid simultaneously

```bash
curl -i -X POST "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 1001,
    "eventType": "ADD",
    "book": {
      "bookId": null,
      "bookName": "",
      "bookAuthor": ""
    }
  }'
```

#### 1.7.4 Completely empty JSON object `{}`

```bash
curl -i -X POST "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d '{}'
```

---

## UPDATE — PUT /v1/library-events

### 2.1 Happy Path — valid UPDATE event (200 OK)

```bash
curl -i -X PUT "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 1001,
    "eventType": "UPDATE",
    "book": {
      "bookId": 10,
      "bookName": "Kafka Fundamentals - 2nd Edition",
      "bookAuthor": "Arshad Iqbal"
    }
  }'
```

### 2.2 Wrong event type — ADD sent to PUT (400)

```bash
curl -i -X PUT "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 1001,
    "eventType": "ADD",
    "book": {
      "bookId": 10,
      "bookName": "Kafka Fundamentals - 2nd Edition",
      "bookAuthor": "Arshad Iqbal"
    }
  }'
```

### 2.3 Malformed / unparseable body (400)

#### 2.3.1 Missing closing brace — malformed JSON

```bash
curl -i -X PUT "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 1001,
    "eventType": "UPDATE",
    "book": {
      "bookId": 10,
      "bookName": "Kafka Fundamentals - 2nd Edition",
      "bookAuthor": "Arshad Iqbal"
    }
  '
```

#### 2.3.2 Completely empty body

```bash
curl -i -X PUT "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d ''
```

#### 2.3.3 Plain text instead of JSON

```bash
curl -i -X PUT "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d 'this is not json'
```

#### 2.3.4 Wrong Content-Type header

```bash
curl -i -X PUT "http://localhost:8080/v1/library-events" \
  -H "Content-Type: text/plain" \
  -d '{
    "libraryEventId": 1001,
    "eventType": "UPDATE",
    "book": {
      "bookId": 10,
      "bookName": "Kafka Fundamentals - 2nd Edition",
      "bookAuthor": "Arshad Iqbal"
    }
  }'
```

### 2.4 `libraryEventId` validation failures (400)

#### 2.4.1 `libraryEventId` is null

```bash
curl -i -X PUT "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": null,
    "eventType": "UPDATE",
    "book": {
      "bookId": 10,
      "bookName": "Kafka Fundamentals - 2nd Edition",
      "bookAuthor": "Arshad Iqbal"
    }
  }'
```

#### 2.4.2 `libraryEventId` is zero (not positive)

```bash
curl -i -X PUT "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 0,
    "eventType": "UPDATE",
    "book": {
      "bookId": 10,
      "bookName": "Kafka Fundamentals - 2nd Edition",
      "bookAuthor": "Arshad Iqbal"
    }
  }'
```

#### 2.4.3 `libraryEventId` is negative

```bash
curl -i -X PUT "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": -5,
    "eventType": "UPDATE",
    "book": {
      "bookId": 10,
      "bookName": "Kafka Fundamentals - 2nd Edition",
      "bookAuthor": "Arshad Iqbal"
    }
  }'
```

### 2.5 `eventType` validation failures (400)

#### 2.5.1 `eventType` is absent (null)

```bash
curl -i -X PUT "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 1001,
    "book": {
      "bookId": 10,
      "bookName": "Kafka Fundamentals - 2nd Edition",
      "bookAuthor": "Arshad Iqbal"
    }
  }'
```

#### 2.5.2 `eventType` is an invalid enum value

```bash
curl -i -X PUT "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 1001,
    "eventType": "DELETE",
    "book": {
      "bookId": 10,
      "bookName": "Kafka Fundamentals - 2nd Edition",
      "bookAuthor": "Arshad Iqbal"
    }
  }'
```

### 2.6 `book` object validation failures (400)

#### 2.6.1 `book` is null

```bash
curl -i -X PUT "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 1001,
    "eventType": "UPDATE",
    "book": null
  }'
```

#### 2.6.2 `bookId` is null

```bash
curl -i -X PUT "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 1001,
    "eventType": "UPDATE",
    "book": {
      "bookId": null,
      "bookName": "Kafka Fundamentals - 2nd Edition",
      "bookAuthor": "Arshad Iqbal"
    }
  }'
```

#### 2.6.3 `bookId` is negative

```bash
curl -i -X PUT "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 1001,
    "eventType": "UPDATE",
    "book": {
      "bookId": -1,
      "bookName": "Kafka Fundamentals - 2nd Edition",
      "bookAuthor": "Arshad Iqbal"
    }
  }'
```

#### 2.6.4 `bookName` is empty string

```bash
curl -i -X PUT "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 1001,
    "eventType": "UPDATE",
    "book": {
      "bookId": 10,
      "bookName": "",
      "bookAuthor": "Arshad Iqbal"
    }
  }'
```

#### 2.6.5 `bookAuthor` is empty string

```bash
curl -i -X PUT "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 1001,
    "eventType": "UPDATE",
    "book": {
      "bookId": 10,
      "bookName": "Kafka Fundamentals - 2nd Edition",
      "bookAuthor": ""
    }
  }'
```

### 2.7 Multiple validation failures at once (400)

#### 2.7.1 `libraryEventId` null + `eventType` absent + `book` null

```bash
curl -i -X PUT "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": null,
    "book": null
  }'
```

#### 2.7.2 Invalid `libraryEventId` + invalid `eventType` + invalid `book` fields

```bash
curl -i -X PUT "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": -1,
    "eventType": "DELETE",
    "book": {
      "bookId": -99,
      "bookName": "",
      "bookAuthor": ""
    }
  }'
```

#### 2.7.3 All `book` fields invalid simultaneously

```bash
curl -i -X PUT "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 1001,
    "eventType": "UPDATE",
    "book": {
      "bookId": null,
      "bookName": "",
      "bookAuthor": ""
    }
  }'
```

#### 2.7.4 Completely empty JSON object `{}`

```bash
curl -i -X PUT "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d '{}'
```


```bash
curl -i -X POST "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 1001,
    "eventType": "ADD",
    "book": {
      "bookId": 10,
      "bookName": "Kafka Fundamentals",
      "bookAuthor": "Arshad Iqbal"
    }
  }'
```

### 1.2 PUT — valid UPDATE event (expected: 200 OK)

```bash
curl -i -X PUT "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 1001,
    "eventType": "UPDATE",
    "book": {
      "bookId": 10,
      "bookName": "Kafka Fundamentals - 2nd Edition",
      "bookAuthor": "Arshad Iqbal"
    }
  }'
```

---

## 2. Malformed / Unparseable Request Body (expected: 400 Bad Request)

### 2.1 Missing closing brace — malformed JSON

```bash
curl -i -X POST "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 1001,
    "eventType": "ADD",
    "book": {
      "bookId": 10,
      "bookName": "Kafka Fundamentals",
      "bookAuthor": "Arshad Iqbal"
    }
  '
```

### 2.2 Completely empty body

```bash
curl -i -X POST "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d ''
```

### 2.3 Plain text instead of JSON

```bash
curl -i -X POST "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d 'this is not json'
```

### 2.4 Wrong Content-Type header

```bash
curl -i -X POST "http://localhost:8080/v1/library-events" \
  -H "Content-Type: text/plain" \
  -d '{
    "libraryEventId": 1001,
    "eventType": "ADD",
    "book": {
      "bookId": 10,
      "bookName": "Kafka Fundamentals",
      "bookAuthor": "Arshad Iqbal"
    }
  }'
```

---

## 3. `libraryEventId` Validation Failures (expected: 400 Bad Request)

### 3.1 `libraryEventId` is null

```bash
curl -i -X POST "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": null,
    "eventType": "ADD",
    "book": {
      "bookId": 10,
      "bookName": "Kafka Fundamentals",
      "bookAuthor": "Arshad Iqbal"
    }
  }'
```

### 3.2 `libraryEventId` is zero (not positive)

```bash
curl -i -X POST "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 0,
    "eventType": "ADD",
    "book": {
      "bookId": 10,
      "bookName": "Kafka Fundamentals",
      "bookAuthor": "Arshad Iqbal"
    }
  }'
```

### 3.3 `libraryEventId` is negative

```bash
curl -i -X POST "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": -5,
    "eventType": "ADD",
    "book": {
      "bookId": 10,
      "bookName": "Kafka Fundamentals",
      "bookAuthor": "Arshad Iqbal"
    }
  }'
```

---

## 4. `eventType` Validation Failures (expected: 400 Bad Request)

### 4.1 `eventType` is absent (null)

```bash
curl -i -X POST "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 1001,
    "book": {
      "bookId": 10,
      "bookName": "Kafka Fundamentals",
      "bookAuthor": "Arshad Iqbal"
    }
  }'
```

### 4.2 `eventType` is an invalid enum value

```bash
curl -i -X POST "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 1001,
    "eventType": "DELETE",
    "book": {
      "bookId": 10,
      "bookName": "Kafka Fundamentals",
      "bookAuthor": "Arshad Iqbal"
    }
  }'
```

---

## 5. `book` Object Validation Failures (expected: 400 Bad Request)

### 5.1 `book` is null

```bash
curl -i -X POST "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 1001,
    "eventType": "ADD",
    "book": null
  }'
```

### 5.2 `bookId` is null

```bash
curl -i -X POST "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 1001,
    "eventType": "ADD",
    "book": {
      "bookId": null,
      "bookName": "Kafka Fundamentals",
      "bookAuthor": "Arshad Iqbal"
    }
  }'
```

### 5.3 `bookId` is negative

```bash
curl -i -X POST "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 1001,
    "eventType": "ADD",
    "book": {
      "bookId": -1,
      "bookName": "Kafka Fundamentals",
      "bookAuthor": "Arshad Iqbal"
    }
  }'
```

### 5.4 `bookName` is empty string

```bash
curl -i -X POST "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 1001,
    "eventType": "ADD",
    "book": {
      "bookId": 10,
      "bookName": "",
      "bookAuthor": "Arshad Iqbal"
    }
  }'
```

### 5.5 `bookAuthor` is empty string

```bash
curl -i -X POST "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 1001,
    "eventType": "ADD",
    "book": {
      "bookId": 10,
      "bookName": "Kafka Fundamentals",
      "bookAuthor": ""
    }
  }'
```

---

## 6. Multiple Validation Failures at Once (expected: 400 Bad Request)

### 6.1 `libraryEventId` null + `eventType` absent + `book` null (all top-level fields invalid)

```bash
curl -i -X POST "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": null,
    "book": null
  }'
```

### 6.2 Invalid `libraryEventId` + invalid `eventType` + invalid `book` fields

```bash
curl -i -X POST "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": -1,
    "eventType": "DELETE",
    "book": {
      "bookId": -99,
      "bookName": "",
      "bookAuthor": ""
    }
  }'
```

### 6.3 All `book` fields invalid simultaneously

```bash
curl -i -X POST "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 1001,
    "eventType": "ADD",
    "book": {
      "bookId": null,
      "bookName": "",
      "bookAuthor": ""
    }
  }'
```

### 6.4 Completely empty JSON object `{}` (all required fields missing)

```bash
curl -i -X POST "http://localhost:8080/v1/library-events" \
  -H "Content-Type: application/json" \
  -d '{}'
```
