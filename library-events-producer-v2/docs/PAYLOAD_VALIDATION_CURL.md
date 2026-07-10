# Payload Validation — cURL Test Cases

---

## 1. Happy Path

### 1.1 POST — valid ADD event (expected: 201 Created)

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
