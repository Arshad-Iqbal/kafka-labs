CREATE TABLE library_event_consumer_failure (
    id                BIGSERIAL    PRIMARY KEY,
    topic             VARCHAR(255) NOT NULL,
    partition         INTEGER      NOT NULL,
    offset_value      BIGINT       NOT NULL,
    record_key        VARCHAR(255),
    record_value      TEXT,
    exception_class   VARCHAR(512) NOT NULL,
    exception_message TEXT,
    failed_at         TIMESTAMP    NOT NULL DEFAULT now()
);
