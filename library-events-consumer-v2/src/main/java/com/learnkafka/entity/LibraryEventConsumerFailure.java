package com.learnkafka.entity;

import jakarta.persistence.*;

import java.time.LocalDateTime;

@Entity
@Table(name = "library_event_consumer_failure")
public class LibraryEventConsumerFailure {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "topic", nullable = false)
    private String topic;

    @Column(name = "partition", nullable = false)
    private Integer partition;

    @Column(name = "offset_value", nullable = false)
    private Long offsetValue;

    @Column(name = "record_key")
    private String recordKey;

    @Column(name = "record_value", columnDefinition = "TEXT")
    private String recordValue;

    @Column(name = "exception_class", nullable = false)
    private String exceptionClass;

    @Column(name = "exception_message", columnDefinition = "TEXT")
    private String exceptionMessage;

    @Column(name = "failed_at", nullable = false, updatable = false)
    private LocalDateTime failedAt;

    @PrePersist
    protected void onCreate() {
        failedAt = LocalDateTime.now();
    }

    public LibraryEventConsumerFailure() {
    }

    public LibraryEventConsumerFailure(String topic, Integer partition, Long offsetValue,
                                       String recordKey, String recordValue,
                                       String exceptionClass, String exceptionMessage) {
        this.topic = topic;
        this.partition = partition;
        this.offsetValue = offsetValue;
        this.recordKey = recordKey;
        this.recordValue = recordValue;
        this.exceptionClass = exceptionClass;
        this.exceptionMessage = exceptionMessage;
    }

    public Long getId() { return id; }
    public String getTopic() { return topic; }
    public Integer getPartition() { return partition; }
    public Long getOffsetValue() { return offsetValue; }
    public String getRecordKey() { return recordKey; }
    public String getRecordValue() { return recordValue; }
    public String getExceptionClass() { return exceptionClass; }
    public String getExceptionMessage() { return exceptionMessage; }
    public LocalDateTime getFailedAt() { return failedAt; }

    @Override
    public String toString() {
        return "LibraryEventConsumerFailure{id=" + id +
                ", topic='" + topic + '\'' +
                ", partition=" + partition +
                ", offsetValue=" + offsetValue +
                ", exceptionClass='" + exceptionClass + '\'' +
                ", failedAt=" + failedAt + '}';
    }
}
