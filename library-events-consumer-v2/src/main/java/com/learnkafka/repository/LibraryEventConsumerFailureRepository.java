package com.learnkafka.repository;

import com.learnkafka.entity.LibraryEventConsumerFailure;
import org.springframework.data.jpa.repository.JpaRepository;

public interface LibraryEventConsumerFailureRepository extends JpaRepository<LibraryEventConsumerFailure, Long> {
}
