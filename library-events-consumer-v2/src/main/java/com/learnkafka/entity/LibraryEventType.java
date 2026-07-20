package com.learnkafka.entity;

/**
 * Represents the type of a library event stored in the database.
 *
 * <ul>
 *   <li>{@link #ADD} — a new book entry is being added to the library.</li>
 *   <li>{@link #UPDATE} — an existing book entry is being modified.</li>
 * </ul>
 */
public enum LibraryEventType {
    ADD,
    UPDATE
}
