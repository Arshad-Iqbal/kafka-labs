package com.arshad.model;

/**
 * Represents the type of a library event.
 *
 * <ul>
 *   <li>{@link #ADD} — a new book entry is being added to the library.</li>
 *   <li>{@link #UPDATE} — an existing book entry is being modified.</li>
 * </ul>
 */
public enum EventType {
    /** Indicates a new library entry is being created. */
    ADD,
    /** Indicates an existing library entry is being updated. */
    UPDATE
}
