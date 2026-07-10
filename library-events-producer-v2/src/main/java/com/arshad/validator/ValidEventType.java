package com.arshad.validator;

import jakarta.validation.Constraint;
import jakarta.validation.Payload;
import java.lang.annotation.*;

/**
 * Custom Bean Validation constraint that ensures an {@link com.arshad.model.EventType} field
 * holds a recognised value ({@code ADD} or {@code UPDATE}).
 *
 * <p>Usage:
 * <pre>{@code
 * @ValidEventType
 * private EventType eventType;
 * }</pre>
 */
@Documented
@Constraint(validatedBy = EventTypeValidator.class)
@Target({ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
public @interface ValidEventType {
    String message() default "eventType must be a valid enum value";
    Class<?>[] groups() default {};
    Class<? extends Payload>[] payload() default {};
}
