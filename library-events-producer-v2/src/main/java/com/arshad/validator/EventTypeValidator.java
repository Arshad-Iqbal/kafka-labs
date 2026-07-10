package com.arshad.validator;

import com.arshad.model.EventType;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;

/**
 * Validator for the {@link ValidEventType} constraint.
 *
 * <p>Accepts only non-null {@link EventType} values that are either {@code ADD} or {@code UPDATE}.
 */
public class EventTypeValidator implements ConstraintValidator<ValidEventType, EventType> {

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize(ValidEventType constraintAnnotation) {
        ConstraintValidator.super.initialize(constraintAnnotation);
    }

    /**
     * Returns {@code true} if {@code value} is {@code ADD} or {@code UPDATE}; {@code false} otherwise.
     *
     * @param value   the {@link EventType} to validate
     * @param context constraint validator context (unused)
     * @return {@code true} if the value is valid
     */
    @Override
    public boolean isValid(EventType value, ConstraintValidatorContext context) {
        if (value == null) {
            return false;
        }
        
        try {
            return value == EventType.ADD || value == EventType.UPDATE;
        } catch (Exception e) {
            return false;
        }
    }
}
