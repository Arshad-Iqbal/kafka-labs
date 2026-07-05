package com.arshad.validator;

import com.arshad.model.EventType;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;

public class EventTypeValidator implements ConstraintValidator<ValidEventType, EventType> {

    @Override
    public void initialize(ValidEventType constraintAnnotation) {
        ConstraintValidator.super.initialize(constraintAnnotation);
    }

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
