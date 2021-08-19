package com.clairvoyant.weather.exception;

/**
 * @author Gufran Khan
 * @version 1.0
 * @date 19-08-2021 11:03
 */
public class NotFoundException extends RuntimeException {
    public NotFoundException(String message) {
        super(message);
    }
}
