package com.finpulse.api.exception;

public class ApiError {
    public String error;
    public String message;

    public ApiError(String error, String message) {
        this.error = error;
        this.message = message;
    }
}
