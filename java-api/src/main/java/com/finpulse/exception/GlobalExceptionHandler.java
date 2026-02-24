package com.finpulse.api.exception;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ApiError> handleIllegalArgument(IllegalArgumentException ex) {
        String msg = ex.getMessage() == null ? "" : ex.getMessage();

        if (msg.startsWith("TICKER_NOT_FOUND:")) {
            String ticker = msg.substring("TICKER_NOT_FOUND:".length());
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(new ApiError("TICKER_NOT_FOUND", "Ticker not found: " + ticker));
        }

        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(new ApiError("BAD_REQUEST", msg));
    }
}