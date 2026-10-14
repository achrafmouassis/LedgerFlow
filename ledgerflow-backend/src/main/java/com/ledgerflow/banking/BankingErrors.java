package com.ledgerflow.banking;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;

@RestControllerAdvice
public class BankingErrors {
    @ExceptionHandler(BankingException.class)
    public ResponseEntity<ProblemDetail> banking(BankingException e) {
        ProblemDetail detail = ProblemDetail.forStatusAndDetail(e.status, e.getMessage());
        detail.setProperty("code", e.code);
        return ResponseEntity.status(e.status).body(detail);
    }
    @ExceptionHandler(DataIntegrityViolationException.class)
    public ResponseEntity<ProblemDetail> conflict(DataIntegrityViolationException e) {
        ProblemDetail detail = ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT,
            "Conflicting data; if retrying a transfer, reuse the same key and payload");
        detail.setProperty("code", "DATA_CONFLICT");
        return ResponseEntity.status(HttpStatus.CONFLICT).body(detail);
    }
}
