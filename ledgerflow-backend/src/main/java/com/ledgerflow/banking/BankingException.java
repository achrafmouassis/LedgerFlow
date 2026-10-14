package com.ledgerflow.banking;

import org.springframework.http.HttpStatus;

class BankingException extends RuntimeException {
    final HttpStatus status;
    final String code;
    BankingException(HttpStatus status, String code, String message) {
        super(message); this.status = status; this.code = code;
    }
}
