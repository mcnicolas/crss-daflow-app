package com.pemc.crss.restresource.app.exception;

public class InvalidMtnException extends Exception {

    public InvalidMtnException() {
        super();
    }

    public InvalidMtnException(String message) {
        super(message);
    }

    public InvalidMtnException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidMtnException(Throwable cause) {
        super(cause);
    }
}
