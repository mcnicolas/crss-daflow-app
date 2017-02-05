package com.pemc.crss.restresource.app.exception;

public class InvalidMtnLocationException extends Exception {
    public InvalidMtnLocationException() {
        super();
    }

    public InvalidMtnLocationException(String message) {
        super(message);
    }

    public InvalidMtnLocationException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidMtnLocationException(Throwable cause) {
        super(cause);
    }
}
