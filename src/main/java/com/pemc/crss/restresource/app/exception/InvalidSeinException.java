package com.pemc.crss.restresource.app.exception;

public class InvalidSeinException extends Exception {
    public InvalidSeinException() {
        super();
    }

    public InvalidSeinException(String message) {
        super(message);
    }

    public InvalidSeinException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidSeinException(Throwable cause) {
        super(cause);
    }
}
