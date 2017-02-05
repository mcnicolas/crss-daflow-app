package com.pemc.crss.restresource.app.exception;

public class InvalidChannelConfigException extends Exception {
    public InvalidChannelConfigException() {
        super();
    }

    public InvalidChannelConfigException(String message) {
        super(message);
    }

    public InvalidChannelConfigException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidChannelConfigException(Throwable cause) {
        super(cause);
    }
}
