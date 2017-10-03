package com.pemc.crss.dataflow.app.exception;

public class JobAlreadyOnQueueException extends RuntimeException {

    public JobAlreadyOnQueueException() {
    }

    public JobAlreadyOnQueueException(String message) {
        super(message);
    }
}
