package com.pemc.crss.dataflow.app.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;

import java.nio.ByteBuffer;

/**
 * Created by jdimayuga on 14/03/2017.
 */

public class TaskRetryListener implements MessageListener {

    private static final Logger LOG = LoggerFactory.getLogger(TaskRetryListener.class);

    @Override
    public void onMessage(Message message, byte[] bytes) {
        LOG.debug("Message received: {}", message.toString());
    }
}
