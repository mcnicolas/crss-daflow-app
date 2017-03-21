package com.pemc.crss.dataflow.app.listener;

import com.pemc.crss.dataflow.app.service.TaskExecutionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.stereotype.Component;

/**
 * Created by jdimayuga on 14/03/2017.
 */
@Component
public class TaskRetryListener implements MessageListener {

    private static final Logger LOG = LoggerFactory.getLogger(TaskRetryListener.class);

    @Autowired
    @Qualifier("dataInterfaceTaskExecutionService")
    private TaskExecutionService taskExecutionService;

    @Override
    public void onMessage(Message message, byte[] bytes) {
        LOG.debug("Message received: {}", message.toString());
        try {
            taskExecutionService.relaunchFailedJob(Long.valueOf(message.toString()));
        } catch (Exception e) {
            LOG.debug("Failed to retry failed job: ", e.getMessage());
        }
    }
}
