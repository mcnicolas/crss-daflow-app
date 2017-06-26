package com.pemc.crss.dataflow.app.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

@Component
public class ShutdownHouseKeeper {

    @Autowired
    private RedisTemplate<String, Long> redisTemplate;

    @EventListener(ContextClosedEvent.class)
    public void contextRefreshedEvent() {
        // should'nt be null since it is incremented on startup
        long subsCount = redisTemplate.opsForValue().get("retryQueueSubscriberCount");
        // decrement retryQueueSubscriberCount on every graceful shutdown
        redisTemplate.opsForValue().set("retryQueueSubscriberCount", subsCount - 1);
    }
}
