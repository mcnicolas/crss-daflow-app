package com.pemc.crss.dataflow.app.jobqueue.impl;

import com.pemc.crss.dataflow.app.jobqueue.SchedulerService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class SchedulerServiceImpl implements SchedulerService {

    @Override
    public void execute() {
        log.info("Executing Service...");
    }
}
