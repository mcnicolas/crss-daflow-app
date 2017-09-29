package com.pemc.crss.dataflow.app.config.scheduler;

import com.pemc.crss.dataflow.app.jobqueue.SchedulerService;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class QueueJob implements Job {

    @Autowired
    private SchedulerService schedulerService;

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        schedulerService.execute();
    }
}
