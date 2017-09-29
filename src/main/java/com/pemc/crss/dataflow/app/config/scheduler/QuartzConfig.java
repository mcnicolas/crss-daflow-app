package com.pemc.crss.dataflow.app.config.scheduler;

import lombok.extern.slf4j.Slf4j;
import org.quartz.DateBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.impl.StdSchedulerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.quartz.SpringBeanJobFactory;

import java.io.IOException;

@Slf4j
@Configuration
public class QuartzConfig {

    @Autowired
    private ApplicationContext applicationContext;

    @Value("${scheduler.interval-seconds}")
    private int intervalSeconds;

    @Bean
    public SpringBeanJobFactory springBeanJobFactory() {
        AutoWiringSpringBeanJobFactory jobFactory = new AutoWiringSpringBeanJobFactory();
        log.info("Configuring Job factory");

        jobFactory.setApplicationContext(applicationContext);
        return jobFactory;
    }


    @Bean
    public Scheduler scheduler(@Autowired final Trigger trigger, @Autowired final JobDetail job)
            throws SchedulerException, IOException {

        StdSchedulerFactory factory = new StdSchedulerFactory();

        Scheduler scheduler = factory.getScheduler();
        scheduler.setJobFactory(springBeanJobFactory());
        scheduler.scheduleJob(job, trigger);

        log.info("Starting Scheduler");
        scheduler.start();

        return scheduler;
    }

    @Bean
    public JobDetail jobDetail() {

        return JobBuilder.newJob()
                .ofType(QueueJob.class)
                .storeDurably()
                .withIdentity(JobKey.jobKey("QUEUE_JOB_DETAIL"))
                .withDescription("Queue Job Detail")
                .build();
    }

    @Bean
    public Trigger trigger(@Autowired final JobDetail jobDetail) {

        return TriggerBuilder.newTrigger()
                .forJob(jobDetail)
                .withIdentity(TriggerKey.triggerKey("QUEUE_JOB_TRIGGER"))
                .startAt(DateBuilder.evenMinuteDate(null))
                .withDescription("Queue Job Trigger ")
                .withSchedule(SimpleScheduleBuilder.simpleSchedule()
                        .withIntervalInSeconds(intervalSeconds)
                        .repeatForever())
                .build();
    }






}
