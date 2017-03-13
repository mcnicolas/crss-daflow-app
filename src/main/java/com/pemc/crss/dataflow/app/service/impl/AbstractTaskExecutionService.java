package com.pemc.crss.dataflow.app.service.impl;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.pemc.crss.dataflow.app.dto.BaseTaskExecutionDto;
import com.pemc.crss.dataflow.app.dto.TaskProgressDto;
import com.pemc.crss.dataflow.app.dto.TaskRunDto;
import com.pemc.crss.dataflow.app.dto.TaskSummaryDto;
import com.pemc.crss.dataflow.app.service.TaskExecutionService;
import com.pemc.crss.shared.commons.util.DateUtil;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobRunLock;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobSkipLog;
import com.pemc.crss.shared.core.dataflow.repository.BatchJobRunLockRepository;
import com.pemc.crss.shared.core.dataflow.repository.ExecutionParamRepository;
import org.apache.commons.lang3.StringUtils;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.repository.dao.ExecutionContextDao;
import org.springframework.batch.core.repository.dao.JobExecutionDao;
import org.springframework.batch.core.repository.dao.JobInstanceDao;
import org.springframework.batch.core.repository.dao.StepExecutionDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.hateoas.ResourceSupport;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestTemplate;

import java.net.URI;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;

/**
 * Created by jdimayuga on 03/03/2017.
 */
public abstract class AbstractTaskExecutionService implements TaskExecutionService {


    protected static final String QUOTE = "\"";
    protected static final String RUN_ID = "run.id";
    protected static final String SPRING_PROFILES_ACTIVE = "spring.profiles.active";
    protected static final String DATE = "date";
    protected static final String START_DATE = "startDate";
    protected static final String END_DATE = "endDate";
    protected static final String PROCESS_TYPE_DAILY = "DAILY";
    protected static final String PARENT_JOB = "parentJob";
    protected static final String METER_TYPE = "meterType";
    protected static final String PROCESS_TYPE = "processType";
    protected DateFormat dateTimeFormat = new SimpleDateFormat(DateUtil.DEFAULT_DATETIME_FORMAT);
    protected DateFormat dateFormat = new SimpleDateFormat(DateUtil.DEFAULT_DATE_FORMAT);

    @Autowired
    protected ExecutionParamRepository executionParamRepository;
    @Autowired
    protected JobExplorer jobExplorer;
    @Autowired
    protected JobInstanceDao jobInstanceDao;
    @Autowired
    protected JobExecutionDao jobExecutionDao;
    @Autowired
    protected StepExecutionDao stepExecutionDao;
    @Autowired
    protected ExecutionContextDao ecDao;
    @Autowired
    protected RestTemplate restTemplate;
    @Autowired
    protected BatchJobRunLockRepository batchJobRunLockRepository;
    @Autowired
    protected Environment environment;
    @Autowired
    protected RedisTemplate<String, Long> redisTemplate;

    @Value("${dataflow.url}")
    protected String dataFlowUrl;

    @Value("${todi-config.dispatch-interval}")
    protected String dispatchInterval;

    @Override
    public abstract Page<? extends BaseTaskExecutionDto> findJobInstances(Pageable pageable);

    @Override
    public abstract void launchJob(TaskRunDto taskRunDto) throws URISyntaxException;

    @Override
    public int getDispatchInterval() {
        //TODO connect to global configuration to get dispatch-interval
        return Integer.valueOf(this.dispatchInterval);
    }


    @Override
    @Transactional(value = "transactionManager")
    public void deleteJob(long jobId) {
        executionParamRepository.deleteCascadeJob(jobId);
    }


    @Override
    public List<BatchJobSkipLog> getBatchJobSkipLogs(int stepId) {
        return executionParamRepository.getBatchJobSkipLogs(stepId);
    }

    protected String convertStatus(BatchStatus batchStatus, String suffix) {
        return batchStatus.toString().concat("-").concat(suffix);
    }

    protected TaskProgressDto processStepProgress(StepExecution runningStep, String stepStr, String key) {
        TaskProgressDto progressDto = new TaskProgressDto();
        progressDto.setRunningStep(stepStr);
        Long stepProg = redisTemplate.opsForValue().get(String.valueOf(runningStep.getId()));
        if (stepProg != null) {
            progressDto.setTotalCount(redisTemplate.opsForValue().get(runningStep.getId() + "_total"));
            progressDto.setExecutedCount(Math.min(stepProg,
                    progressDto.getTotalCount()));
        }
        return progressDto;
    }

    protected List<JobExecution> getJobExecutions(JobInstance jobInstance) {
        List<JobExecution> executions = jobExecutionDao.findJobExecutions(jobInstance);
        for (JobExecution jobExecution : executions) {
            getJobExecutionDependencies(jobExecution);
            for (StepExecution stepExecution : jobExecution.getStepExecutions()) {
                getStepExecutionDependencies(stepExecution);
            }
        }
        return executions;
    }

    protected void getJobExecutionDependencies(JobExecution jobExecution) {
        JobInstance jobInstance = jobInstanceDao.getJobInstance(jobExecution);
        stepExecutionDao.addStepExecutions(jobExecution);
        jobExecution.setJobInstance(jobInstance);
        jobExecution.setExecutionContext(ecDao.getExecutionContext(jobExecution));
    }

    protected void getStepExecutionDependencies(StepExecution stepExecution) {
        if (stepExecution != null && stepExecution.getStepName().endsWith("Step")) {
            stepExecution.setExecutionContext(ecDao.getExecutionContext(stepExecution));
        }
    }

    protected List<TaskSummaryDto> showSummary(JobExecution jobExecution) {
        return jobExecution.getStepExecutions().parallelStream()
                .filter(stepExecution -> stepExecution.getStepName().endsWith("Step"))
                .map(stepExecution -> {
                    TaskSummaryDto taskSummaryDto = new TaskSummaryDto();
                    taskSummaryDto.setStepName(stepExecution.getStepName());
                    taskSummaryDto.setReadCount(stepExecution.getReadCount());
                    taskSummaryDto.setWriteCount(stepExecution.getWriteCount());
                    taskSummaryDto.setSkipCount(stepExecution.getSkipCount());
                    taskSummaryDto.setStepId(stepExecution.getId());
                    return taskSummaryDto;
                })
                .sorted(comparing(TaskSummaryDto::getStepId))
                .collect(toList());
    }


    protected String fetchSpringProfilesActive(String profile) {
        List<String> profiles = Lists.newArrayList(environment.getActiveProfiles());
        profiles.add(profile);
        return StringUtils.join(profiles, ",");
    }

    protected String concatKeyValue(String key, String value, String dataType) {
        return key.concat(dataType != null ? "(".concat(dataType).concat(")") : "").concat("=").concat(value);
    }

    protected String concatKeyValue(String key, String value) {
        return concatKeyValue(key, value, null);
    }

    protected String processFailedMessage(JobExecution jobExecution) {
        return jobExecution.getStepExecutions().parallelStream()
                .filter(stepExecution -> stepExecution.getStepName().matches("(.*)StepPartition(.*)"))
                .filter(stepExecution -> stepExecution.getStatus().isUnsuccessful())
                .findFirst().map(stepExecution -> stepExecution.getExitStatus().getExitDescription()).orElse(null);
    }

    protected void calculateProgress(JobExecution jobExecution, BaseTaskExecutionDto taskExecutionDto) {
        TaskProgressDto progressDto = null;
        if (!jobExecution.getStepExecutions().isEmpty()) {
            StepExecution runningStep = jobExecution.getStepExecutions().parallelStream()
                    .filter(stepExecution -> stepExecution.getStatus().isRunning())
                    .filter(stepExecution -> stepExecution.getStepName().endsWith("Step"))
                    .findFirst().get();
            if (runningStep.getStepName().equals("processGapStep")) {
                progressDto = processStepProgress(runningStep, "Generate gap records", "gapPartitionerTotal");
            } else if (runningStep.getStepName().equals("computeMqStep")) {
                progressDto = processStepProgress(runningStep, "Generate raw mq data", "mqPartitionerTotal");
            } else if (runningStep.getStepName().equals("applySSLAStep")) {
                progressDto = processStepProgress(runningStep, "Applying SSLA Computation", "sslaPartitionerTotal");
            } else if (runningStep.getStepName().equals("generateReportStep")) {
                progressDto = processStepProgress(runningStep, "Generate Report", "reportPartitionerTotal");
            }
        }
        taskExecutionDto.setProgress(progressDto);
    }

    protected void doLaunchAndLockJob(TaskRunDto taskRunDto, String jobName, List<String> properties, List<String> arguments) throws URISyntaxException {
        ResourceSupport resourceSupport = restTemplate.getForObject(new URI(dataFlowUrl), ResourceSupport.class);
        restTemplate.postForObject(resourceSupport.getLink("tasks/deployments/deployment").expand(jobName).getHref().concat(
                "?arguments={arguments}&properties={properties}"), null, Object.class, ImmutableMap.of("arguments", StringUtils.join(arguments, ","),
                "properties", StringUtils.join(properties, ",")));
        if (batchJobRunLockRepository.lockJob(taskRunDto.getJobName()) == 0) {
            BatchJobRunLock batchJobRunLock = new BatchJobRunLock();
            batchJobRunLock.setJobName(taskRunDto.getJobName());
            batchJobRunLock.setLocked(true);
            batchJobRunLock.setLockedDate(new Date());
            batchJobRunLockRepository.save(batchJobRunLock);
        }

    }


}