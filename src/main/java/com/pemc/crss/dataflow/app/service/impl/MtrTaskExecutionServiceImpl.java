package com.pemc.crss.dataflow.app.service.impl;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.pemc.crss.dataflow.app.dto.*;
import com.pemc.crss.dataflow.app.service.MtrTaskExecutionService;
import com.pemc.crss.shared.commons.util.DateUtil;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobRunLock;
import com.pemc.crss.shared.core.dataflow.repository.BatchJobRunLockRepository;
import com.pemc.crss.shared.core.dataflow.repository.ExecutionParamRepository;
import com.pemc.crss.shared.core.nmms.repository.EnergyPriceSchedRepository;
import com.pemc.crss.shared.core.nmms.repository.ReservePriceSchedRepository;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.repository.dao.ExecutionContextDao;
import org.springframework.batch.core.repository.dao.JobExecutionDao;
import org.springframework.batch.core.repository.dao.JobInstanceDao;
import org.springframework.batch.core.repository.dao.StepExecutionDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.hateoas.ResourceSupport;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestTemplate;

import java.net.URI;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;

@Service("mtrTaskExecutionServiceImpl")
@Transactional(readOnly = true, value = "transactionManager")
public class MtrTaskExecutionServiceImpl implements MtrTaskExecutionService {

    private static final Logger LOG = LoggerFactory.getLogger(TaskExecutionServiceImpl.class);

    private static final String RUN_MTR_JOB_NAME  = "generateMTR";
    private static final String DATE = "date";
    private static final String START_DATE = "startDate";
    private static final String END_DATE = "endDate";
    private static final String PROCESS_TYPE_DAILY = "DAILY";
    private static final String SPRING_PROFILES_ACTIVE = "spring.profiles.active";

    @Autowired
    private JobExplorer jobExplorer;
    @Autowired
    private JobInstanceDao jobInstanceDao;
    @Autowired
    private JobExecutionDao jobExecutionDao;
    @Autowired
    private StepExecutionDao stepExecutionDao;
    @Autowired
    private ExecutionContextDao ecDao;
    @Autowired
    EnergyPriceSchedRepository energyPriceSchedRepository;
    @Autowired
    ReservePriceSchedRepository reservePriceSchedRepository;
    @Autowired
    ExecutionParamRepository executionParamRepository;

    @Autowired
    private RestTemplate restTemplate;

    /**
     * Need to evaluate where to retrieve billing period.
     * Currently located at meterprocess db.
     */
    @Autowired
    private BatchJobRunLockRepository batchJobRunLockRepository;
    @Autowired
    private Environment environment;
    @Autowired
    private RedisTemplate<String, Long> redisTemplate;

    @Value("${dataflow.url}")
    private String dataFlowUrl;

    @Override
    public Page<TaskExecutionDto> findJobInstances(Pageable pageable) {
        int count = 0;

        try {
            count = jobExplorer.getJobInstanceCount(RUN_MTR_JOB_NAME.concat("Daily"));
            count += jobExplorer.getJobInstanceCount(RUN_MTR_JOB_NAME.concat("Monthly"));
        } catch (NoSuchJobException e) {
            LOG.error("Exception: " + e);
        }

        List<TaskExecutionDto> taskExecutionDtos = Lists.newArrayList();

        if (count > 0) {
            taskExecutionDtos = jobExplorer.findJobInstancesByJobName(RUN_MTR_JOB_NAME.concat("*"),
                    pageable.getOffset(), pageable.getPageSize()).stream()
                    .map((JobInstance jobInstance) -> {

                        JobExecution jobExecution = getJobExecutions(jobInstance).iterator().next();

                        TaskExecutionDto taskExecutionDto = new TaskExecutionDto();
                        taskExecutionDto.setId(jobInstance.getId());
                        taskExecutionDto.setRunDateTime(jobExecution.getStartTime());
                        taskExecutionDto.setParams(Maps.transformValues(
                                jobExecution.getJobParameters().getParameters(), JobParameter::getValue));

                        List<JobInstance> mtrJobs = jobExplorer.findJobInstancesByJobName(
                                RUN_MTR_JOB_NAME.concat("*-").concat(jobInstance.getId().toString()), 0, 1);

                        if (!mtrJobs.isEmpty()) {
                            JobExecution mtrJobExecution = getJobExecutions(mtrJobs.get(0)).iterator().next();
                            taskExecutionDto.setMtrStatus(mtrJobExecution.getStatus());

                            if (taskExecutionDto.getMtrStatus().isUnsuccessful()) {
                                taskExecutionDto.setExitMessage(processFailedMessage(mtrJobExecution));
                            } else if (taskExecutionDto.getMtrStatus() == BatchStatus.COMPLETED) {
                                taskExecutionDto.getSummary().put(RUN_MTR_JOB_NAME, showSummary(mtrJobExecution));
                            }
                        }

                        return taskExecutionDto;

                    }).collect(toList());
        }
        return new PageImpl<>(taskExecutionDtos, pageable, count);
    }

    @Override
    @Transactional(value = "transactionManager")
    public void launchJob(TaskRunDto taskRunDto) throws URISyntaxException {
        Preconditions.checkNotNull(taskRunDto.getJobName());
        Preconditions.checkState(batchJobRunLockRepository.countByJobNameAndLockedIsTrue(taskRunDto.getJobName()) == 0,
                "There is an existing ".concat(taskRunDto.getJobName()).concat(" job running"));

        String jobName = null;
        List<String> properties = Lists.newArrayList();
        List<String> arguments = Lists.newArrayList();

        if (RUN_MTR_JOB_NAME.equals(taskRunDto.getJobName())) {
            if (PROCESS_TYPE_DAILY.equals(taskRunDto.getMeterProcessType())) {
                arguments.add(concatKeyValue(DATE, taskRunDto.getTradingDate(), "date"));
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("dailyMtr")));
            } else {
                arguments.add(concatKeyValue(START_DATE, taskRunDto.getStartDate(), "date"));
                arguments.add(concatKeyValue(END_DATE, taskRunDto.getEndDate(), "date"));
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyMtr")));
            }
            jobName = "crss-meterprocess-task-mtr";
        }

        LOG.debug("Running job name={}, properties={}, arguments={}", taskRunDto.getJobName(), properties, arguments);

        if (jobName != null) {
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

    private String fetchSpringProfilesActive(String profile) {
        List<String> profiles = Lists.newArrayList(environment.getActiveProfiles());
        profiles.add(profile);
        return StringUtils.join(profiles, ",");
    }

    private String concatKeyValue(String key, String value, String dataType) {
        return key.concat(dataType != null ? "(".concat(dataType).concat(")") : "").concat("=").concat(value);
    }

    private String concatKeyValue(String key, String value) {
        return concatKeyValue(key, value, null);
    }

    private String processFailedMessage(JobExecution jobExecution) {
        return jobExecution.getStepExecutions().parallelStream()
                .filter(stepExecution -> stepExecution.getStepName().matches("(.*)StepPartition(.*)"))
                .filter(stepExecution -> stepExecution.getStatus().isUnsuccessful())
                .findFirst().map(stepExecution -> stepExecution.getExitStatus().getExitDescription()).orElse(null);
    }

    private List<TaskSummaryDto> showSummary(JobExecution jobExecution) {
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

    private List<JobExecution> getJobExecutions(JobInstance jobInstance) {
        List<JobExecution> executions = jobExecutionDao.findJobExecutions(jobInstance);
        for (JobExecution jobExecution : executions) {
            getJobExecutionDependencies(jobExecution);
            for (StepExecution stepExecution : jobExecution.getStepExecutions()) {
                getStepExecutionDependencies(stepExecution);
            }
        }
        return executions;
    }

    private void getJobExecutionDependencies(JobExecution jobExecution) {
        JobInstance jobInstance = jobInstanceDao.getJobInstance(jobExecution);
        stepExecutionDao.addStepExecutions(jobExecution);
        jobExecution.setJobInstance(jobInstance);
        jobExecution.setExecutionContext(ecDao.getExecutionContext(jobExecution));
    }

    private void getStepExecutionDependencies(StepExecution stepExecution) {
        if (stepExecution != null && stepExecution.getStepName().endsWith("Step")) {
            stepExecution.setExecutionContext(ecDao.getExecutionContext(stepExecution));
        }
    }
}
