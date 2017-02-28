package com.pemc.crss.dataflow.app.service.impl;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.pemc.crss.dataflow.app.dto.*;
import com.pemc.crss.dataflow.app.service.TaskExecutionService;
import com.pemc.crss.meterprocess.core.main.entity.BillingPeriod;
import com.pemc.crss.shared.commons.reference.MarketInfoType;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobRunLock;
import com.pemc.crss.shared.core.dataflow.repository.BatchJobRunLockRepository;
import com.pemc.crss.shared.core.dataflow.repository.ExecutionParamRepository;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.LocalDateTime;
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
import java.util.*;

import static java.util.stream.Collectors.toList;

@Service("todiTaskExecutionServiceImpl")
@Transactional(readOnly = true, value = "transactionManager")
public class TodiTaskExecutionServiceImpl implements TaskExecutionService {

    private static final Logger LOG = LoggerFactory.getLogger(TaskExecutionServiceImpl.class);

    private static final String RUN_TODI_JOB_NAME = "import";
    private static final String START_DATE = "startDate";
    private static final String END_DATE = "endDate";
    private static final String QUOTE = "\"";
    private static final String MODE = "mode";
    private static final String RUN_ID = "run.id";
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
    ExecutionParamRepository executionParamRepository;

    @Autowired
    private RestTemplate restTemplate;
    @Autowired
    private BatchJobRunLockRepository batchJobRunLockRepository;
    @Autowired
    private Environment environment;
    @Autowired
    private RedisTemplate<String, Long> redisTemplate;

    @Value("${dataflow.url}")
    private String dataFlowUrl;

    @Value("${todi-config.dispatch-interval}")
    private String dispatchInterval;

    @Override
    @Transactional(value = "transactionManager")
    public void launchJob(TaskRunDto taskRunDto) throws URISyntaxException {
        Preconditions.checkNotNull(taskRunDto.getJobName());
        Preconditions.checkState(batchJobRunLockRepository.countByJobNameAndLockedIsTrue(taskRunDto.getJobName()) == 0,
                "There is an existing ".concat(taskRunDto.getJobName()).concat(" job running"));

        String jobName = null;
        List<String> properties = Lists.newArrayList();
        List<String> arguments = Lists.newArrayList();

        List<MarketInfoType> MARKET_INFO_TYPES = Arrays.asList(MarketInfoType.values());

        if (MARKET_INFO_TYPES.contains(MarketInfoType.getByJobName(taskRunDto.getJobName()))) {

            if (!StringUtils.isEmpty(taskRunDto.getStartDate())) {
                arguments.add(concatKeyValue(START_DATE, StringUtils.containsWhitespace(taskRunDto.getStartDate())
                        ? QUOTE + taskRunDto.getStartDate() + QUOTE : taskRunDto.getStartDate(), "date"));
                arguments.add(concatKeyValue(END_DATE, StringUtils.containsWhitespace(taskRunDto.getEndDate())
                        ? QUOTE + taskRunDto.getEndDate() + QUOTE : taskRunDto.getEndDate(), "date"));
                arguments.add(concatKeyValue(MODE, "Manual"));
            }

            arguments.add(concatKeyValue(RUN_ID, String.valueOf(System.currentTimeMillis()), "long"));
            properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(MarketInfoType
                    .getByJobName(taskRunDto.getJobName()).getProfileName())));

            jobName = "crss-datainterface-task-ingest";
        }

        LOG.debug("Running job name={}, properties={}, arguments={}", taskRunDto.getJobName(), properties, arguments);

        if (jobName != null) {
            ResourceSupport resourceSupport = restTemplate.getForObject(new URI(dataFlowUrl), ResourceSupport.class);
            restTemplate.postForObject(resourceSupport.getLink("tasks/deployments/deployment").expand(jobName).getHref()
                    .concat("?arguments={arguments}&properties={properties}"), null, Object.class, ImmutableMap.of("arguments",
                    StringUtils.join(arguments, ","), "properties", StringUtils.join(properties, ",")));
            if (batchJobRunLockRepository.lockJob(taskRunDto.getJobName()) == 0) {
                BatchJobRunLock batchJobRunLock = new BatchJobRunLock();
                batchJobRunLock.setLocked(true);
                batchJobRunLock.setLockedDate(new Date());
                batchJobRunLock.setJobName(taskRunDto.getJobName());
                batchJobRunLockRepository.save(batchJobRunLock);
            }
        }
    }

    @Override
    public Page<DataInterfaceExecutionDTO> findDataInterfaceInstances(Pageable pageable) {
        List<DataInterfaceExecutionDTO> dataInterfaceExecutionDTOs = new ArrayList<>();

        int count = 0;

        try {
            count += jobExplorer.getJobInstanceCount(RUN_TODI_JOB_NAME.concat("EnergyPriceSchedJob"));
            count += jobExplorer.getJobInstanceCount(RUN_TODI_JOB_NAME.concat("ReservePriceSchedJob"));
            count += jobExplorer.getJobInstanceCount(RUN_TODI_JOB_NAME.concat("ReserveBCQ"));
            count += jobExplorer.getJobInstanceCount(RUN_TODI_JOB_NAME.concat("RTUJob"));
        } catch (NoSuchJobException e) {
            LOG.error("Exception: " + e);
        }

        if (count > 0) {
            dataInterfaceExecutionDTOs
                    = jobExplorer.findJobInstancesByJobName(RUN_TODI_JOB_NAME.concat("*"),
                    pageable.getOffset(), pageable.getPageSize())
                    .stream().map((JobInstance jobInstance) -> {

                        DataInterfaceExecutionDTO dataInterfaceExecutionDTO = new DataInterfaceExecutionDTO();
                        JobExecution jobExecution = getJobExecutions(jobInstance).iterator().next();

                        Map jobParameters = Maps.transformValues(jobExecution.getJobParameters().getParameters(), JobParameter::getValue);
                        String jobName = jobExecution.getJobInstance().getJobName();
                        String mode = StringUtils.upperCase((String) jobParameters.getOrDefault(MODE, "AUTOMATIC"));

                        LocalDateTime runDate = new LocalDateTime(jobExecution.getStartTime());

                        Date tradingDayStart = !mode.equals("AUTOMATIC")?(Date)jobParameters.get("startDate")
                                : runDate.minusDays(1).withHourOfDay(00).withMinuteOfHour(05).toDate();
                        Date tradingDayEnd = !mode.equals("AUTOMATIC") ? (Date)jobParameters.get("endDate")
                                : runDate.withHourOfDay(00).withMinuteOfHour(00).toDate();

                        dataInterfaceExecutionDTO.setId(jobInstance.getId());
                        dataInterfaceExecutionDTO.setRunStartDateTime(jobExecution.getStartTime());
                        dataInterfaceExecutionDTO.setRunEndDateTime(jobExecution.getEndTime());
                        dataInterfaceExecutionDTO.setStatus(jobExecution.getStatus().toString());
                        dataInterfaceExecutionDTO.setParams(jobParameters);
                        dataInterfaceExecutionDTO.setBatchStatus(jobExecution.getStatus());
                        dataInterfaceExecutionDTO.setType(MarketInfoType.getByJobName(jobName).getLabel());
                        dataInterfaceExecutionDTO.setMode(mode);
                        dataInterfaceExecutionDTO.setTradingDayStart(tradingDayStart);
                        dataInterfaceExecutionDTO.setTradingDayEnd(tradingDayEnd);
                        setLogs(dataInterfaceExecutionDTO, jobExecution);

                        if (jobExecution.getStatus().isRunning()) {
                            calculateProgress(jobExecution, dataInterfaceExecutionDTO);
                        }

                        return dataInterfaceExecutionDTO;
                    }).collect(toList());
        }
        Collections.reverse(dataInterfaceExecutionDTOs);
        return new PageImpl<>(dataInterfaceExecutionDTOs, pageable, count);
    }

    @Override
    public int getDispatchInterval() {
        //TODO connect to global configuration to get dispatch-interval
        return Integer.valueOf(this.dispatchInterval);
    }

    @Override
    public List<BillingPeriod> findBillingPeriods() {
        return null;
    }

    @Override
    public Page<TaskExecutionDto> findJobInstances(Pageable pageable) {
        return null;
    }


    private void setLogs(DataInterfaceExecutionDTO executionDTO, JobExecution jobExecution) {
        StepExecution stepExecution = null;

        Collection<StepExecution> executionSteps = jobExecution.getStepExecutions();
        Iterator it = executionSteps.iterator();
        while(it.hasNext()) {
            StepExecution stepChecker = (StepExecution)it.next();
            if (stepChecker.getStepName().equals("step1")) {
                stepExecution = stepChecker;
                break;
            }
        }

        if (stepExecution != null) {
            executionDTO.setRecordsRead(stepExecution.getReadCount());
            executionDTO.setExpectedRecord(jobExecution.getExecutionContext().getInt("expected_record", 0));
            if (stepExecution.getJobExecution().getStatus().isUnsuccessful()) {
                executionDTO.setRecordsWritten(0);
            } else {
                executionDTO.setRecordsWritten(stepExecution.getWriteCount());
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

    private void calculateProgress(JobExecution jobExecution, TaskExecutionDto taskExecutionDto) {
        TaskProgressDto progressDto = null;
        List<MarketInfoType> MARKET_INFO_TYPES = Arrays.asList(MarketInfoType.values());
        if (MARKET_INFO_TYPES.contains(MarketInfoType.getByJobName(jobExecution.getJobInstance().getJobName()))) {
            StepExecution stepExecution = null;
            Collection<StepExecution> executionSteps = jobExecution.getStepExecutions();
            Iterator it = executionSteps.iterator();
            while(it.hasNext()) {
                StepExecution stepChecker = (StepExecution)it.next();
                if (stepChecker.getStepName().equals("step1")) {
                    stepExecution = stepChecker;
                    break;
                }
            }
            if (stepExecution != null) {
                if (stepExecution.getStepName().equals("step1")) {
                    progressDto = processStepProgress(stepExecution, "Importing Data", null);
                }
            }
            taskExecutionDto.setProgress(progressDto);
        }
        taskExecutionDto.setProgress(progressDto);
    }

    private TaskProgressDto processStepProgress(StepExecution runningStep, String stepStr, String key) {
        TaskProgressDto progressDto = new TaskProgressDto();
        progressDto.setRunningStep(stepStr);
        Long stepProg = redisTemplate.opsForValue().get(String.valueOf(runningStep.getId()));
        if (stepProg != null) {
            progressDto.setExecutedCount(Math.min(stepProg, progressDto.getTotalCount()));
            progressDto.setTotalCount(redisTemplate.opsForValue().get(runningStep.getId() + "_total"));
        }

        return progressDto;
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
