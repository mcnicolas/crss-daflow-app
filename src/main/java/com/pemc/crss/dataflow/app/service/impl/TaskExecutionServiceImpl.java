package com.pemc.crss.dataflow.app.service.impl;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.pemc.crss.dataflow.app.dto.*;
import com.pemc.crss.dataflow.app.service.TaskExecutionService;
import com.pemc.crss.meterprocess.core.main.entity.BillingPeriod;
import com.pemc.crss.meterprocess.core.main.reference.MeterType;
import com.pemc.crss.meterprocess.core.main.repository.BillingPeriodRepository;
import com.pemc.crss.shared.commons.reference.MeterProcessType;
import com.pemc.crss.shared.commons.util.DateUtil;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobRunLock;
import com.pemc.crss.shared.core.dataflow.entity.StepProgress;
import com.pemc.crss.shared.core.dataflow.repository.BatchJobRunLockRepository;
import com.pemc.crss.shared.core.dataflow.repository.StepProgressRepository;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.data.domain.Pageable;
import org.springframework.hateoas.ResourceSupport;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestTemplate;

import java.net.URI;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created on 1/22/17.
 */
@Service
@Transactional(readOnly = true, value = "transactionManager")
public class TaskExecutionServiceImpl implements TaskExecutionService {

    private static final Logger LOG = LoggerFactory.getLogger(TaskExecutionServiceImpl.class);

    private static final String RUN_WESM_JOB_NAME = "computeWesmMq";
    private static final String RUN_RCOA_JOB_NAME = "computeRcoaMq";
    private static final String RUN_COMPUTE_STL_JOB_NAME = "computeSettlement";
    private static final String RUN_DATA_INTERFACE_JOB_NAME = "dataInterfaceJob";
    private static final String RUN_STL_READY_JOB_NAME = "processStlReady";
    private static final String DATE = "date";
    private static final String START_DATE = "startDate";
    private static final String END_DATE = "endDate";
    private static final String PROCESS_TYPE = "processType";
    private static final String PARENT_JOB = "parentJob";
    private static final String PROCESS_TYPE_DAILY = "DAILY";
    private static final String METER_TYPE = "meterType";
    private static final String SPRING_PROFILES_ACTIVE = "spring.profiles.active";
    private static final String QUOTE = "\"";

    private DateFormat dateFormat = new SimpleDateFormat(DateUtil.DEFAULT_DATE_FORMAT);

    @Autowired
    private JobExplorer jobExplorer;

    @Autowired
    private RestTemplate restTemplate;

    /**
     * Need to evaluate where to retrieve billing period.
     * Currently located at meterprocess db.
     */
    @Autowired
    private BillingPeriodRepository billingPeriodRepository;
    @Autowired
    private BatchJobRunLockRepository batchJobRunLockRepository;
    @Autowired
    private StepProgressRepository stepProgressRepository;

    @Autowired
    private Environment environment;

    @Value("${dataflow.url}")
    private String dataFlowUrl;
    @Value("${job.maxRun}")
    private int jobMaxRun;

    @Override
    public List<TaskExecutionDto> findJobInstances(Pageable pageable) {

        int count = 0;

        try {
            count = jobExplorer.getJobInstanceCount(RUN_WESM_JOB_NAME.concat("Daily"));
            count += jobExplorer.getJobInstanceCount(RUN_WESM_JOB_NAME.concat("Monthly"));
        } catch (NoSuchJobException e) {
            LOG.error("Exception: " + e);
        }

        List<TaskExecutionDto> taskExecutionDtos = Lists.newArrayList();

        if (count > 0) {
            taskExecutionDtos = jobExplorer.findJobInstancesByJobName(RUN_WESM_JOB_NAME.concat("*"),
                    pageable.getOffset(), pageable.getPageSize()).stream()
                    .map((JobInstance jobInstance) -> {

                        JobExecution jobExecution = jobExplorer.getJobExecutions(jobInstance).iterator().next();

                        TaskExecutionDto taskExecutionDto = new TaskExecutionDto();
                        taskExecutionDto.setId(jobInstance.getId());
                        taskExecutionDto.setRunDateTime(jobExecution.getStartTime());
                        taskExecutionDto.setParams(Maps.transformValues(
                                jobExecution.getJobParameters().getParameters(), JobParameter::getValue));
                        taskExecutionDto.setWesmStatus(jobExecution.getStatus());

                        if (taskExecutionDto.getWesmStatus().isRunning()) {
                            calculateProgress(jobExecution, taskExecutionDto);
                        } else if (taskExecutionDto.getWesmStatus().isUnsuccessful()) {
                            taskExecutionDto.setExitMessage(processFailedMessage(jobExecution));
                        } else if (taskExecutionDto.getWesmStatus() == BatchStatus.COMPLETED) {
                            taskExecutionDto.getSummary().put(RUN_WESM_JOB_NAME, showSummary(jobExecution));
                        }

                        taskExecutionDto.setStatus(convertStatus(taskExecutionDto.getWesmStatus(), "WESM"));

                        List<JobInstance> rcoaJobs = jobExplorer.findJobInstancesByJobName(
                                RUN_RCOA_JOB_NAME.concat("*-")
                                        .concat(jobInstance.getId().toString()), 0, 1);

                        if (!rcoaJobs.isEmpty()) {
                            JobExecution rcoaJobExecution = jobExplorer.getJobExecutions(rcoaJobs.get(0)).iterator().next();
                            taskExecutionDto.setRcoaStatus(rcoaJobExecution.getStatus());

                            if (taskExecutionDto.getRcoaStatus().isRunning()) {
                                calculateProgress(rcoaJobExecution, taskExecutionDto);
                            } else if (taskExecutionDto.getRcoaStatus().isUnsuccessful()) {
                                taskExecutionDto.setExitMessage(processFailedMessage(rcoaJobExecution));
                            } else if (taskExecutionDto.getRcoaStatus() == BatchStatus.COMPLETED) {
                                taskExecutionDto.getSummary().put(RUN_RCOA_JOB_NAME, showSummary(rcoaJobExecution));
                            }

                            taskExecutionDto.setStatus(convertStatus(taskExecutionDto.getRcoaStatus(), "RCOA"));
                        }

                        List<JobInstance> settlementJobs = jobExplorer.findJobInstancesByJobName(
                                RUN_STL_READY_JOB_NAME.concat("*-").concat(jobInstance.getId().toString()), 0, 1);

                        if (!settlementJobs.isEmpty()) {
                            JobExecution settlementJobExecution = jobExplorer.getJobExecutions(settlementJobs.get(0)).iterator().next();
                            taskExecutionDto.setSettlementStatus(settlementJobExecution.getStatus());

                            if (taskExecutionDto.getSettlementStatus().isUnsuccessful()) {
                                taskExecutionDto.setExitMessage(processFailedMessage(settlementJobExecution));
                            } else if (taskExecutionDto.getSettlementStatus() == BatchStatus.COMPLETED) {
                                taskExecutionDto.getSummary().put(RUN_STL_READY_JOB_NAME, showSummary(settlementJobExecution));
                            }
                            taskExecutionDto.setStatus(convertStatus(taskExecutionDto.getSettlementStatus(), "SETTLEMENT"));
                        }

                        return taskExecutionDto;

                    }).collect(Collectors.toList());
        }
        return taskExecutionDtos;
    }

    @Override
    public List<TaskExecutionDto> findSettlementJobInstances(Pageable pageable) {
        int count = 0;

        try {
            count = jobExplorer.getJobInstanceCount(RUN_COMPUTE_STL_JOB_NAME);
        } catch (NoSuchJobException e) {
            LOG.error("Exception: " + e);
        }

        List<TaskExecutionDto> taskExecutionDtos = Lists.newArrayList();
        if (count > 0) {
            taskExecutionDtos = jobExplorer.findJobInstancesByJobName(RUN_COMPUTE_STL_JOB_NAME,
                    pageable.getOffset(), pageable.getPageSize()).stream()
                    .map((JobInstance jobInstance) -> {

                        JobExecution jobExecution = jobExplorer.getJobExecutions(jobInstance).iterator().next();
                        BatchStatus status = jobExecution.getStatus();

                        TaskExecutionDto taskExecutionDto = new TaskExecutionDto();
                        taskExecutionDto.setId(jobInstance.getId());
                        taskExecutionDto.setRunDateTime(jobExecution.getStartTime());
                        taskExecutionDto.setParams(Maps.transformValues(
                                jobExecution.getJobParameters().getParameters(), JobParameter::getValue));
                        taskExecutionDto.setSettlementStatus(status);

                        if (status.isRunning()) {
//                            calculateProgress(jobExecution, taskExecutionDto);
                        } else if (status.isUnsuccessful()) {
                            taskExecutionDto.setExitMessage(jobExecution.getExitStatus().getExitDescription());
                        }

                        taskExecutionDto.setStatus(status.name());

                        return taskExecutionDto;

                    }).collect(Collectors.toList());
        }

        return taskExecutionDtos;
    }

    @Override
    public List<BillingPeriod> findBillingPeriods() {
        return billingPeriodRepository.findAll();
    }

    @Override
    @Transactional(value = "transactionManager")
    public void launchJob(TaskRunDto taskRunDto) throws URISyntaxException {
        Preconditions.checkNotNull(taskRunDto.getJobName());
        Preconditions.checkState(batchJobRunLockRepository.countByJobNameAndLockedIsTrue(taskRunDto.getJobName()) == 0,
                "There is an existing ".concat(taskRunDto.getJobName()).concat(" job running"));
        Preconditions.checkState(batchJobRunLockRepository.countByLockedIsTrue() < jobMaxRun,
                "Job already exceeds the maximum allowable concurrent run");

        String jobName = null;
        List<String> properties = Lists.newArrayList();
        List<String> arguments = Lists.newArrayList();
        if (RUN_DATA_INTERFACE_JOB_NAME.equals(taskRunDto.getJobName())) {
            arguments.add(concatKeyValue(START_DATE, StringUtils.containsWhitespace(taskRunDto.getStartDate()) ? QUOTE + taskRunDto.getStartDate() + QUOTE : taskRunDto.getStartDate(), "date"));
            arguments.add(concatKeyValue(END_DATE, StringUtils.containsWhitespace(taskRunDto.getEndDate()) ? QUOTE + taskRunDto.getEndDate() + QUOTE : taskRunDto.getEndDate(), "date"));
            arguments.add(concatKeyValue(PROCESS_TYPE, taskRunDto.getMarketInformationType()));

            //TODO create market info type enum in shared
            if (taskRunDto.getMarketInformationType().equals("energyPriceAndSchedule")) {
                String testProfile = fetchSpringProfilesActive("energyPriceSched");
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, testProfile));
            } else if (taskRunDto.getMarketInformationType().equals("reservePriceAndSchedule")) {
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("reservePriceSched")));
            } else if (taskRunDto.getMarketInformationType().equals("reserveBCQ")) {
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("reserveBCQ")));
            } else if (taskRunDto.getMarketInformationType().equals("actualDispatchData")) {
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("actualDispatchData")));
            }
            jobName = "data-interface-data-job";
        } else if (RUN_COMPUTE_STL_JOB_NAME.equals(taskRunDto.getJobName())) {
            String type = taskRunDto.getMeterProcessType();
            if (PROCESS_TYPE_DAILY.equals(type)) {
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("dailyCalculation")));
            } else {
                if (MeterProcessType.ADJUSTED.name().equals(type)) {
                    properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyAdjustedCalculation")));
                } else if (MeterProcessType.PRELIMINARY.name().equals(type)) {
                    properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyPrelimCalculation")));
                } else if (MeterProcessType.FINAL.name().equals(type)) {
                    properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyFinalCalculation")));
                }
            }
            arguments.add(concatKeyValue(PROCESS_TYPE, type));
            arguments.add(concatKeyValue(START_DATE, taskRunDto.getStartDate(), "date"));
            arguments.add(concatKeyValue(END_DATE, taskRunDto.getEndDate(), "date"));
            jobName = "crss-settlement-task-calculation";
        } else {
            if (RUN_WESM_JOB_NAME.equals(taskRunDto.getJobName())) {
                if (PROCESS_TYPE_DAILY.equals(taskRunDto.getMeterProcessType())) {
                    arguments.add(concatKeyValue(DATE, taskRunDto.getTradingDate(), "date"));
                    properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("dailyMq")));
                } else {
                    arguments.add(concatKeyValue(START_DATE, taskRunDto.getStartDate(), "date"));
                    arguments.add(concatKeyValue(END_DATE, taskRunDto.getEndDate(), "date"));
                    arguments.add(concatKeyValue(PROCESS_TYPE, taskRunDto.getMeterProcessType()));
                    properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyMq")));
                }
                arguments.add(concatKeyValue(METER_TYPE, MeterType.MIRF_MT_WESM.name()));
                jobName = "crss-meterprocess-task-mqcomputation";
            } else if (taskRunDto.getParentJob() != null) {
                JobInstance jobInstance = jobExplorer.getJobInstance(Long.valueOf(taskRunDto.getParentJob()));
                JobParameters jobParameters = jobExplorer.getJobExecutions(jobInstance).get(0).getJobParameters();
                if (jobParameters.getString(PROCESS_TYPE) == null) {
                    arguments.add(concatKeyValue(DATE, dateFormat.format(jobParameters.getDate(DATE)), "date"));
                } else {
                    arguments.add(concatKeyValue(START_DATE, dateFormat.format(jobParameters.getDate(START_DATE)), "date"));
                    arguments.add(concatKeyValue(END_DATE, dateFormat.format(jobParameters.getDate(END_DATE)), "date"));
                    arguments.add(concatKeyValue(PROCESS_TYPE, jobParameters.getString(PROCESS_TYPE)));
                }
                arguments.add(concatKeyValue(PARENT_JOB, taskRunDto.getParentJob(), "long"));
                if (RUN_RCOA_JOB_NAME.equals(taskRunDto.getJobName())) {
                    arguments.add(concatKeyValue(METER_TYPE, MeterType.MIRF_MT_RCOA.name()));
                    if (PROCESS_TYPE_DAILY.equals(taskRunDto.getMeterProcessType())) {
                        properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("dailyMq")));
                    } else {
                        properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyMq")));
                    }
                    jobName = "crss-meterprocess-task-mqcomputation";
                } else if (RUN_STL_READY_JOB_NAME.equals(taskRunDto.getJobName())) {
                    if (MeterProcessType.ADJUSTED.name().equals(taskRunDto.getMeterProcessType())) {
                        properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("dailyMq")));
                    } else if (MeterProcessType.ADJUSTED.name().equals(taskRunDto.getMeterProcessType())) {
                        properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyAdjusted")));
                    } else if (MeterProcessType.PRELIMINARY.name().equals(taskRunDto.getMeterProcessType())) {
                        properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyPrelim")));
                    } else if (MeterProcessType.FINAL.name().equals(taskRunDto.getMeterProcessType())) {
                        properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyFinal")));
                    }
                    jobName = "crss-meterprocess-task-stlready";
                }
            }
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

    @Override
    public List<DataInterfaceExecutionDTO> findDataInterfaceInstances(Pageable pageable) {

        int count = 0;

        try {
            count = jobExplorer.getJobInstanceCount("dataInterfaceJob");
        } catch (NoSuchJobException e) {
            LOG.error("Exception: " + e);
        }

        List<DataInterfaceExecutionDTO> dataInterfaceExecutionDTOs = Lists.newArrayList();

        if (count > 0) {
            dataInterfaceExecutionDTOs = jobExplorer.findJobInstancesByJobName("importEnergyPriceSchedJob",
                    pageable.getOffset(), pageable.getPageSize()).stream()
                    .map(jobInstance -> {

                        JobExecution jobExecution = jobExplorer.getJobExecutions(jobInstance).iterator().next();
                        DataInterfaceExecutionDTO dataInterfaceExecutionDTO = new DataInterfaceExecutionDTO();
                        dataInterfaceExecutionDTO.setId(jobInstance.getId());
                        dataInterfaceExecutionDTO.setRunStartDateTime(jobExecution.getStartTime());
                        dataInterfaceExecutionDTO.setRunEndDateTime(jobExecution.getEndTime());
                        dataInterfaceExecutionDTO.setStatus(jobExecution.getStatus().toString());
                        dataInterfaceExecutionDTO.setParams(Maps.transformValues(
                                jobExecution.getJobParameters().getParameters(), JobParameter::getValue));
                        dataInterfaceExecutionDTO.setBatchStatus(jobExecution.getStatus());

                        return dataInterfaceExecutionDTO;

                    }).collect(Collectors.toList());
        }

        return dataInterfaceExecutionDTOs;
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
                .sorted(Comparator.comparing(TaskSummaryDto::getStepId))
                .collect(Collectors.toList());
    }

    private void calculateProgress(JobExecution jobExecution, TaskExecutionDto taskExecutionDto) {
        TaskProgressDto progressDto = null;
        if (!jobExecution.getStepExecutions().isEmpty()) {
            StepExecution runningStep = jobExecution.getStepExecutions().parallelStream()
                    .filter(stepExecution -> stepExecution.getStatus().isRunning())
                    .filter(stepExecution -> stepExecution.getStepName().endsWith("Step"))
                    .findFirst().get();
            if (runningStep.getStepName().equals("computeMqStep")) {
                progressDto = processStepProgress(runningStep, "Generate raw mq data", "mqPartitionerTotal");
            } else if (runningStep.getStepName().equals("applySSLAStep")) {
                progressDto = processStepProgress(runningStep, "Applying SSLA Computation", "sslaPartitionerTotal");
            } else if (runningStep.getStepName().equals("generateReportStep")) {
                progressDto = processStepProgress(runningStep, "Generate Report", "reportPartitionerTotal");
            }
        }
        taskExecutionDto.setProgress(progressDto);
    }

    private String convertStatus(BatchStatus batchStatus, String suffix) {
        return batchStatus.toString().concat("-").concat(suffix);
    }

    private TaskProgressDto processStepProgress(StepExecution runningStep, String stepStr, String key) {
        TaskProgressDto progressDto = new TaskProgressDto();
        progressDto.setRunningStep(stepStr);
        progressDto.setExecutedCount(stepProgressRepository.findByStepId(runningStep.getId()).map(StepProgress::getChunkCount).orElse(0L));
        progressDto.setTotalCount(runningStep.getExecutionContext().getLong(key));
        return progressDto;
    }
}
