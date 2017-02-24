package com.pemc.crss.dataflow.app.service.impl;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.pemc.crss.dataflow.app.dto.*;
import com.pemc.crss.dataflow.app.service.TaskExecutionService;
import com.pemc.crss.meterprocess.core.main.entity.BillingPeriod;
import com.pemc.crss.meterprocess.core.main.repository.BillingPeriodRepository;
import com.pemc.crss.shared.commons.reference.MeterProcessType;
import com.pemc.crss.shared.commons.util.DateUtil;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobRunLock;
import com.pemc.crss.shared.core.dataflow.repository.BatchJobRunLockRepository;
import com.pemc.crss.shared.core.dataflow.repository.StepProgressRepository;
import com.pemc.crss.shared.core.nmms.repository.EnergyPriceSchedRepository;
import com.pemc.crss.shared.core.nmms.repository.ReservePriceSchedRepository;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.explore.JobExplorer;
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
import org.springframework.hateoas.ResourceSupport;
import org.springframework.stereotype.Service;
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

@Service("stlTaskExecutionServiceImpl")
@Transactional(readOnly = true, value = "transactionManager")
public class StlTaskExecutionServiceImpl implements TaskExecutionService {

    private static final Logger LOG = LoggerFactory.getLogger(TaskExecutionServiceImpl.class);

    private static final String RUN_COMPUTE_STL_JOB_NAME = "computeSettlement";
    private static final String RUN_GENERATE_INVOICE_STL_JOB_NAME = "generateInvoiceSettlement";
    private static final String RUN_FINALIZE_STL_JOB_NAME = "finalizeSettlement";

    private static final String RUN_STL_READY_JOB_NAME = "processStlReady";
    private static final String START_DATE = "startDate";
    private static final String END_DATE = "endDate";
    private static final String AMS_INVOICE_DATE = "amsInvoiceDate";
    private static final String AMS_DUE_DATE = "amsDueDate";
    private static final String AMS_REMARKS = "amsRemarks";
    private static final String PROCESS_TYPE = "processType";
    private static final String PARENT_JOB = "parentJob";
    private static final String PROCESS_TYPE_DAILY = "DAILY";
    private static final String RUN_ID = "run.id";
    private static final String SPRING_PROFILES_ACTIVE = "spring.profiles.active";

    private DateFormat dateFormat = new SimpleDateFormat(DateUtil.DEFAULT_DATE_FORMAT);
    private DateFormat dateTimeFormat = new SimpleDateFormat(DateUtil.DEFAULT_DATETIME_FORMAT);

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

    @Override
    public Page<TaskExecutionDto> findJobInstances(Pageable pageable) {
        List<TaskExecutionDto> taskExecutionDtos = jobExplorer.findJobInstancesByJobName(RUN_STL_READY_JOB_NAME.concat("*"),
                pageable.getOffset(), pageable.getPageSize()).stream()
                .map((JobInstance jobInstance) -> {
                    JobExecution jobExecution = getJobExecutions(jobInstance).iterator().next();
                    BatchStatus jobStatus = jobExecution.getStatus();

                    String parentId = jobInstance.getJobName().split("-")[1];
                    if (StringUtils.isEmpty(parentId)) {
                        LOG.warn("Parent id not appended for job instance id {}. Setting parent as self..", jobInstance.getId());
                        parentId = String.valueOf(jobInstance.getInstanceId());
                    }
                    JobInstance parentJob = jobExplorer.getJobInstance(Long.parseLong(parentId));
                    JobExecution parentExecutions = getJobExecutions(parentJob).iterator().next();
                    
                    TaskExecutionDto taskExecutionDto = new TaskExecutionDto();
                    taskExecutionDto.setId(parentJob.getId());
                    taskExecutionDto.setRunDateTime(parentExecutions.getStartTime());
                    taskExecutionDto.setParams(Maps.transformValues(
                            parentExecutions.getJobParameters().getParameters(), JobParameter::getValue));
                    taskExecutionDto.setStatus(convertStatus(jobStatus, "SETTLEMENT"));
                    taskExecutionDto.setSettlementStatus(jobStatus);

                    List<JobInstance> calculationJobs = jobExplorer.findJobInstancesByJobName(
                            RUN_COMPUTE_STL_JOB_NAME.concat("*-").concat(parentId), 0, 1);

                    Date calculationEndTime = null;
                    if (!calculationJobs.isEmpty()) {
                        JobExecution calculationJobExecution = getJobExecutions(calculationJobs.get(0)).iterator().next();
                        BatchStatus calculationStatus = calculationJobExecution.getStatus();
                        calculationEndTime = calculationJobExecution.getEndTime();


                        taskExecutionDto.setCalculationStatus(calculationStatus);

                        if (calculationStatus.isUnsuccessful()) {
                            taskExecutionDto.setExitMessage(processFailedMessage(calculationJobExecution));
                        }
                        if (!calculationStatus.isRunning() && calculationEndTime != null) {
                            taskExecutionDto.setStatusDetails("Ended as of ".concat(dateTimeFormat.format(calculationEndTime)));
                        }
                        taskExecutionDto.setStatus(convertStatus(calculationStatus, "CALCULATION"));
                    }

                    List<JobInstance> generateInvoiceJobs = jobExplorer.findJobInstancesByJobName(
                            RUN_GENERATE_INVOICE_STL_JOB_NAME.concat("*-").concat(parentId), 0, 1);

                    if (!generateInvoiceJobs.isEmpty()) {
                        JobExecution invoiceGenJobExecution = getJobExecutions(generateInvoiceJobs.get(0)).iterator().next();
                        BatchStatus invoiceGenStatus = invoiceGenJobExecution.getStatus();
                        Date invoiceGenEndDate = invoiceGenJobExecution.getEndTime();

                        if (!invoiceGenEndDate.before(calculationEndTime)) {
                            taskExecutionDto.setTaggingStatus(invoiceGenStatus);

                            if (invoiceGenStatus.isUnsuccessful()) {
                                taskExecutionDto.setExitMessage(processFailedMessage(invoiceGenJobExecution));
                            }
                            if (!invoiceGenStatus.isRunning() && invoiceGenEndDate != null) {
                                taskExecutionDto.setStatusDetails("Ended as of ".concat(dateTimeFormat.format(invoiceGenEndDate)));
                            }
                            taskExecutionDto.setStatus(convertStatus(invoiceGenStatus, "TAGGING"));
                        }
                    }

                    return taskExecutionDto;

                }).collect(toList());

        return new PageImpl<>(taskExecutionDtos, pageable, taskExecutionDtos.size());
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

        String jobName = null;
        List<String> properties = Lists.newArrayList();
        List<String> arguments = Lists.newArrayList();

        if (RUN_COMPUTE_STL_JOB_NAME.equals(taskRunDto.getJobName())) {
            String type = taskRunDto.getMeterProcessType();
            if (type == null) {
                type = PROCESS_TYPE_DAILY;
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("dailyCalculation")));
                arguments.add(concatKeyValue(START_DATE, taskRunDto.getTradingDate(), "date"));
            } else {
                if (MeterProcessType.ADJUSTED.name().equals(type)) {
                    properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyAdjustedCalculation")));
                } else if (MeterProcessType.PRELIMINARY.name().equals(type) || "PRELIM".equals(type)) {
                    properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyPrelimCalculation")));
                } else if (MeterProcessType.FINAL.name().equals(type)) {
                    properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyFinalCalculation")));
                }
                arguments.add(concatKeyValue(START_DATE, taskRunDto.getStartDate(), "date"));
                arguments.add(concatKeyValue(END_DATE, taskRunDto.getEndDate(), "date"));
            }
            arguments.add(concatKeyValue(RUN_ID, String.valueOf(System.currentTimeMillis())));
            arguments.add(concatKeyValue(PARENT_JOB, taskRunDto.getParentJob(), "long"));
            arguments.add(concatKeyValue(PROCESS_TYPE, type));
            jobName = "crss-settlement-task-calculation";
        } else if (RUN_GENERATE_INVOICE_STL_JOB_NAME.equals(taskRunDto.getJobName())) {
            String type = taskRunDto.getMeterProcessType();
            if (MeterProcessType.ADJUSTED.name().equals(type)) {
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyAdjustedInvoiceGeneration")));
            } else if (MeterProcessType.PRELIMINARY.name().equals(type) || "PRELIM".equals(type)) {
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyPrelimInvoiceGeneration")));
            } else if (MeterProcessType.FINAL.name().equals(type)) {
//                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyFinalInvoiceGeneration")));
            }
            arguments.add(concatKeyValue(RUN_ID, String.valueOf(System.currentTimeMillis())));
            arguments.add(concatKeyValue(PARENT_JOB, taskRunDto.getParentJob(), "long"));
            arguments.add(concatKeyValue(PROCESS_TYPE, type));
            arguments.add(concatKeyValue(START_DATE, taskRunDto.getStartDate(), "date"));
            arguments.add(concatKeyValue(END_DATE, taskRunDto.getEndDate(), "date"));
            jobName = "crss-settlement-task-invoice-generation";
        } else if (RUN_FINALIZE_STL_JOB_NAME.equals(taskRunDto.getJobName())) {
            String type = taskRunDto.getMeterProcessType();
            if (MeterProcessType.FINAL.name().equals(type)) {
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyFinalInvoiceGeneration")));
            }
            arguments.add(concatKeyValue(RUN_ID, String.valueOf(System.currentTimeMillis())));
            arguments.add(concatKeyValue(PARENT_JOB, taskRunDto.getParentJob(), "long"));
            arguments.add(concatKeyValue(PROCESS_TYPE, type));
            arguments.add(concatKeyValue(START_DATE, taskRunDto.getStartDate(), "date"));
            arguments.add(concatKeyValue(END_DATE, taskRunDto.getEndDate(), "date"));
            arguments.add(concatKeyValue(AMS_INVOICE_DATE, taskRunDto.getAmsInvoiceDate(), "date"));
            arguments.add(concatKeyValue(AMS_DUE_DATE, taskRunDto.getAmsDueDate(), "date"));
            arguments.add(concatKeyValue(AMS_REMARKS, taskRunDto.getAmsRemarks(), "string"));
            jobName = "crss-settlement-task-invoice-generation";
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
    public int getDispatchInterval() {
        return 0;
    }

    @Override
    public Page<DataInterfaceExecutionDTO> findDataInterfaceInstances(Pageable pageable) {
        return null;
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
        /*if (runningStep.getExecutionContext().containsKey(key)) {
            progressDto.setTotalCount(runningStep.getExecutionContext().getLong(key));
            progressDto.setExecutedCount(Math.min(stepProgressRepository.findByStepId(runningStep.getId()).map(StepProgress::getChunkCount).orElse(0L),
                    progressDto.getTotalCount()));
        }*/
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
