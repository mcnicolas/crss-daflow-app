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
import com.pemc.crss.shared.commons.reference.MarketInfoType;
import com.pemc.crss.shared.commons.reference.MeterProcessType;
import com.pemc.crss.shared.commons.util.DateUtil;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobRunLock;
import com.pemc.crss.shared.core.dataflow.repository.BatchJobRunLockRepository;
import com.pemc.crss.shared.core.dataflow.repository.StepProgressRepository;
import com.pemc.crss.shared.core.nmms.repository.EnergyPriceSchedRepository;
import com.pemc.crss.shared.core.nmms.repository.ReservePriceSchedRepository;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.Days;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
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
import org.springframework.hateoas.ResourceSupport;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;

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
    private static final String RUN_TAG_AS_FINAL_STL_JOB_NAME = "tagAsFinal";
    private static final String RUN_GENERATE_INVOICE_STL_JOB_NAME = "generateInvoiceSettlement";
    private static final String RUN_STL_READY_JOB_NAME = "processStlReady";
    private static final String DATE = "date";
    private static final String START_DATE = "startDate";
    private static final String END_DATE = "endDate";
    private static final String PROCESS_TYPE = "processType";
    private static final String PARENT_JOB = "parentJob";
    private static final String PROCESS_TYPE_DAILY = "DAILY";
    private static final String METER_TYPE = "meterType";
    private static final String QUOTE = "\"";
    private static final String MODE = "mode";
    private static final String RUN_ID = "run.id";
    private static final String SPRING_PROFILES_ACTIVE = "spring.profiles.active";

    private DateFormat dateFormat = new SimpleDateFormat(DateUtil.DEFAULT_DATE_FORMAT);

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

                        JobExecution jobExecution = getJobExecutions(jobInstance).iterator().next();

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
                            JobExecution rcoaJobExecution = getJobExecutions(rcoaJobs.get(0)).iterator().next();
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
                            JobExecution settlementJobExecution = getJobExecutions(settlementJobs.get(0)).iterator().next();
                            taskExecutionDto.setSettlementStatus(settlementJobExecution.getStatus());

                            if (taskExecutionDto.getSettlementStatus().isUnsuccessful()) {
                                taskExecutionDto.setExitMessage(processFailedMessage(settlementJobExecution));
                            } else if (taskExecutionDto.getSettlementStatus() == BatchStatus.COMPLETED) {
                                taskExecutionDto.getSummary().put(RUN_STL_READY_JOB_NAME, showSummary(settlementJobExecution));
                            }
                            taskExecutionDto.setStatus(convertStatus(taskExecutionDto.getSettlementStatus(), "SETTLEMENT"));
                        }

                        List<JobInstance> calculationJobs = jobExplorer.findJobInstancesByJobName(
                                RUN_COMPUTE_STL_JOB_NAME.concat("*-").concat(jobInstance.getId().toString()), 0, 1);

                        if (!calculationJobs.isEmpty()) {
                            JobExecution calculationJobExecution = jobExplorer.getJobExecutions(calculationJobs.get(0)).iterator().next();
                            taskExecutionDto.setCalculationStatus(calculationJobExecution.getStatus());

                            if (taskExecutionDto.getCalculationStatus().isUnsuccessful()) {
                                taskExecutionDto.setExitMessage(processFailedMessage(calculationJobExecution));
                            }
                            taskExecutionDto.setStatus(convertStatus(taskExecutionDto.getCalculationStatus(), "CALCULATION"));
                        }

                        List<JobInstance> generateInvoiceJobs = jobExplorer.findJobInstancesByJobName(
                                RUN_GENERATE_INVOICE_STL_JOB_NAME.concat("*-").concat(jobInstance.getId().toString()), 0, 1);

                        if (!generateInvoiceJobs.isEmpty()) {
                            JobExecution invoiceGenerationJobExecution = jobExplorer.getJobExecutions(generateInvoiceJobs.get(0)).iterator().next();
                            taskExecutionDto.setTaggingStatus(invoiceGenerationJobExecution.getStatus());

                            if (taskExecutionDto.getTaggingStatus().isUnsuccessful()) {
                                taskExecutionDto.setExitMessage(processFailedMessage(invoiceGenerationJobExecution));
                            }
                            taskExecutionDto.setStatus(convertStatus(taskExecutionDto.getTaggingStatus(), "TAGGING"));
                        }

                        return taskExecutionDto;

                    }).collect(toList());
        }
        return new PageImpl<>(taskExecutionDtos, pageable, count);
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

        List<MarketInfoType> MARKET_INFO_TYPES = Arrays.asList(MarketInfoType.values());

        if (MARKET_INFO_TYPES.contains(MarketInfoType.getByJobName(taskRunDto.getJobName()))) {
            arguments.add(concatKeyValue(START_DATE, StringUtils.containsWhitespace(taskRunDto.getStartDate())
                    ? QUOTE + taskRunDto.getStartDate() + QUOTE : taskRunDto.getStartDate(), "date"));
            arguments.add(concatKeyValue(END_DATE, StringUtils.containsWhitespace(taskRunDto.getEndDate())
                    ? QUOTE + taskRunDto.getEndDate() + QUOTE : taskRunDto.getEndDate(), "date"));
            arguments.add(concatKeyValue(PROCESS_TYPE, taskRunDto.getMarketInformationType()));
            arguments.add(concatKeyValue(MODE, "Manual"));
            arguments.add(concatKeyValue(RUN_ID, String.valueOf(System.currentTimeMillis()), "long"));

            properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(MarketInfoType
                    .getByJobName(taskRunDto.getJobName()).getProfileName())));

            jobName = "crss-datainterface-task-ingest";

        } else if (RUN_COMPUTE_STL_JOB_NAME.equals(taskRunDto.getJobName())) {
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
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyFinalInvoiceGeneration")));
            }
            arguments.add(concatKeyValue(RUN_ID, String.valueOf(System.currentTimeMillis())));
            arguments.add(concatKeyValue(PARENT_JOB, taskRunDto.getParentJob(), "long"));
            arguments.add(concatKeyValue(PROCESS_TYPE, type));
            arguments.add(concatKeyValue(START_DATE, taskRunDto.getStartDate(), "date"));
            arguments.add(concatKeyValue(END_DATE, taskRunDto.getEndDate(), "date"));
            jobName = "crss-settlement-task-invoice-generation";
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
                arguments.add(concatKeyValue(RUN_ID, String.valueOf(System.currentTimeMillis()), "long"));
                arguments.add(concatKeyValue(METER_TYPE, MeterType.MIRF_MT_WESM.name()));
                jobName = "crss-meterprocess-task-mqcomputation";
            } else if (taskRunDto.getParentJob() != null) {
                JobInstance jobInstance = jobExplorer.getJobInstance(Long.valueOf(taskRunDto.getParentJob()));
                JobParameters jobParameters = getJobExecutions(jobInstance).get(0).getJobParameters();
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
    public Page<DataInterfaceExecutionDTO> findDataInterfaceInstances(Pageable pageable) {
        List<DataInterfaceExecutionDTO> dataInterfaceExecutionDTOs = new ArrayList<>();

        int count = 0;
        for (MarketInfoType marketInfoType : MarketInfoType.values()) {
            count += processDataInterfaceJobByName(dataInterfaceExecutionDTOs, marketInfoType, pageable);
        }

        return new PageImpl<>(dataInterfaceExecutionDTOs, pageable, count);
    }

    private int processDataInterfaceJobByName(List<DataInterfaceExecutionDTO> dataInterfaceExecutions,
                                              MarketInfoType marketInfoType, Pageable pageable) {
        int count = 0;
        String jobName = marketInfoType.getJobName();
        String type = marketInfoType.getLabel();

        try {
            count = jobExplorer.getJobInstanceCount(jobName);
        } catch (NoSuchJobException e) {
            LOG.error("Exception: " + e);
        }

        if (count > 0) {

            List<DataInterfaceExecutionDTO> dataInterfaceExecutionDTOs = jobExplorer.findJobInstancesByJobName(jobName,
                    pageable.getOffset(), pageable.getPageSize())
                    .stream().map((JobInstance jobInstance) -> {

                        DataInterfaceExecutionDTO dataInterfaceExecutionDTO = new DataInterfaceExecutionDTO();
                        JobExecution jobExecution = getJobExecutions(jobInstance).iterator().next();
                        Map jobParameters = Maps.transformValues(jobExecution.getJobParameters().getParameters(), JobParameter::getValue);

                        LocalDateTime startDateTime = new LocalDateTime(jobParameters.get("startDate"));
                        LocalDateTime endDateTime = new LocalDateTime(jobParameters.get("endDate"));

                        Days daysInBetween = Days.daysBetween(startDateTime, endDateTime);
                        int noOfDaysInBetween = daysInBetween.getDays();

                        DateTimeFormatter dtf = DateTimeFormat.forPattern("MM/dd/yyyy HH:mm");
                        String tradingDay = noOfDaysInBetween > 0 ? dtf.print(startDateTime).split(" ")[0]
                                : dtf.print(startDateTime).split(" ")[0] + " - " + dtf.print(endDateTime).split(" ")[0];

                        String dispatchInterval = dtf.print(startDateTime).split(" ")[1] + "-" + dtf.print(endDateTime).split(" ")[1];

                        dataInterfaceExecutionDTO.setId(jobInstance.getId());
                        dataInterfaceExecutionDTO.setRunStartDateTime(jobExecution.getStartTime());
                        dataInterfaceExecutionDTO.setRunEndDateTime(jobExecution.getEndTime());
                        dataInterfaceExecutionDTO.setTradingDay(tradingDay);
                        dataInterfaceExecutionDTO.setStatus(jobExecution.getStatus().toString());
                        dataInterfaceExecutionDTO.setParams(jobParameters);
                        dataInterfaceExecutionDTO.setBatchStatus(jobExecution.getStatus());
                        dataInterfaceExecutionDTO.setType(type);
                        dataInterfaceExecutionDTO.setDispatchInterval(dispatchInterval);
                        dataInterfaceExecutionDTO.setMode(StringUtils.upperCase((String) jobParameters.getOrDefault(MODE, "AUTOMATIC")));
                        setLogs(jobName, dataInterfaceExecutionDTO, jobExecution);

                        return dataInterfaceExecutionDTO;
                    }).collect(toList());
            dataInterfaceExecutions.addAll(dataInterfaceExecutionDTOs);
        }
        return count;
    }

    private void setLogs(String jobName, DataInterfaceExecutionDTO executionDTO, JobExecution jobExecution) {
        //todo to get treshhold in config_db
        BigDecimal abTreshhold = new BigDecimal(10000) ;
        StepExecution stepExecution = null;
        int abnormalPrice = 0;

        if (jobName.equalsIgnoreCase("importEnergyPriceSchedJob")) {
            abnormalPrice = energyPriceSchedRepository.getAbnormalPriceCount(abTreshhold, jobExecution.getJobId());
        } else if (jobName.equalsIgnoreCase("importReservePriceSchedJob")) {
            abnormalPrice = reservePriceSchedRepository.getAbnormalPriceCount(abTreshhold, jobExecution.getJobId());
        }

        executionDTO.setAbnormalPrice(abnormalPrice);

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
            executionDTO.setRecordsWritten(stepExecution.getWriteCount());
            executionDTO.setRecordsRead(stepExecution.getReadCount());
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
