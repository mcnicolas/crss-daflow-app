package com.pemc.crss.dataflow.app.service.impl;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.pemc.crss.dataflow.app.dto.DataInterfaceExecutionDTO;
import com.pemc.crss.dataflow.app.dto.StlTaskExecutionDto;
import com.pemc.crss.dataflow.app.dto.TaskExecutionDto;
import com.pemc.crss.dataflow.app.dto.TaskProgressDto;
import com.pemc.crss.dataflow.app.dto.TaskRunDto;
import com.pemc.crss.dataflow.app.dto.TaskSummaryDto;
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
import org.apache.commons.lang3.StringUtils;
import org.joda.time.Days;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.repository.dao.ExecutionContextDao;
import org.springframework.batch.core.repository.dao.JobExecutionDao;
import org.springframework.batch.core.repository.dao.JobInstanceDao;
import org.springframework.batch.core.repository.dao.StepExecutionDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.hateoas.ResourceSupport;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestTemplate;

import javax.sql.DataSource;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
    @Qualifier("crssNmmsDataSource")
    private DataSource crssNmmsDataSource;

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

                        return taskExecutionDto;

                    }).collect(toList());
        }
        return new PageImpl<>(taskExecutionDtos, pageable, count);
    }

    @Override
    public Page<StlTaskExecutionDto> findSettlementJobInstances(Pageable pageable) {
        int count = 0;

        try {
            count = jobExplorer.getJobInstanceCount(RUN_COMPUTE_STL_JOB_NAME.concat("Daily"));
            count += jobExplorer.getJobInstanceCount(RUN_COMPUTE_STL_JOB_NAME.concat("MonthlyAdjusted"));
            count += jobExplorer.getJobInstanceCount(RUN_COMPUTE_STL_JOB_NAME.concat("MonthlyPrelim"));
            count += jobExplorer.getJobInstanceCount(RUN_COMPUTE_STL_JOB_NAME.concat("MonthlyFinal"));
        } catch (NoSuchJobException e) {
            LOG.error("Exception: " + e);
        }

        List<StlTaskExecutionDto> stlTaskExecutionDtos = Lists.newArrayList();
        if (count > 0) {
            stlTaskExecutionDtos.addAll(stlTaskExecutionDtoList("Daily", null));
            stlTaskExecutionDtos.addAll(stlTaskExecutionDtoList("MonthlyAdjusted", MeterProcessType.ADJUSTED));
            stlTaskExecutionDtos.addAll(stlTaskExecutionDtoList("MonthlyPrelim", MeterProcessType.PRELIMINARY));
            stlTaskExecutionDtos.addAll(stlTaskExecutionDtoList("MonthlyFinal", MeterProcessType.FINAL));
        }

        return new PageImpl<>(stlTaskExecutionDtos, pageable, count);
    }

    private List<StlTaskExecutionDto> stlTaskExecutionDtoList(String processType, MeterProcessType meterProcessType) {
        List<StlTaskExecutionDto> data = jobExplorer.findJobInstancesByJobName(RUN_COMPUTE_STL_JOB_NAME.concat(processType), 0, 1)
                .stream().map((JobInstance jobInstance) -> {

                    JobExecution jobExecution = getJobExecutions(jobInstance).iterator().next();
                    BatchStatus status = jobExecution.getStatus();

                    StlTaskExecutionDto stlTaskExecutionDto = new StlTaskExecutionDto();
                    stlTaskExecutionDto.setId(jobInstance.getId());
                    stlTaskExecutionDto.setRunDateTime(jobExecution.getStartTime());
                    stlTaskExecutionDto.setParams(Maps.transformValues(
                            jobExecution.getJobParameters().getParameters(), JobParameter::getValue));
                    stlTaskExecutionDto.setCalculationStatus(status);

                    if (status.isRunning()) {
//                            calculateProgress(jobExecution, taskExecutionDto);
                    } else if (status.isUnsuccessful()) {
                        stlTaskExecutionDto.setExitMessage(jobExecution.getExitStatus().getExitDescription());
                    }

                    stlTaskExecutionDto.setStatus(status.name());

                    List<JobInstance> settlementJobs = jobExplorer.findJobInstancesByJobName(
                            RUN_GENERATE_INVOICE_STL_JOB_NAME.concat("*-").concat(jobInstance.getId().toString()), 0, 1);

                    if (!settlementJobs.isEmpty()) {
                        JobExecution settlementJobExecution = getJobExecutions(settlementJobs.get(0)).iterator().next();
                        stlTaskExecutionDto.setInvoiceGenerationStatus(settlementJobExecution.getStatus());

                        if (stlTaskExecutionDto.getInvoiceGenerationStatus().isUnsuccessful()) {
                            stlTaskExecutionDto.setExitMessage(processFailedMessage(settlementJobExecution));
                        } else if (stlTaskExecutionDto.getInvoiceGenerationStatus() == BatchStatus.COMPLETED) {
                            stlTaskExecutionDto.getSummary().put(RUN_GENERATE_INVOICE_STL_JOB_NAME, showSummary(settlementJobExecution));
                        }
                        stlTaskExecutionDto.setStatus(convertStatus(stlTaskExecutionDto.getInvoiceGenerationStatus(), "SETTLEMENT"));
                    }

                    return stlTaskExecutionDto;

                }).collect(toList());


        StlTaskExecutionDto dto = new StlTaskExecutionDto();
        dto.setId(9999L);
        dto.setCalculationStatus(BatchStatus.FAILED);
        dto.setInvoiceGenerationStatus(null);
        dto.setExitMessage("");
        dto.setParams(ImmutableMap.of(START_DATE, "2017-06-26",
                END_DATE, "2017-07-25",
                PROCESS_TYPE, meterProcessType == null ? "DAILY" : meterProcessType.name()));
        dto.setProgress(null);
        dto.setStatus("SETTLEMENT READY");
        dto.setRunDateTime(new Date());

        return data.isEmpty()
                ? Lists.newArrayList(dto)
                : data;
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
        Preconditions.checkState(batchJobRunLockRepository.countByJobNameAndLockedIsTrue(taskRunDto.getJobName()) < jobMaxRun,
                "Job already exceeds the maximum allowable concurrent run");

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
            arguments.add(concatKeyValue(RUN_ID, String.valueOf(System.currentTimeMillis())));

            properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(MarketInfoType
                    .getByJobName(taskRunDto.getJobName()).getProfileName())));

            jobName = "crss-datainterface-task-ingest";

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
                arguments.add(concatKeyValue(END_DATE, taskRunDto.getEndDate(), "date"));
            }
            arguments.add(concatKeyValue(PROCESS_TYPE, type));
            arguments.add(concatKeyValue(START_DATE, taskRunDto.getStartDate(), "date"));
            arguments.add(concatKeyValue(RUN_ID, String.valueOf(System.currentTimeMillis())));
            jobName = "crss-settlement-task-calculation";
        } else if (RUN_GENERATE_INVOICE_STL_JOB_NAME.equals(taskRunDto.getJobName())) {
            String type = taskRunDto.getMeterProcessType();
            if (MeterProcessType.ADJUSTED.name().equals(type)) {
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyAdjustedInvoiceGeneration")));
            } else if (MeterProcessType.PRELIMINARY.name().equals(type)) {
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyPrelimInvoiceGeneration")));
            } else if (MeterProcessType.FINAL.name().equals(type)) {
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyFinalInvoiceGeneration")));
            }
            arguments.add(concatKeyValue(PARENT_JOB, taskRunDto.getParentJob(), "long"));
            arguments.add(concatKeyValue(PROCESS_TYPE, type));
            arguments.add(concatKeyValue(START_DATE, taskRunDto.getStartDate(), "date"));
            arguments.add(concatKeyValue(END_DATE, taskRunDto.getEndDate(), "date"));
            arguments.add(concatKeyValue(RUN_ID, String.valueOf(System.currentTimeMillis())));
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
                arguments.add(concatKeyValue(RUN_ID, String.valueOf(System.currentTimeMillis())));
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

        List<DataInterfaceExecutionDTO> dataInterfaceExecutionDTOs = Lists.newArrayList();

        if (count > 0) {

            List<JobInstance> jobInstances = jobExplorer.findJobInstancesByJobName(jobName,
                    pageable.getOffset(), pageable.getPageSize());

            for (JobInstance jobInstance : jobInstances) {

                JobExecution jobExecution = getJobExecutions(jobInstance).iterator().next();
                Map jobParameters = Maps.transformValues(jobExecution.getJobParameters().getParameters(), JobParameter::getValue);

                LocalDateTime startDateTime = new LocalDateTime(jobParameters.get("startDatetime"));
                LocalDateTime endDateTime = new LocalDateTime(jobParameters.get("endDatetime"));

                Days daysInBetween = Days.daysBetween(startDateTime, endDateTime);
                int noOfDaysInBetween = daysInBetween.getDays();
                List<LocalDateTime> daysRange;
                if (noOfDaysInBetween > 0) {
                    daysRange = Stream.iterate(startDateTime, date -> date.plusDays(1))
                            .limit(daysInBetween.getDays() + 2).collect(toList());
                } else {
                    daysRange = new ArrayList<>();
                    daysRange.add(startDateTime);
                }

                DateTimeFormatter dtf = DateTimeFormat.forPattern("MM/dd/yyyy HH:mm");
                String startDateChecker = dtf.print(startDateTime).split(" ")[0];
                String endDateChecker = dtf.print(endDateTime).split(" ")[0];

                List<DataInterfaceExecutionDTO> tempList = daysRange.stream().map(days -> {
                    DataInterfaceExecutionDTO temp = new DataInterfaceExecutionDTO();

                    String date = dtf.print(days);
                    String tradingDay = date.split(" ")[0];
                    String dispatchInterval = "";

                    temp.setId(jobInstance.getId());
                    temp.setRunStartDateTime(jobExecution.getStartTime());
                    temp.setRunEndDateTime(jobExecution.getEndTime());
                    temp.setTradingDay(tradingDay);
                    temp.setStatus(jobExecution.getStatus().toString());
                    temp.setParams(jobParameters);
                    temp.setBatchStatus(jobExecution.getStatus());
                    temp.setType(type);

                    if (tradingDay.equals(startDateChecker) && tradingDay.equals(endDateChecker)) {
                        dispatchInterval = dtf.print(startDateTime).split(" ")[1] + "-" + dtf.print(endDateTime).split(" ")[1];
                    } else if (tradingDay.equals(startDateChecker)) {
                        dispatchInterval = dtf.print(startDateTime).split(" ")[1] + "-24:00";
                    } else if (tradingDay.equals(endDateChecker)) {
                        dispatchInterval = "00:05-" + dtf.print(endDateTime).split(" ")[1];
                    } else {
                        dispatchInterval = "00:05-24:00";
                    }

                    temp.setDispatchInterval(dispatchInterval);

                    setLogs(jobName, temp, jobExecution);

                    temp.setMode(StringUtils.upperCase((String) jobParameters.getOrDefault(MODE, "AUTOMATIC")));

                    return temp;
                }).collect(Collectors.toList());

                dataInterfaceExecutionDTOs.addAll(tempList);
            }
        }

        dataInterfaceExecutions.addAll(dataInterfaceExecutionDTOs);

        return count;
    }

    private void setLogs(String jobName, DataInterfaceExecutionDTO executionDTO, JobExecution jobExecution) {
        //todo to get treshhold in config_db change to spring data repo

        JdbcTemplate crssNmmsJdbcTemplate = new JdbcTemplate(crssNmmsDataSource);

        int abTreshhold = 10000;

        int abnormalPrice = 0;
        boolean checkAbnormalPrice = false;
        StepExecution stepExecution = null;
        String tableName = "";
        String query = "";

        Collection<StepExecution> executionSteps = jobExecution.getStepExecutions();
        Iterator it = executionSteps.iterator();

        while (it.hasNext()) {
            StepExecution stepChecker = (StepExecution) it.next();
            if (stepChecker.getStepName().equals("step1")) {
                stepExecution = stepChecker;
                break;
            }
        }

        if (jobName.equalsIgnoreCase("importEnergyPriceSchedJob")) {
            checkAbnormalPrice = true;
            tableName = "txn_energy_price_sched";

            query = "select count(*) from " + tableName + " where job_id = " + jobExecution.getJobId() +
                    " and fedp > " + abTreshhold + "or fedp_mlc > " + abTreshhold + " or fedp_mcc > " + abTreshhold +
                    " or fedp_smp > " + abTreshhold;
        } else if (jobName.equalsIgnoreCase("importReservePriceSchedJob")) {
            checkAbnormalPrice = true;
            tableName = "txn_reserve_price_sched";

            query = "select count(*) from " + tableName + " where job_id = " + jobExecution.getJobId() +
                    " and price > " + abTreshhold;
        }

        LOG.info("QUERY: " + query);

        if (checkAbnormalPrice) {
            abnormalPrice = crssNmmsJdbcTemplate.queryForObject(query, Integer.class);
        }

        if (stepExecution != null) {
            executionDTO.setRecordsWritten(stepExecution.getWriteCount());
            executionDTO.setRecordsRead(stepExecution.getReadCount());
            executionDTO.setAbnormalPrice(abnormalPrice);
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
