package com.pemc.crss.dataflow.app.service.impl;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.pemc.crss.dataflow.app.dto.*;
import com.pemc.crss.dataflow.app.dto.parent.GroupTaskExecutionDto;
import com.pemc.crss.dataflow.app.dto.parent.StubTaskExecutionDto;
import com.pemc.crss.dataflow.app.support.PageableRequest;
import com.pemc.crss.shared.commons.reference.MarketInfoType;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.net.URISyntaxException;
import java.util.*;

import static java.util.stream.Collectors.toList;

@Service("dataInterfaceTaskExecutionService")
@Transactional(readOnly = true, value = "transactionManager")
public class DataInterfaceTaskExecutionServiceImpl extends AbstractTaskExecutionService {

    private static final Logger LOG = LoggerFactory.getLogger(DataInterfaceTaskExecutionServiceImpl.class);

    private static final String MODE = "mode";
    private static final String RETRY_ATTEMPT = "retryAttempt";
    private static final String MANUAL_MODE = "manual";
    private static final String AUTOMATIC_MODE = "automatic";

    DateTimeFormatter df = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

    @Override
    @Transactional(value = "transactionManager")
    public void launchJob(TaskRunDto taskRunDto) throws URISyntaxException {
        LOG.debug("Launching Data Interface Job....");
        Preconditions.checkNotNull(taskRunDto.getJobName());

        String jobName = null;
        List<String> properties = Lists.newArrayList();
        List<String> arguments = Lists.newArrayList();

        List<MarketInfoType> MARKET_INFO_TYPES = Arrays.asList(MarketInfoType.values());

        MarketInfoType marketInfoType = MarketInfoType.getByJobName(taskRunDto.getJobName());
        if (MARKET_INFO_TYPES.contains(marketInfoType)) {

            if (!marketInfoType.equals(MarketInfoType.MTN_DATA)) {
                if (!StringUtils.isEmpty(taskRunDto.getStartDate())) {
                    LOG.debug("Starting Manual Import........");
                    arguments.add(concatKeyValue(START_DATE, StringUtils.containsWhitespace(taskRunDto.getStartDate())
                            ? QUOTE + taskRunDto.getStartDate() + QUOTE : taskRunDto.getStartDate(), "date"));
                    arguments.add(concatKeyValue(END_DATE, StringUtils.containsWhitespace(taskRunDto.getEndDate())
                            ? QUOTE + taskRunDto.getEndDate() + QUOTE : taskRunDto.getEndDate(), "date"));
                    arguments.add(concatKeyValue(USERNAME, taskRunDto.getCurrentUser()));
                    arguments.add(concatKeyValue(MODE, MANUAL_MODE));
                } else {
                    LOG.debug("Starting Automatic Import........");

                    Preconditions.checkState(batchJobRunLockRepository.countByJobNameAndLockedDateAndLockedIsTrue(taskRunDto.getJobName(), new Date()) == 0,
                            "There is an existing ".concat(taskRunDto.getJobName()).concat(" job running"));

                    DateTimeFormatter df = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
                    String startDate =  df.print(LocalDateTime.now().minusDays(1).withHourOfDay(0).withMinuteOfHour(5).withSecondOfMinute(0));;
                    String endDate = df.print(LocalDateTime.now().withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0));

                    arguments.add(concatKeyValue(START_DATE, StringUtils.containsWhitespace(startDate)
                            ? QUOTE + startDate + QUOTE : startDate, "date"));
                    arguments.add(concatKeyValue(END_DATE, StringUtils.containsWhitespace(endDate)
                            ? QUOTE + endDate + QUOTE : endDate, "date"));
                    arguments.add(concatKeyValue(USERNAME, "system"));
                    arguments.add(concatKeyValue(MODE, AUTOMATIC_MODE));
                }
            }
            arguments.add(concatKeyValue(PROCESS_TYPE, taskRunDto.getMarketInformationType()));
            arguments.add(concatKeyValue(RUN_ID, String.valueOf(System.currentTimeMillis()), "long"));
            properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(MarketInfoType
                    .getByJobName(taskRunDto.getJobName()).getProfileName())));

            jobName = "crss-datainterface-task-ingest";
        }

        LOG.debug("Running job name={}, properties={}, arguments={}", taskRunDto.getJobName(), properties, arguments);

        if (jobName != null) {
            LOG.debug("Running job name={}, properties={}, arguments={}", taskRunDto.getJobName(), properties, arguments);
            launchJob(jobName, properties, arguments);
            lockJob(taskRunDto);
        }
    }

    @Override
    public Page<? extends BaseTaskExecutionDto> findJobInstances(Pageable pageable) {
        return null;
    }

    @Override
    public Page<GroupTaskExecutionDto> findDistinctBillingPeriodAndProcessType(Pageable pageable) {
        return null;
    }

    @Override
    public Page<? extends StubTaskExecutionDto> findJobInstancesByBillingPeriodAndProcessType(Pageable pageable, String billingPeriod, String processType) {
        return null;
    }

    public Page<DataInterfaceExecutionDTO> findJobInstances(Pageable pageable, String type,
                                                            String status, String filterMode,
                                                            String runStartDate, String tradingStartDate,
                                                            String tradingEndDate, String username) {

        List<DataInterfaceExecutionDTO> dataInterfaceExecutionDTOs = new ArrayList<>();

        int count = 0;

        try {
            count += dataFlowJdbcJobExecutionDao.getJobInstanceCount(type, status, filterMode, runStartDate,
                    tradingStartDate, tradingEndDate, username);
        } catch (NoSuchJobException e) {
            LOG.error("Exception: " + e);
        }

        if (count > 0) {
            dataInterfaceExecutionDTOs = dataFlowJdbcJobExecutionDao.findJobInstancesByName(type,
                    pageable.getOffset(), pageable.getPageSize(), status, filterMode, runStartDate,
                    tradingStartDate, tradingEndDate, username)
                    .stream().map((JobInstance jobInstance) -> {

                        DataInterfaceExecutionDTO dataInterfaceExecutionDTO = new DataInterfaceExecutionDTO();
                        if (getJobExecutions(jobInstance).iterator().hasNext()) {
                            JobExecution jobExecution = getJobExecutions(jobInstance, status, filterMode, runStartDate,
                                    tradingStartDate, tradingEndDate, username).iterator().next();

                            Map jobParameters = Maps.transformValues(jobExecution.getJobParameters().getParameters(), JobParameter::getValue);
                            String jobName = jobExecution.getJobInstance().getJobName();

                            String mode = StringUtils.upperCase((String) jobParameters.getOrDefault(MODE, "automatic"));
                            String user = (String) jobParameters.getOrDefault(USERNAME, "");

                            DateTimeFormatter emdbFormat = DateTimeFormat.forPattern("dd-MMM-yy HH:mm:ss");
                            DateTimeFormatter rbcqFormat = DateTimeFormat.forPattern("yyyyMMdd");
                            DateTimeFormatter displayFormat = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

                            Date automaticTradngDayStart = null;
                            Date automaticTradingDayEnd = null;

                            //todo handle exceptions, temporarily revert to original behavior when startDate is null
                            LocalDateTime runDate = new LocalDateTime(jobExecution.getStartTime());

                            if (mode.equalsIgnoreCase("automatic")) {
                                try {
                                    String automaticStart = jobExecution.getExecutionContext().getString("startDate");
                                    String automaticEnd = jobExecution.getExecutionContext().getString("endDate", "");

                                    if (StringUtils.isEmpty(automaticEnd)) {
                                        automaticTradngDayStart = displayFormat.parseDateTime(displayFormat.print(rbcqFormat
                                                .parseDateTime(automaticStart))).toDate();
                                    } else {
                                        automaticTradngDayStart = displayFormat.parseDateTime(displayFormat.print(emdbFormat
                                                .parseDateTime(automaticStart))).toDate();
                                        automaticTradingDayEnd = displayFormat.parseDateTime(displayFormat.print(emdbFormat
                                                .parseDateTime(automaticEnd))).toDate();
                                    }

                                } catch (Exception e) {
                                    automaticTradngDayStart = runDate.minusDays(1).withHourOfDay(00).withMinuteOfHour(05).toDate();
                                    automaticTradingDayEnd = runDate.withHourOfDay(00).withMinuteOfHour(00).toDate();
                                }
                            }

                            Date tradingDayStart = !mode.equals("automatic") ? (Date) jobParameters.get("startDate")
                                    : automaticTradngDayStart;
                            Date tradingDayEnd = !mode.equals("automatic") ? (Date) jobParameters.get("endDate")
                                    : automaticTradingDayEnd;

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
                            dataInterfaceExecutionDTO.setUser(user);
                            setLogs(dataInterfaceExecutionDTO, jobExecution);

                            if (jobExecution.getStatus().isRunning()) {
                                calculateDataInterfaceProgress(jobExecution, dataInterfaceExecutionDTO);
                            }

                            return dataInterfaceExecutionDTO;
                        } else {
                            return null;
                        }
                    })
                    .filter(Objects::nonNull)
                    .collect(toList());
        }
        Collections.reverse(dataInterfaceExecutionDTOs);
        return new PageImpl<>(dataInterfaceExecutionDTOs, pageable, count);
    }

    @Override
    public Page<? extends BaseTaskExecutionDto> findJobInstances(PageableRequest pageableRequest) {
        return null;
    }

    @Override
    public int getDispatchInterval() {
        //TODO connect to global configuration to get dispatch-interval
        return Integer.valueOf(super.getDispatchInterval());
    }

    public void relaunchFailedJob(long jobId) throws URISyntaxException {
        LOG.debug("Executing relaunch failed job....");
        JobExecution failedJobExecution = jobExplorer.getJobExecution(jobId);
        Map jobParameters = Maps.transformValues(failedJobExecution.getJobParameters().getParameters(), JobParameter::getValue);
        String mode = StringUtils.upperCase((String) jobParameters.getOrDefault(MODE, "automatic"));
        String stringRetryAttempt = (String) jobParameters.getOrDefault(RETRY_ATTEMPT, "0");
        String failedJobName = failedJobExecution.getJobInstance().getJobName();
        int retryAttempt = Integer.valueOf(stringRetryAttempt);
        LOG.debug("jobId={}, jobName={}, mode={}, retryAttempt={}", jobId, failedJobName, mode, retryAttempt);
        if (mode.equalsIgnoreCase("automatic")) {
            if (retryAttempt < maxRetry) {

                String startDate = df.print(LocalDateTime.now().minusDays(1).withHourOfDay(0).withMinuteOfHour(5).withSecondOfMinute(0));
                String endDate = df.print(LocalDateTime.now().withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0));

                List<String> properties = Lists.newArrayList();
                List<String> arguments = Lists.newArrayList();
                String jobName;

                TaskRunDto taskRunDto = new TaskRunDto();
                taskRunDto.setJobName(failedJobName);

                arguments.add(concatKeyValue(RUN_ID, String.valueOf(System.currentTimeMillis()), "long"));
                arguments.add(concatKeyValue(RETRY_ATTEMPT, String.valueOf(retryAttempt + 1)));
                arguments.add(concatKeyValue(START_DATE, StringUtils.containsWhitespace(startDate)
                        ? QUOTE + startDate + QUOTE : startDate, "date"));
                arguments.add(concatKeyValue(END_DATE, StringUtils.containsWhitespace(endDate)
                        ? QUOTE + endDate + QUOTE : endDate, "date"));
                arguments.add(concatKeyValue(PROCESS_TYPE, MarketInfoType.getByJobName(failedJobName).getLabel()));
                arguments.add(concatKeyValue(MODE, AUTOMATIC_MODE));
                arguments.add(concatKeyValue(USERNAME, "system"));
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(MarketInfoType
                        .getByJobName(failedJobName).getProfileName())));

                jobName = "crss-datainterface-task-ingest";

                LOG.debug("Running job name={}, properties={}, arguments={}", taskRunDto.getJobName(), properties, arguments);

                if (jobName != null) {
                    LOG.debug("Running job name={}, properties={}, arguments={}", taskRunDto.getJobName(), properties, arguments);
                    launchJob(jobName, properties, arguments);
                    lockJob(taskRunDto);
                }
            } else {
                LOG.debug("Retry Attempt for failed job already reached limit, limit = {}", retryAttempt);
            }
        }
    }

    private void setLogs(DataInterfaceExecutionDTO executionDTO, JobExecution jobExecution) {
        StepExecution stepExecution = null;

        Collection<StepExecution> executionSteps = jobExecution.getStepExecutions();
        Iterator it = executionSteps.iterator();
        while (it.hasNext()) {
            StepExecution stepChecker = (StepExecution) it.next();
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
                executionDTO.setStacktrace(stepExecution.getExitStatus().getExitDescription());
                executionDTO.setFailureException(jobExecution.getExecutionContext().getString("userFriendlyError", ""));
            } else {
                executionDTO.setRecordsWritten(stepExecution.getWriteCount());
            }
        }
    }

    private void calculateDataInterfaceProgress(JobExecution jobExecution, TaskExecutionDto taskExecutionDto) {
        TaskProgressDto progressDto = null;
        List<MarketInfoType> MARKET_INFO_TYPES = Arrays.asList(MarketInfoType.values());
        if (MARKET_INFO_TYPES.contains(MarketInfoType.getByJobName(jobExecution.getJobInstance().getJobName()))) {
            StepExecution stepExecution = null;
            Collection<StepExecution> executionSteps = jobExecution.getStepExecutions();
            Iterator it = executionSteps.iterator();
            while (it.hasNext()) {
                StepExecution stepChecker = (StepExecution) it.next();
                if (stepChecker.getStepName().equals("step1")) {
                    stepExecution = stepChecker;
                    break;
                }
            }
            if (stepExecution != null) {
                if (stepExecution.getStepName().equals("step1")) {
                    progressDto = processStepProgress(stepExecution, "Importing Data");
                }
            }
            taskExecutionDto.setProgress(progressDto);
        }
        taskExecutionDto.setProgress(progressDto);
    }

}
