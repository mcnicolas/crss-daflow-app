package com.pemc.crss.dataflow.app.service.impl;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.pemc.crss.dataflow.app.dto.DataInterfaceExecutionDTO;
import com.pemc.crss.dataflow.app.dto.TaskExecutionDto;
import com.pemc.crss.dataflow.app.dto.TaskProgressDto;
import com.pemc.crss.dataflow.app.dto.TaskRunDto;
import com.pemc.crss.shared.commons.reference.MarketInfoType;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobRetryAttempt;
import com.pemc.crss.shared.core.dataflow.repository.BatchJobRetryAttemptRepository;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.net.URISyntaxException;
import java.util.*;

import static java.util.stream.Collectors.toList;

@Service("dataInterfaceTaskExecutionService")
@Transactional(readOnly = true, value = "transactionManager")
public class DataInterfaceTaskExecutionServiceImpl extends DataFlowAbstractTaskExecutionService {

    private static final Logger LOG = LoggerFactory.getLogger(DataInterfaceTaskExecutionServiceImpl.class);

    private static final String RUN_TODI_JOB_NAME = "import";
    private static final String MODE = "mode";

    @Autowired
    BatchJobRetryAttemptRepository batchJobRetryAttemptRepository;

    @Autowired
    private JdbcOperations dataflowJdbcTemplate;


    @Override
    @Transactional(value = "transactionManager")
    public void launchJob(TaskRunDto taskRunDto) throws URISyntaxException {
        LOG.debug("Launching Data Interface Job....");
        Preconditions.checkNotNull(taskRunDto.getJobName());
        Preconditions.checkState(batchJobRunLockRepository.countByJobNameAndLockedIsTrue(taskRunDto.getJobName()) == 0,
                "There is an existing ".concat(taskRunDto.getJobName()).concat(" job running"));

        String jobName = null;
        List<String> properties = Lists.newArrayList();
        List<String> arguments = Lists.newArrayList();

        List<MarketInfoType> MARKET_INFO_TYPES = Arrays.asList(MarketInfoType.values());

        if (MARKET_INFO_TYPES.contains(MarketInfoType.getByJobName(taskRunDto.getJobName()))) {

            if (!StringUtils.isEmpty(taskRunDto.getStartDate())) {
                LOG.debug("Starting Manual Import........");
                arguments.add(concatKeyValue(START_DATE, StringUtils.containsWhitespace(taskRunDto.getStartDate())
                        ? QUOTE + taskRunDto.getStartDate() + QUOTE : taskRunDto.getStartDate(), "date"));
                arguments.add(concatKeyValue(END_DATE, StringUtils.containsWhitespace(taskRunDto.getEndDate())
                        ? QUOTE + taskRunDto.getEndDate() + QUOTE : taskRunDto.getEndDate(), "date"));
                arguments.add(concatKeyValue(PROCESS_TYPE, taskRunDto.getMarketInformationType()));
                arguments.add(concatKeyValue(MODE, "Manual"));
            } else {
                LOG.debug("Starting Automatic Import........");
            }

            arguments.add(concatKeyValue(RUN_ID, String.valueOf(System.currentTimeMillis()), "long"));
            properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(MarketInfoType
                    .getByJobName(taskRunDto.getJobName()).getProfileName())));

            jobName = "crss-datainterface-task-ingest";
        }

        LOG.debug("Running job name={}, properties={}, arguments={}", taskRunDto.getJobName(), properties, arguments);

        if (jobName != null) {
            LOG.debug("Running job name={}, properties={}, arguments={}", taskRunDto.getJobName(), properties, arguments);
            doLaunchAndLockJob(taskRunDto, jobName, properties, arguments);
        }
    }

    @Override
    public Page<DataInterfaceExecutionDTO> findJobInstances(Pageable pageable, String type,
                                                            String status, String filterMode,
                                                            String runStartDate, String runEndDate,
                                                            String tradingStartDate, String tradingEndDate) {
        try {
            this.relaunchFailedJob(new Long(1)); //todo remove this
        } catch (Exception e) {
            System.out.println(e);
        }

        List<DataInterfaceExecutionDTO> dataInterfaceExecutionDTOs = new ArrayList<>();

        int count = 0;

        try {
            count += dataFlowJdbcJobExecutionDao.getJobInstanceCount(type, status, filterMode, runStartDate, runEndDate, tradingStartDate, tradingEndDate);
        } catch (NoSuchJobException e) {
            LOG.error("Exception: " + e);
        }

        if (count > 0) {
            dataInterfaceExecutionDTOs
                    = dataFlowJdbcJobExecutionDao.findJobInstancesByName(type,
                    pageable.getOffset(), pageable.getPageSize(), status, filterMode, runStartDate, runEndDate,tradingStartDate, tradingEndDate)
                    .stream().map((JobInstance jobInstance) -> {

                        DataInterfaceExecutionDTO dataInterfaceExecutionDTO = new DataInterfaceExecutionDTO();
                        JobExecution jobExecution = getJobExecutions(jobInstance, status, filterMode, runStartDate, runEndDate, tradingStartDate, tradingEndDate).iterator().next();

                        Map jobParameters = Maps.transformValues(jobExecution.getJobParameters().getParameters(), JobParameter::getValue);
                        String jobName = jobExecution.getJobInstance().getJobName();
                        String mode = StringUtils.upperCase((String) jobParameters.getOrDefault(MODE, "automatic"));

                        DateTimeFormatter emdbFormat = DateTimeFormat.forPattern("dd-MMM-yy HH:mm:ss");
                        DateTimeFormatter rbcqFormat = DateTimeFormat.forPattern("yyyyMMdd");
                        DateTimeFormatter displayFormat = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

                        Date automaticTradngDayStart = null;
                        Date automaticTradingDayEnd = null;

                        //todo handle exceptions, temporarily revert to original behavior when startDate is null
                        LocalDateTime runDate = new LocalDateTime(jobExecution.getStartTime());

                        if (mode.equals("automatic")) {
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
                        setLogs(dataInterfaceExecutionDTO, jobExecution);

                        if (jobExecution.getStatus().isRunning()) {
                            calculateDataInterfaceProgress(jobExecution, dataInterfaceExecutionDTO);
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

    public void relaunchFailedJob(Long jobId) throws URISyntaxException {
        JobExecution failedJobExecution = jobExplorer.getJobExecution(new Long(223));
        Map jobParameters = Maps.transformValues(failedJobExecution.getJobParameters().getParameters(), JobParameter::getValue);
        String mode = StringUtils.upperCase((String) jobParameters.getOrDefault(MODE, "automatic"));

        if(mode.equalsIgnoreCase("automatic")) {
            if(this.checkIfRetryLimitReached(jobId)) {
                List<String> properties = Lists.newArrayList();
                List<String> arguments = Lists.newArrayList();
                String jobName;

                TaskRunDto taskRunDto = new TaskRunDto();
                taskRunDto.setJobName(failedJobExecution.getJobInstance().getJobName());

                arguments.add(concatKeyValue(RUN_ID, String.valueOf(System.currentTimeMillis()), "long"));
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(MarketInfoType
                        .getByJobName(taskRunDto.getJobName()).getProfileName())));

                jobName = "crss-datainterface-task-ingest";

                LOG.debug("Running job name={}, properties={}, arguments={}", taskRunDto.getJobName(), properties, arguments);

                if (jobName != null) {
                    LOG.debug("Running job name={}, properties={}, arguments={}", taskRunDto.getJobName(), properties, arguments);
                    doLaunchAndLockJob(taskRunDto, jobName, properties, arguments);
                }

                //update retry count
                int retryAttempt = batchJobRetryAttemptRepository.findByJobExecutionId(jobId).get(0).getRetryAttempt();
                batchJobRetryAttemptRepository.updateRetryAttempt(jobId, retryAttempt+1);
            }
        }
    }

    private boolean checkIfRetryLimitReached(Long jobId) {
        //todo put retry limit in property file
        boolean isRetryable;
        List<BatchJobRetryAttempt> batchJobRetryAttempt = batchJobRetryAttemptRepository.findByJobExecutionId(jobId);
        if(batchJobRetryAttempt.size() <= 0) {
            /*BatchJobRetryAttempt firstRetryAttempt = new BatchJobRetryAttempt();
            firstRetryAttempt.setRetryAttempt(0);
            firstRetryAttempt.setId(jobId);
            batchJobRetryAttemptRepository.save(firstRetryAttempt);*/

            dataflowJdbcTemplate.update("insert into batch_job_retry_attempt(job_execution_id, retry_attempt) values(?, ?)", jobId, 0);
        }
        isRetryable = batchJobRetryAttemptRepository.findByJobExecutionId(jobId).get(0).getRetryAttempt() < 3;
        return isRetryable;
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
                    progressDto = processStepProgress(stepExecution, "Importing Data", "");
                }
            }
            taskExecutionDto.setProgress(progressDto);
        }
        taskExecutionDto.setProgress(progressDto);
    }

}
