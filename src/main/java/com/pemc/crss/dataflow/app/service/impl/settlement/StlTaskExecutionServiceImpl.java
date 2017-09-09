package com.pemc.crss.dataflow.app.service.impl.settlement;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.pemc.crss.dataflow.app.dto.BaseTaskExecutionDto;
import com.pemc.crss.dataflow.app.dto.JobCalculationDto;
import com.pemc.crss.dataflow.app.dto.SettlementTaskExecutionDto;
import com.pemc.crss.dataflow.app.dto.StlJobGroupDto;
import com.pemc.crss.dataflow.app.dto.TaskRunDto;
import com.pemc.crss.dataflow.app.service.impl.AbstractTaskExecutionService;
import com.pemc.crss.shared.commons.reference.MeterProcessType;
import com.pemc.crss.shared.commons.reference.SettlementStepUtil;
import com.pemc.crss.shared.commons.util.DateUtil;
import com.pemc.crss.shared.core.dataflow.dto.DistinctStlReadyJob;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobAddtlParams;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobAdjRun;
import com.pemc.crss.shared.core.dataflow.entity.SettlementJobLock;
import com.pemc.crss.shared.core.dataflow.entity.ViewSettlementJob;
import com.pemc.crss.shared.core.dataflow.reference.StlCalculationType;
import com.pemc.crss.shared.core.dataflow.repository.BatchJobAddtlParamsRepository;
import com.pemc.crss.shared.core.dataflow.repository.BatchJobAdjRunRepository;
import com.pemc.crss.shared.core.dataflow.repository.SettlementJobLockRepository;
import com.pemc.crss.shared.core.dataflow.repository.ViewSettlementJobRepository;
import com.querydsl.core.BooleanBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameters;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import java.net.URISyntaxException;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static com.pemc.crss.dataflow.app.support.StlJobStage.*;
import static com.pemc.crss.shared.commons.reference.MeterProcessType.*;
import static com.pemc.crss.shared.commons.util.TaskUtil.*;
import static com.pemc.crss.shared.core.dataflow.entity.QSettlementJobLock.settlementJobLock;

@Slf4j
public abstract class StlTaskExecutionServiceImpl extends AbstractTaskExecutionService {

    static final String SPRING_BATCH_MODULE_STL_CALC = "crss-settlement-task-calculation";

    static final String SPRING_BATCH_MODULE_FILE_GEN = "crss-settlement-task-invoice-generation";

    private static final String PARTIAL = "PARTIAL-";
    private static final String FULL = "FULL-";

    // from batch_job_execution_context
    static final String INVOICE_GENERATION_FILENAME_KEY = "INVOICE_GENERATION_FILENAME";

    @Autowired
    private BatchJobAdjRunRepository batchJobAdjRunRepository;

    @Autowired
    private BatchJobAddtlParamsRepository batchJobAddtlParamsRepository;

    @Autowired
    private SettlementJobLockRepository settlementJobLockRepository;

    @Autowired
    ViewSettlementJobRepository viewSettlementJobRepository;

    // Abstract Methods

    abstract StlCalculationType getStlCalculationType();

    abstract String getDailyGenInputWorkspaceProfile();

    abstract String getPrelimGenInputWorkspaceProfile();

    abstract String getFinalGenInputWorkspaceProfile();

    abstract String getAdjustedMtrAdjGenInputWorkSpaceProfile();

    abstract String getAdjustedMtrFinGenInputWorkSpaceProfile();

    abstract List<String> getInputWorkSpaceStepsForSkipLogs();

    abstract String getDailyCalculateProfile();

    abstract String getPrelimCalculateProfile();

    abstract String getFinalCalculateProfile();

    abstract String getAdjustedMtrAdjCalculateProfile();

    abstract String getAdjustedMtrFinCalculateProfile();

    abstract List<String> getCalculateStepsForSkipLogs();

    abstract String getPrelimTaggingProfile();

    abstract String getFinalTaggingProfile();

    abstract String getAdjustedTaggingProfile();

    abstract String getPrelimGenFileProfile();

    abstract String getFinalGenFileProfile();

    abstract String getAdjustedGenFileProfile();

    /* findJobInstances methods start */

    private String parseGroupId(final String billingPeriod, final MeterProcessType processType, final String parentId) {
        if (processType.equals(ADJUSTED)) {
            return billingPeriod.concat(parentId);
        } else {
            return billingPeriod;
        }

    }

    private Date extractDateFromBillingPeriod(final String billingPeriod, final String param) {
        try {
            String toParse = null;
            switch (param) {
                case "startDate":
                    // returns 160126 from 160126160225
                    toParse = billingPeriod.substring(0, 6);
                    break;
                case "endDate":
                    // returns 160225 from 160126160225
                    toParse = billingPeriod.substring(6, billingPeriod.length());
                    break;
                case "dailyDate":
                    toParse = billingPeriod;
                    break;
                default:
                    // do nothing
            }

            return DateUtil.convertToDate(toParse, "yyMMdd");
        } catch (Exception e) {
            log.error("Unable to parse {} from the provided billing period: {}. Cause: {}",
                    param, billingPeriod, e);

            // return 1970-01-01 so as not to encounter npes later on.
            return new Date(0);
        }
    }

    SettlementTaskExecutionDto initializeTaskExecutionDto(final DistinctStlReadyJob stlReadyJob, final String parentId) {

        String billingPeriod = stlReadyJob.getBillingPeriod();

        SettlementTaskExecutionDto taskExecutionDto = new SettlementTaskExecutionDto();
        taskExecutionDto.setBillPeriodStr(stlReadyJob.getBillingPeriod());
        taskExecutionDto.setParentId(Long.parseLong(parentId));
        taskExecutionDto.setStlReadyGroupId(parseGroupId(stlReadyJob.getBillingPeriod(), stlReadyJob.getProcessType(), parentId));
        taskExecutionDto.setRunDateTime(DateUtil.convertToDate(stlReadyJob.getMaxJobExecStartTime()));

        // all queried stlReadyJob instance are filtered for 'COMPLETED' job runs
        taskExecutionDto.setStatus(convertStatus(BatchStatus.COMPLETED, "SETTLEMENT"));
        taskExecutionDto.setStlReadyStatus(BatchStatus.COMPLETED);

        if (!stlReadyJob.getProcessType().equals(DAILY)) {
            taskExecutionDto.setBillPeriodStartDate(extractDateFromBillingPeriod(billingPeriod, "startDate"));
            taskExecutionDto.setBillPeriodEndDate(extractDateFromBillingPeriod(billingPeriod, "endDate"));
        } else {
            taskExecutionDto.setDailyDate(extractDateFromBillingPeriod(billingPeriod, "dailyDate"));
        }

        taskExecutionDto.setProcessType(stlReadyJob.getProcessType());

        return taskExecutionDto;
    }

    List<JobInstance> findJobInstancesByNameAndProcessTypeAndParentId(final String jobNamePrefix,
                                                                      final MeterProcessType processType,
                                                                      final Long parentId) {
        List<String> processTypes = new ArrayList<>();
        processTypes.add(processType.name());

        // also add ADJUSTED processType for child job instances with same parentId as FINAL job instance
        if (processType == FINAL) {
            processTypes.add(ADJUSTED.name());
        }

        return dataFlowJdbcJobExecutionDao.findJobInstancesByNameAndProcessTypeAndParentId(jobNamePrefix, processTypes,
                String.valueOf(parentId));
    }

    JobExecution getJobExecutionFromJobInstance(final JobInstance jobInstance) {
        List<JobExecution> jobExecutions = getJobExecutions(jobInstance);

        // most of the time one jobInstance contains only one jobExecution.
        if (jobExecutions.size() > 1) {
            log.info("Found multiple job executions for JobInstance with id: {}", jobInstance.getInstanceId());
        }

        // by default jobExecutions are ordered by job_execution_id desc. We need to only get the first instance
        // since spring batch does not allow repeated job executions if the previous job execution is COMPLETED
        return jobExecutions.isEmpty() ? null : jobExecutions.get(0);
    }

    void initializeGenInputWorkSpace(final List<JobInstance> generateInputWsJobInstances,
                                     final Map<String, StlJobGroupDto> stlJobGroupDtoMap,
                                     final SettlementTaskExecutionDto taskExecutionDto,
                                     final String stlReadyGroupId) {

        for (JobInstance genWsStlJobInstance : generateInputWsJobInstances) {

            JobExecution genWsJobExec = getJobExecutionFromJobInstance(genWsStlJobInstance);

            Date billPeriodStartDate = taskExecutionDto.getBillPeriodStartDate();
            Date billPeriodEndDate = taskExecutionDto.getBillPeriodEndDate();

            BatchStatus currentBatchStatus = genWsJobExec.getStatus();
            JobParameters genInputWsJobParameters = genWsJobExec.getJobParameters();
            String groupId = genInputWsJobParameters.getString(GROUP_ID);
            Date genInputWsStartDate = genInputWsJobParameters.getDate(START_DATE);
            Date genInputWsEndDate = genInputWsJobParameters.getDate(END_DATE);

            final StlJobGroupDto stlJobGroupDto = stlJobGroupDtoMap.getOrDefault(groupId, new StlJobGroupDto());

            if (currentBatchStatus.isRunning()) {
                // for validation of gmr calculation in case stl amt is recalculated
                stlJobGroupDto.setRunningGenInputWorkspace(true);
            }

            boolean fullGenInputWs = billPeriodStartDate != null && billPeriodEndDate != null
                    && genInputWsStartDate.compareTo(billPeriodStartDate) == 0 && genInputWsEndDate.compareTo(billPeriodEndDate) == 0;

            final String jobGenInputWsStatus = fullGenInputWs
                    ? convertStatus(currentBatchStatus, FULL + GENERATE_IWS.getLabel())
                    : convertStatus(currentBatchStatus, PARTIAL + GENERATE_IWS.getLabel());

            List<JobCalculationDto> jobCalculationDtoList = stlJobGroupDto.getJobCalculationDtos();

            if (jobCalculationDtoList.isEmpty()) {
                stlJobGroupDto.setRunStartDateTime(genWsJobExec.getStartTime());
                stlJobGroupDto.setRunEndDateTime(genWsJobExec.getEndTime());
            }

            JobCalculationDto partialCalcDto = new JobCalculationDto(genWsJobExec.getStartTime(),
                    genWsJobExec.getEndTime(), genInputWsStartDate, genInputWsEndDate,
                    jobGenInputWsStatus, GENERATE_IWS, currentBatchStatus);

            // for skiplogs use
            partialCalcDto.setTaskSummaryList(showSummary(genWsJobExec, getInputWorkSpaceStepsForSkipLogs()));

            jobCalculationDtoList.add(partialCalcDto);

            stlJobGroupDto.setJobCalculationDtos(jobCalculationDtoList);
            stlJobGroupDto.setGroupId(groupId);

            Date latestJobExecStartDate = stlJobGroupDto.getLatestJobExecStartDate();
            if (latestJobExecStartDate == null || !latestJobExecStartDate.after(genWsJobExec.getStartTime())) {
                updateProgress(genWsJobExec, stlJobGroupDto);
            }

            Date maxPartialGenInputWsDate = stlJobGroupDto.getJobCalculationDtos().stream()
                    .filter(jobCalc -> jobCalc.getJobStage().equals(GENERATE_IWS))
                    .map(JobCalculationDto::getRunDate)
                    .max(Date::compareTo).get();

            stlJobGroupDto.setMaxPartialGenIwRunDate(maxPartialGenInputWsDate);
            stlJobGroupDtoMap.put(groupId, stlJobGroupDto);

            // for showing calculate stl amt button
            if (currentBatchStatus == BatchStatus.COMPLETED) {
                stlJobGroupDto.setHasCompletedGenInputWs(true);
            }

            if (stlReadyGroupId.equals(groupId)) {
                stlJobGroupDto.setHeader(true);
                taskExecutionDto.setParentStlJobGroupDto(stlJobGroupDto);
            }

        }
    }

    void initializeStlCalculation(final List<JobInstance> stlCalculationJobInstances,
                                  final Map<String, StlJobGroupDto> stlJobGroupDtoMap,
                                  final SettlementTaskExecutionDto taskExecutionDto,
                                  final String stlReadyGroupId) {

        for (JobInstance stlCalcJobInstance : stlCalculationJobInstances) {

            JobExecution stlCalcJobExec = getJobExecutionFromJobInstance(stlCalcJobInstance);

            Date billPeriodStartDate = taskExecutionDto.getBillPeriodStartDate();
            Date billPeriodEndDate = taskExecutionDto.getBillPeriodEndDate();

            BatchStatus currentBatchStatus = stlCalcJobExec.getStatus();
            JobParameters calcJobParameters = stlCalcJobExec.getJobParameters();
            String groupId = calcJobParameters.getString(GROUP_ID);
            Date calcStartDate = calcJobParameters.getDate(START_DATE);
            Date calcEndDate = calcJobParameters.getDate(END_DATE);

            final StlJobGroupDto stlJobGroupDto = stlJobGroupDtoMap.getOrDefault(groupId, new StlJobGroupDto());

            if (currentBatchStatus.isRunning()) {
                // for validation of gmr calculation in case stl amt is recalculated
                stlJobGroupDto.setRunningStlCalculation(true);
            }

            boolean fullCalculation = billPeriodStartDate != null && billPeriodEndDate != null
                    && calcStartDate.compareTo(billPeriodStartDate) == 0 && calcEndDate.compareTo(billPeriodEndDate) == 0;

            final String jobCalcStatus = fullCalculation
                    ? convertStatus(currentBatchStatus, FULL + CALCULATE_STL.getLabel())
                    : convertStatus(currentBatchStatus, PARTIAL + CALCULATE_STL.getLabel());

            List<JobCalculationDto> jobCalculationDtoList = stlJobGroupDto.getJobCalculationDtos();

            JobCalculationDto partialCalcDto = new JobCalculationDto(stlCalcJobExec.getStartTime(),
                    stlCalcJobExec.getEndTime(), calcStartDate, calcEndDate,
                    jobCalcStatus, CALCULATE_STL, currentBatchStatus);

            partialCalcDto.setTaskSummaryList(showSummary(stlCalcJobExec, getCalculateStepsForSkipLogs()));

            jobCalculationDtoList.add(partialCalcDto);

            stlJobGroupDto.setJobCalculationDtos(jobCalculationDtoList);
            stlJobGroupDto.setGroupId(groupId);

            Date latestJobExecStartDate = stlJobGroupDto.getLatestJobExecStartDate();
            if (latestJobExecStartDate == null || !latestJobExecStartDate.after(stlCalcJobExec.getStartTime())) {
                updateProgress(stlCalcJobExec, stlJobGroupDto);
            }

            Date maxPartialCalcDate = stlJobGroupDto.getJobCalculationDtos().stream()
                    .filter(jobCalc -> jobCalc.getJobStage().equals(CALCULATE_STL))
                    .map(JobCalculationDto::getRunDate)
                    .max(Date::compareTo).get();

            stlJobGroupDto.setMaxPartialCalcRunDate(maxPartialCalcDate);
            stlJobGroupDtoMap.put(groupId, stlJobGroupDto);

            if (stlReadyGroupId.equals(groupId)) {
                stlJobGroupDto.setHeader(true);
                taskExecutionDto.setParentStlJobGroupDto(stlJobGroupDto);
            }

        }
    }

    void initializeTagging(final List<JobInstance> taggingJobInstances,
                           final Map<String, StlJobGroupDto> stlJobGroupDtoMap,
                           final SettlementTaskExecutionDto taskExecutionDto,
                           final String stlReadyGroupId) {

        Set<String> tagNames = Sets.newHashSet();

        for (JobInstance taggingJobInstance : taggingJobInstances) {

            String tagStlJobName = taggingJobInstance.getJobName();
            if (tagNames.contains(tagStlJobName)) {
                continue;
            }

            JobExecution tagJobExecution = getJobExecutionFromJobInstance(taggingJobInstance);
            JobParameters tagJobParameters = tagJobExecution.getJobParameters();
            Date tagStartDate = tagJobParameters.getDate(START_DATE);
            Date tagEndDate = tagJobParameters.getDate(END_DATE);
            String groupId = tagJobParameters.getString(GROUP_ID);

            StlJobGroupDto stlJobGroupDto = stlJobGroupDtoMap.getOrDefault(groupId, new StlJobGroupDto());
            BatchStatus currentStatus = tagJobExecution.getStatus();
            stlJobGroupDto.setTaggingStatus(currentStatus);
            stlJobGroupDto.setGroupId(groupId);

            JobCalculationDto finalizeJobDto = new JobCalculationDto(tagJobExecution.getStartTime(),
                    tagJobExecution.getEndTime(), tagStartDate, tagEndDate,
                    convertStatus(currentStatus, FINALIZE.getLabel()), FINALIZE, currentStatus);

            stlJobGroupDto.getJobCalculationDtos().add(finalizeJobDto);

            if (!stlJobGroupDto.getLatestJobExecStartDate().after(tagJobExecution.getStartTime())) {
                updateProgress(tagJobExecution, stlJobGroupDto);
            }

            stlJobGroupDtoMap.put(groupId, stlJobGroupDto);

            if (stlReadyGroupId.equals(groupId)) {
                stlJobGroupDto.setHeader(true);
                taskExecutionDto.setParentStlJobGroupDto(stlJobGroupDto);
            }

            tagNames.add(tagStlJobName);
        }
    }

    void initializeFileGen(final List<JobInstance> fileGenJobInstances,
                           final Map<String, StlJobGroupDto> stlJobGroupDtoMap,
                           final SettlementTaskExecutionDto taskExecutionDto,
                           final String stlReadyGroupId) {

        Set<String> generationNames = Sets.newHashSet();

        for (JobInstance genFileJobInstance : fileGenJobInstances) {

            String generationStlJobName = genFileJobInstance.getJobName();
            if (generationNames.contains(generationStlJobName)) {
                continue;
            }

            JobExecution generationJobExecution = getJobExecutionFromJobInstance(genFileJobInstance);
            JobParameters generationJobParameters = generationJobExecution.getJobParameters();
            String groupId = generationJobParameters.getString(GROUP_ID);

            StlJobGroupDto stlJobGroupDto = stlJobGroupDtoMap.getOrDefault(groupId, new StlJobGroupDto());
            BatchStatus currentStatus = generationJobExecution.getStatus();
            stlJobGroupDto.setInvoiceGenerationStatus(currentStatus);
            stlJobGroupDto.setGroupId(groupId);
            stlJobGroupDto.setRunId(generationJobParameters.getLong(RUN_ID));
            stlJobGroupDto.setRunStartDateTime(generationJobExecution.getStartTime());
            stlJobGroupDto.setRunEndDateTime(generationJobExecution.getEndTime());

            if (!stlJobGroupDto.getLatestJobExecStartDate().after(generationJobExecution.getStartTime())) {
                updateProgress(generationJobExecution, stlJobGroupDto);
            }

            stlJobGroupDtoMap.put(groupId, stlJobGroupDto);

            Optional.ofNullable(generationJobExecution.getExecutionContext()
                    .get(INVOICE_GENERATION_FILENAME_KEY)).ifPresent(val ->
                    stlJobGroupDto.setInvoiceGenFolder((String) val));

            if (stlReadyGroupId.equals(groupId)) {
                stlJobGroupDto.setHeader(true);
                taskExecutionDto.setParentStlJobGroupDto(stlJobGroupDto);
            }

            generationNames.add(generationStlJobName);
        }
    }

    SortedSet<LocalDate> getRemainingDatesForCalculation(final List<JobCalculationDto> jobDtos,
                                               final Date billPeriodStart,
                                               final Date billPeriodEnd) {

        SortedSet<LocalDate> remainingCalcDates = createRange(billPeriodStart, billPeriodEnd);

        List<JobCalculationDto> filteredJobDtosAsc = jobDtos.stream().filter(jobDto ->
                Arrays.asList(CALCULATE_STL, GENERATE_IWS).contains(jobDto.getJobStage())
                && jobDto.getJobExecStatus() == BatchStatus.COMPLETED)
                .sorted(Comparator.comparing(JobCalculationDto::getRunDate)).collect(Collectors.toList());

        for (JobCalculationDto jobDto : filteredJobDtosAsc) {
            switch (jobDto.getJobStage()) {
                case GENERATE_IWS:
                    addDateRangeTo(remainingCalcDates, jobDto.getStartDate(), jobDto.getEndDate());
                    break;
                case CALCULATE_STL:
                    removeDateRangeFrom(remainingCalcDates, jobDto.getStartDate(), jobDto.getEndDate());
                    break;
                default:
                    // do nothing
            }
        }

        return remainingCalcDates;
    }

    SortedSet<LocalDate> getRemainingDatesForGenInputWs(final List<JobCalculationDto> jobDtos,
                                                        final Date billPeriodStart,
                                                        final Date billPeriodEnd) {

        SortedSet<LocalDate> remainingGenInputWsDates = createRange(billPeriodStart, billPeriodEnd);

        List<JobCalculationDto> filteredGenInputWsDtosAsc = jobDtos.stream()
                .filter(jobDto ->
                        Objects.equals(GENERATE_IWS, jobDto.getJobStage())
                                && jobDto.getJobExecStatus() == BatchStatus.COMPLETED)
                .sorted(Comparator.comparing(JobCalculationDto::getRunDate))
                .collect(Collectors.toList());

        for (JobCalculationDto jobDto : filteredGenInputWsDtosAsc) {
            removeDateRangeFrom(remainingGenInputWsDates, jobDto.getStartDate(), jobDto.getEndDate());
        }

        return remainingGenInputWsDates;
    }

    void determineStlJobGroupDtoStatus(final StlJobGroupDto stlJobGroupDto, final boolean isDaily) {
        stlJobGroupDto.getSortedJobCalculationDtos().stream().findFirst().ifPresent(jobDto -> {

            if (jobDto.getJobExecStatus().isRunning() || isDaily) {
                stlJobGroupDto.setStatus(jobDto.getStatus());
            } else {
                // special rules for generate input ws and calculations
                switch (jobDto.getJobStage()) {
                    case GENERATE_IWS:
                        if (stlJobGroupDto.getRemainingDatesGenInputWs().isEmpty()) {
                            stlJobGroupDto.setStatus(convertStatus(jobDto.getJobExecStatus(), FULL + GENERATE_IWS.getLabel()));
                        } else {
                            stlJobGroupDto.setStatus(convertStatus(jobDto.getJobExecStatus(), PARTIAL + GENERATE_IWS.getLabel()));
                        }
                        break;
                    case CALCULATE_STL:
                        if (stlJobGroupDto.getRemainingDatesCalc().isEmpty()) {
                            stlJobGroupDto.setStatus(convertStatus(jobDto.getJobExecStatus(), FULL + CALCULATE_STL.getLabel()));
                        } else {
                            stlJobGroupDto.setStatus(convertStatus(jobDto.getJobExecStatus(), PARTIAL + CALCULATE_STL.getLabel()));
                        }
                        break;
                    default:
                        stlJobGroupDto.setStatus(jobDto.getStatus());
                }
            }

        });
    }

    SortedSet<LocalDate> getOutdatedTradingDates(final List<JobCalculationDto> jobDtos,
                                                 final List<ViewSettlementJob> stlReadyJobs,
                                                 final Date billPeriodStart,
                                                 final Date billPeriodEnd) {

        Map<LocalDate, LocalDateTime> stlReadyDateMap = new HashMap<>();

        createRange(billPeriodStart, billPeriodEnd).forEach(tradingDate -> stlReadyDateMap.put(tradingDate, LocalDateTime.MIN));

        // create map of updated trading dates from stlReady per day
        stlReadyJobs.forEach(stlReadyJob -> {
            Date stlReadyStartDate = DateUtil.convertToDate(stlReadyJob.getStartDate());
            Date stlReadyEndDate = DateUtil.convertToDate(stlReadyJob.getEndDate());

            SortedSet<LocalDate> stlReadyDates = createRange(stlReadyStartDate, stlReadyEndDate);

            stlReadyDates.forEach(stlReadyDate -> {
                if (stlReadyDateMap.containsKey(stlReadyDate) && stlReadyJob.getJobExecStartTime()
                        .isAfter(stlReadyDateMap.get(stlReadyDate))) {

                    // update tradingDateMap with the latest stlReadyRun per day
                    stlReadyDateMap.put(stlReadyDate, stlReadyJob.getJobExecStartTime());
                }
            });
        });

        List<JobCalculationDto> filteredGenInputWsDtosAsc = jobDtos.stream()
                .filter(jobDto ->
                        Objects.equals(GENERATE_IWS, jobDto.getJobStage())
                                && jobDto.getJobExecStatus() == BatchStatus.COMPLETED)
                .sorted(Comparator.comparing(JobCalculationDto::getRunDate))
                .collect(Collectors.toList());

        Map<LocalDate, LocalDateTime> genInputWsDateMap = new HashMap<>();

        // create map of updated trading dates from gen input workspace per day
        filteredGenInputWsDtosAsc.forEach(genInputWsDto -> createRange(genInputWsDto.getStartDate(), genInputWsDto.getEndDate())
                .forEach(genInputWsDate -> genInputWsDateMap.put(genInputWsDate,
                        DateUtil.convertToLocalDateTime(genInputWsDto.getRunDate()))));

        SortedSet<LocalDate> outdatedTradingDates = new TreeSet<>();

        genInputWsDateMap.forEach((genInputWsDate, genInputWsExecDateTime) -> {

            // determine outdated dates
            if (stlReadyDateMap.containsKey(genInputWsDate)
                    && stlReadyDateMap.get(genInputWsDate).isAfter(genInputWsExecDateTime)) {
                outdatedTradingDates.add(genInputWsDate);
            }
        });

        return outdatedTradingDates;
    }

    private SortedSet<LocalDate> createRange(final Date start, final Date end) {
        if (start == null || end == null) {
            return new TreeSet<>();
        }

        SortedSet<LocalDate> localDates = new TreeSet<>();

        LocalDate currentDate = DateUtil.convertToLocalDate(start);
        LocalDate endDate = DateUtil.convertToLocalDate(end);

        while (!currentDate.isAfter(endDate)) {
            localDates.add(currentDate);
            currentDate = currentDate.plusDays(1);
        }

        return localDates;
    }

    void updateProgress(JobExecution jobExecution, StlJobGroupDto dto) {
        List<String> runningTasks = Lists.newArrayList();
        if (!jobExecution.getStepExecutions().isEmpty()) {
            jobExecution.getStepExecutions().parallelStream()
                    .filter(stepExecution -> stepExecution.getStatus().isRunning())
                    .forEach(stepExecution -> {
                        Map<String, String> map = SettlementStepUtil.getProgressNameTaskMap();
                        String stepName = stepExecution.getStepName();
                        if (map.containsKey(stepName)) {
                            runningTasks.add(map.get(stepName));
                        } else {
                            log.warn("Step name {} not existing in current mapping.", stepName);
                        }
                    });
        }

        dto.setRunningSteps(runningTasks);
        dto.setLatestJobExecStartDate(jobExecution.getStartTime());
        dto.setLatestJobExecEndDate(jobExecution.getEndTime());
    }

    void determineIfJobsAreLocked(final SettlementTaskExecutionDto taskExecutionDto) {

        LocalDateTime billPeriodStart = DateUtil.convertToLocalDateTime(taskExecutionDto.getBillPeriodStartDate());
        LocalDateTime billPeriodEnd =   DateUtil.convertToLocalDateTime(taskExecutionDto.getBillPeriodEndDate());

        if (taskExecutionDto.getProcessType() == PRELIM) {
            boolean prelimIsLocked = settlementJobLockRepository.billingPeriodIsFinalized(billPeriodStart, billPeriodEnd,
                    PRELIM.name(), getStlCalculationType().name());

            taskExecutionDto.getParentStlJobGroupDto().setLocked(prelimIsLocked);

        } else {

            List<SettlementJobLock> stlJobLocks = settlementJobLockRepository
                    .findByStartDateAndEndDateAndStlCalculationTypeAndProcessTypeIn(billPeriodStart, billPeriodEnd,
                            getStlCalculationType(), Arrays.asList(FINAL, ADJUSTED));

            ViewSettlementJob latestStlJobFromMetering = viewSettlementJobRepository
                    .findFirstByProcessTypeInAndAndBillingPeriodAndStatusOrderByJobExecStartTimeDesc(Arrays.asList(ADJUSTED, FINAL),
                            taskExecutionDto.getBillPeriodStr(), BatchStatus.COMPLETED);

            for (StlJobGroupDto stlJobGroupDto : taskExecutionDto.getStlJobGroupDtoMap().values()) {

                stlJobLocks.stream().filter(stlJobLock -> stlJobLock.getGroupId().equals(stlJobGroupDto.getGroupId())
                        && stlJobLock.getParentJobId().equals(taskExecutionDto.getParentId()))
                        .findFirst().ifPresent(stlLock -> stlJobGroupDto.setLocked(stlLock.isLocked()));

                // additional lock checking for adjusted type if it's not yet locked
                if (ADJUSTED.equals(taskExecutionDto.getProcessType()) && !stlJobGroupDto.isLocked()) {
                    stlJobGroupDto.setLocked(true);

                    // release lock if FINAL is already locked
                    stlJobLocks.stream().filter(stlJobLock -> stlJobLock.getProcessType() == FINAL && stlJobLock.isLocked())
                            .findFirst().ifPresent(finalStlLock -> stlJobGroupDto.setLocked(false));
                }

                // Set canRunAdjustment
                if (stlJobLocks.stream().allMatch(SettlementJobLock::isLocked)) {
                    // get latest Settlement Job Lock check if groupId matches
                    stlJobLocks.stream().sorted(Collections.reverseOrder(Comparator.comparing(SettlementJobLock::getLockDate)))
                            .findFirst().ifPresent(stlJobLock -> stlJobGroupDto
                            .setCanRunAdjustment(Objects.equals(stlJobLock.getGroupId(), stlJobGroupDto.getGroupId()))
                    );

                    // additional checking from metering triggered adjustments
                    if (latestStlJobFromMetering != null) {

                        String latestStlJobGroupId = parseGroupId(latestStlJobFromMetering.getBillingPeriod(),
                                latestStlJobFromMetering.getProcessType(), latestStlJobFromMetering.getParentId());

                        if (!Objects.equals(latestStlJobGroupId, taskExecutionDto.getParentStlJobGroupDto().getGroupId())) {
                            stlJobGroupDto.setCanRunAdjustment(false);
                        }
                    }

                }
            }
        }

    }

    private void removeDateRangeFrom(final SortedSet<LocalDate> remainingDates, final Date calcStartDate,
                                     final Date calcEndDate) {
        SortedSet<LocalDate> datesToRemove = createRange(calcStartDate, calcEndDate);

        datesToRemove.forEach(date -> {
            if (remainingDates.contains(date)) {
                remainingDates.remove(date);
            }
        });
    }

    private void addDateRangeTo(final SortedSet<LocalDate> remainingDates, final Date calcStartDate,
                                final Date calcEndDate) {

       SortedSet<LocalDate> datesToAdd = createRange(calcStartDate, calcEndDate);

       datesToAdd.forEach(date -> {
           if (!remainingDates.contains(date)) {
               remainingDates.add(date);
           }
       });
    }

    /* findJobInstances methods end */

    /* launchJob methods start */
    void validateJobName(final String jobName) {
        Preconditions.checkNotNull(jobName, "Job Name must not be null");
        Preconditions.checkState(batchJobRunLockRepository.countByJobNameAndLockedIsTrue(jobName) == 0,
                "There is an existing ".concat(jobName).concat(" job running"));
    }

    List<String> initializeJobArguments(final TaskRunDto taskRunDto, final Long runId, final String groupId,
                                        final String processType) {
        List<String> arguments = Lists.newArrayList();
        arguments.add(concatKeyValue(PARENT_JOB, taskRunDto.getParentJob(), "long"));
        arguments.add(concatKeyValue(RUN_ID, String.valueOf(runId), "long"));
        arguments.add(concatKeyValue(GROUP_ID, groupId));
        arguments.add(concatKeyValue(USERNAME, taskRunDto.getCurrentUser()));
        arguments.add(concatKeyValue(PROCESS_TYPE, processType));

        return arguments;
    }

    void launchGenerateInputWorkspaceJob(final TaskRunDto taskRunDto) throws URISyntaxException {
        final Long runId = System.currentTimeMillis();
        final String groupId = taskRunDto.isNewGroup() ? runId.toString() : taskRunDto.getGroupId();
        final String type = taskRunDto.getMeterProcessType();

        List<String> arguments = initializeJobArguments(taskRunDto, runId, groupId, type);

        List<String> properties = Lists.newArrayList();

        LocalDateTime billPeriodStartDate = DateUtil.parseStringDateToLocalDateTime(taskRunDto.getBaseStartDate(),
                DateUtil.DEFAULT_DATE_FORMAT);
        LocalDateTime billPeriodEndDate = DateUtil.parseStringDateToLocalDateTime(taskRunDto.getBaseEndDate(),
                DateUtil.DEFAULT_DATE_FORMAT);

        MeterProcessType processType = MeterProcessType.valueOf(type);

        switch (processType) {
            case DAILY:
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(getDailyGenInputWorkspaceProfile())));
                arguments.add(concatKeyValue(START_DATE, taskRunDto.getTradingDate(), "date"));
                break;
            case PRELIM:
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(getPrelimGenInputWorkspaceProfile())));
                arguments.add(concatKeyValue(START_DATE, taskRunDto.getStartDate(), "date"));
                arguments.add(concatKeyValue(END_DATE, taskRunDto.getEndDate(), "date"));
                break;
            case FINAL:
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(getFinalGenInputWorkspaceProfile())));
                arguments.add(concatKeyValue(START_DATE, taskRunDto.getStartDate(), "date"));
                arguments.add(concatKeyValue(END_DATE, taskRunDto.getEndDate(), "date"));

                // only save in batch job adj run repo if trading amounts for tracking groupdId for delta calcs
                if (batchJobAdjRunRepository.countByGroupIdAndBillingPeriodStartAndBillingPeriodEnd(groupId,
                        billPeriodStartDate, billPeriodEndDate) < 1 && getStlCalculationType() == StlCalculationType.TRADING_AMOUNTS) {
                    log.info("Saving to batchjobadjrun with groupId=[{}] and billingPeriodStart=[{}] and billingPeriodEnd=[{}]",
                            groupId, billPeriodStartDate, billPeriodEndDate);
                    saveAdjRun(FINAL, taskRunDto.getParentJob(), groupId, billPeriodStartDate, billPeriodEndDate);
                }
                break;
            case ADJUSTED:
                boolean finalBased = MeterProcessType.valueOf(taskRunDto.getBaseType()).equals(FINAL);

                // only save in batch job adj run repo if trading amounts for tracking groupdId for delta calcs
                if (batchJobAdjRunRepository.countByGroupIdAndBillingPeriodStartAndBillingPeriodEnd(groupId,
                        billPeriodStartDate, billPeriodEndDate) < 1 && getStlCalculationType() == StlCalculationType.TRADING_AMOUNTS) {
                    log.info("Saving to batchjobadjrun with groupId=[{}] and billingPeriodStart=[{}] and billingPeriodEnd=[{}]",
                            groupId, billPeriodStartDate, billPeriodEndDate);
                    saveAdjRun(ADJUSTED, taskRunDto.getParentJob(), groupId, billPeriodStartDate, billPeriodEndDate);
                }

                final String activeProfile = finalBased ? getAdjustedMtrFinGenInputWorkSpaceProfile() :
                        getAdjustedMtrAdjGenInputWorkSpaceProfile();

                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(activeProfile)));
                arguments.add(concatKeyValue(START_DATE, taskRunDto.getStartDate(), "date"));
                arguments.add(concatKeyValue(END_DATE, taskRunDto.getEndDate(), "date"));
                break;
            default:
                throw new RuntimeException("Failed to launch job. Unhandled processType: " + type);
        }

        // Create SettlementJobLock. Do not include daily since it does not have finalize job
        if (processType != DAILY) {
            BooleanBuilder predicate = new BooleanBuilder();
            predicate.and(settlementJobLock.groupId.eq(groupId)
                     .and(settlementJobLock.processType.eq(processType))
                     .and(settlementJobLock.stlCalculationType.eq(getStlCalculationType())));

            if (!settlementJobLockRepository.exists(predicate)) {
                log.info("Creating new Settlement Job Lock. groupdId {}, stlCalculationType {}, processType: {}",
                        groupId, getStlCalculationType(), processType);
                SettlementJobLock jobLock = new SettlementJobLock();
                jobLock.setCreatedDatetime(LocalDateTime.now());
                jobLock.setStartDate(DateUtil.parseLocalDate(taskRunDto.getBaseStartDate(), "yyyy-MM-dd").atStartOfDay());
                jobLock.setEndDate(DateUtil.parseLocalDate(taskRunDto.getBaseEndDate(), "yyyy-MM-dd").atStartOfDay());
                jobLock.setGroupId(groupId);
                jobLock.setParentJobId(Long.valueOf(taskRunDto.getParentJob()));
                jobLock.setStlCalculationType(getStlCalculationType());
                jobLock.setProcessType(processType);
                jobLock.setLocked(false);

                settlementJobLockRepository.save(jobLock);
            }

        }

        log.info("Running generate input workspace job name={}, properties={}, arguments={}", taskRunDto.getJobName(), properties, arguments);

        launchJob(SPRING_BATCH_MODULE_STL_CALC, properties, arguments);
        lockJob(taskRunDto);
    }

    void launchCalculateJob(final TaskRunDto taskRunDto) throws URISyntaxException {
        final Long runId = System.currentTimeMillis();
        final String groupId = taskRunDto.getGroupId();
        final String type = taskRunDto.getMeterProcessType();

        List<String> arguments = initializeJobArguments(taskRunDto, runId, groupId, type);

        List<String> properties = Lists.newArrayList();

        switch (MeterProcessType.valueOf(type)) {
            case DAILY:
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(getDailyCalculateProfile())));
                arguments.add(concatKeyValue(START_DATE, taskRunDto.getTradingDate(), "date"));
                break;
            case PRELIM:
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(getPrelimCalculateProfile())));
                arguments.add(concatKeyValue(START_DATE, taskRunDto.getStartDate(), "date"));
                arguments.add(concatKeyValue(END_DATE, taskRunDto.getEndDate(), "date"));
                break;
            case FINAL:
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(getFinalCalculateProfile())));
                arguments.add(concatKeyValue(START_DATE, taskRunDto.getStartDate(), "date"));
                arguments.add(concatKeyValue(END_DATE, taskRunDto.getEndDate(), "date"));
                break;
            case ADJUSTED:
                boolean finalBased = MeterProcessType.valueOf(taskRunDto.getBaseType()).equals(FINAL);

                final String activeProfile = finalBased ? getAdjustedMtrFinCalculateProfile() :
                        getAdjustedMtrAdjCalculateProfile();

                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(activeProfile)));
                arguments.add(concatKeyValue(START_DATE, taskRunDto.getStartDate(), "date"));
                arguments.add(concatKeyValue(END_DATE, taskRunDto.getEndDate(), "date"));
                break;
            default:
                throw new RuntimeException("Failed to launch job. Unhandled processType: " + type);
        }

        log.info("Running calculate job name={}, properties={}, arguments={}", taskRunDto.getJobName(), properties, arguments);

        launchJob(SPRING_BATCH_MODULE_STL_CALC, properties, arguments);
        lockJob(taskRunDto);
    }

    void launchFinalizeJob(final TaskRunDto taskRunDto) throws URISyntaxException {
        final Long runId = System.currentTimeMillis();
        final String groupId = taskRunDto.getGroupId();
        final String type = taskRunDto.getMeterProcessType();

        List<String> arguments = initializeJobArguments(taskRunDto, runId, groupId, type);
        arguments.add(concatKeyValue(START_DATE, taskRunDto.getBaseStartDate(), "date"));
        arguments.add(concatKeyValue(END_DATE, taskRunDto.getBaseEndDate(), "date"));

        List<String> properties = Lists.newArrayList();

        switch (MeterProcessType.valueOf(type)) {
            case PRELIM:
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(
                        getPrelimTaggingProfile())));
                break;
            case FINAL:
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(
                        getFinalTaggingProfile())));
                break;
            case ADJUSTED:
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(
                        getAdjustedTaggingProfile())));
                break;
            default:
                throw new RuntimeException("Failed to launch job. Unhandled processType: " + type);
        }

        log.info("Running calculate gmr job name={}, properties={}, arguments={}", taskRunDto.getJobName(), properties, arguments);

        launchJob(SPRING_BATCH_MODULE_STL_CALC, properties, arguments);
        lockJob(taskRunDto);
    }

    void launchGenerateFileJob(final TaskRunDto taskRunDto) throws URISyntaxException {
        final Long runId = System.currentTimeMillis();
        final String groupId = taskRunDto.getGroupId();
        final String type = taskRunDto.getMeterProcessType();

        List<String> arguments = initializeJobArguments(taskRunDto, runId, groupId, type);
        arguments.add(concatKeyValue(START_DATE, taskRunDto.getBaseStartDate(), "date"));
        arguments.add(concatKeyValue(END_DATE, taskRunDto.getBaseEndDate(), "date"));

        List<String> properties = Lists.newArrayList();

        switch (MeterProcessType.valueOf(type)) {
            case PRELIM:
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(
                        getPrelimGenFileProfile())));
                break;
            case FINAL:
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(
                        getFinalGenFileProfile())));
                saveAMSadditionalParams(runId, taskRunDto);
                break;
            case ADJUSTED:
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(
                        getAdjustedGenFileProfile())));
                saveAMSadditionalParams(runId, taskRunDto);
                break;
            default:
                throw new RuntimeException("Failed to launch job. Unhandled processType: " + type);
        }

        log.info("Running generate file job name={}, properties={}, arguments={}", taskRunDto.getJobName(), properties, arguments);

        launchJob(SPRING_BATCH_MODULE_FILE_GEN, properties, arguments);
        lockJob(taskRunDto);
    }

    private void saveAdjRun(MeterProcessType type, String jobId, String groupId, LocalDateTime start, LocalDateTime end) {
        BatchJobAdjRun batchJobAdjRun = new BatchJobAdjRun();
        batchJobAdjRun.setJobId(jobId);
        batchJobAdjRun.setAdditionalCompensation(false);
        batchJobAdjRun.setGroupId(groupId);
        batchJobAdjRun.setMeterProcessType(type);
        batchJobAdjRun.setBillingPeriodStart(start);
        batchJobAdjRun.setBillingPeriodEnd(end);
        batchJobAdjRun.setOutputReady(false);
        batchJobAdjRunRepository.save(batchJobAdjRun);
    }

    void saveAMSadditionalParams(final Long runId, final TaskRunDto taskRunDto) {
        log.info("Saving additional AMS params. TaskRunDto: {}", taskRunDto);
        try {
            BatchJobAddtlParams batchJobAddtlParamsInvoiceDate = new BatchJobAddtlParams();
            batchJobAddtlParamsInvoiceDate.setRunId(runId);
            batchJobAddtlParamsInvoiceDate.setType("DATE");
            batchJobAddtlParamsInvoiceDate.setKey(AMS_INVOICE_DATE);
            batchJobAddtlParamsInvoiceDate.setDateVal(DateUtil.getStartRangeDate(taskRunDto.getAmsInvoiceDate()));
            batchJobAddtlParamsRepository.save(batchJobAddtlParamsInvoiceDate);

            BatchJobAddtlParams batchJobAddtlParamsDueDate = new BatchJobAddtlParams();
            batchJobAddtlParamsDueDate.setRunId(runId);
            batchJobAddtlParamsDueDate.setType("DATE");
            batchJobAddtlParamsDueDate.setKey(AMS_DUE_DATE);
            batchJobAddtlParamsDueDate.setDateVal(DateUtil.getStartRangeDate(taskRunDto.getAmsDueDate()));
            batchJobAddtlParamsRepository.save(batchJobAddtlParamsDueDate);

            switch (getStlCalculationType()) {
                case TRADING_AMOUNTS:
                    BatchJobAddtlParams batchJobAddtlParamsRemarksInv = new BatchJobAddtlParams();
                    batchJobAddtlParamsRemarksInv.setRunId(runId);
                    batchJobAddtlParamsRemarksInv.setType("STRING");
                    batchJobAddtlParamsRemarksInv.setKey(AMS_REMARKS_INV);
                    batchJobAddtlParamsRemarksInv.setStringVal(taskRunDto.getAmsRemarksInv());
                    batchJobAddtlParamsRepository.save(batchJobAddtlParamsRemarksInv);
                    break;
                case ENERGY_MARKET_FEE:
                case RESERVE_MARKET_FEE:
                    BatchJobAddtlParams batchJobAddtlParamsRemarksMf = new BatchJobAddtlParams();
                    batchJobAddtlParamsRemarksMf.setRunId(runId);
                    batchJobAddtlParamsRemarksMf.setType("STRING");
                    batchJobAddtlParamsRemarksMf.setKey(AMS_REMARKS_MF);
                    batchJobAddtlParamsRemarksMf.setStringVal(taskRunDto.getAmsRemarksMf());
                    batchJobAddtlParamsRepository.save(batchJobAddtlParamsRemarksMf);
                    break;
                default:
                    // do nothing
            }

        } catch (ParseException e) {
            log.error("Error parsing additional batch job params for AMS: {}", e);
        }
    }

    /* launchJob methods end */

    // unused inherited methods
    @Override
    public Page<? extends BaseTaskExecutionDto> findJobInstances(Pageable pageable, String type, String status, String mode, String runStartDate, String tradingStartDate, String tradingEndDate, String username) {
        return null;
    }

    @Override
    public void relaunchFailedJob(long jobId) throws URISyntaxException {

    }

    @Override
    public Page<? extends BaseTaskExecutionDto> findJobInstances(Pageable pageable) {
        return null;
    }
}
