package com.pemc.crss.dataflow.app.service.impl.settlement;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.pemc.crss.dataflow.app.dto.*;
import com.pemc.crss.dataflow.app.service.impl.AbstractTaskExecutionService;
import com.pemc.crss.dataflow.app.support.StlJobStage;
import com.pemc.crss.shared.commons.reference.MeterProcessType;
import com.pemc.crss.shared.commons.reference.SettlementStepUtil;
import com.pemc.crss.shared.commons.util.DateTimeUtil;
import com.pemc.crss.shared.commons.util.DateUtil;
import com.pemc.crss.shared.core.dataflow.dto.DistinctStlReadyJob;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobAddtlParams;
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
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.net.URISyntaxException;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static com.pemc.crss.dataflow.app.support.StlJobStage.*;
import static com.pemc.crss.shared.commons.reference.MeterProcessType.*;
import static com.pemc.crss.shared.commons.util.TaskUtil.*;
import static com.pemc.crss.shared.core.dataflow.entity.QSettlementJobLock.settlementJobLock;

@Slf4j
public abstract class StlTaskExecutionServiceImpl extends AbstractTaskExecutionService {

    static final String SPRING_BATCH_MODULE_STL_CALC = "crss-settlement-task-calculation";

    static final String SPRING_BATCH_MODULE_FILE_GEN = "crss-settlement-task-invoice-generation";

    static final String PARTIAL = "PARTIAL-";
    static final String FULL = "FULL-";

    // from batch_job_execution_context
    static final String INVOICE_GENERATION_FILENAME_KEY = "INVOICE_GENERATION_FILENAME";

    @Autowired
    private BatchJobAdjRunRepository batchJobAdjRunRepository;

    @Autowired
    SettlementJobLockRepository settlementJobLockRepository;

    @Autowired
    ViewSettlementJobRepository viewSettlementJobRepository;

    @Autowired
    BatchJobAddtlParamsRepository batchJobAddtlParamsRepository;

    // Abstract Methods

    abstract StlCalculationType getStlCalculationType();

    abstract String getDailyGenInputWorkspaceProfile();

    abstract String getPrelimGenInputWorkspaceProfile();

    abstract String getFinalGenInputWorkspaceProfile();

    abstract String getAdjustedMtrAdjGenInputWorkSpaceProfile();

    abstract String getAdjustedMtrFinGenInputWorkSpaceProfile();

    abstract Map<String, String> getInputWorkSpaceStepsForSkipLogs();

    abstract String getDailyCalculateProfile();

    abstract String getPrelimCalculateProfile();

    abstract String getFinalCalculateProfile();

    abstract String getAdjustedMtrAdjCalculateProfile();

    abstract String getAdjustedMtrFinCalculateProfile();

    abstract Map<String, String> getCalculateStepsForSkipLogs();

    abstract String getPrelimTaggingProfile();

    abstract String getFinalTaggingProfile();

    abstract String getAdjustedTaggingProfile();

    abstract String getDailyTaggingProfile();

    abstract String getPrelimGenFileProfile();

    abstract String getPrelimGenBillStatementProfile();

    abstract String getFinalGenFileProfile();

    abstract String getAdjustedGenFileProfile();

    /* findJobInstances methods start */

    private String parseGroupId(final String billingPeriod, final MeterProcessType processType, final String parentId) {
        if (processType.equals(ADJUSTED)) {
            return parentId;
        } else {
            return billingPeriod;
        }

    }

    SettlementTaskExecutionDto initializeTaskExecutionDto(final DistinctStlReadyJob stlReadyJob, final String parentId) {

        String billingPeriod = stlReadyJob.getBillingPeriod();

        SettlementTaskExecutionDto taskExecutionDto = new SettlementTaskExecutionDto();
        taskExecutionDto.setBillPeriodStr(stlReadyJob.getBillingPeriod());
        taskExecutionDto.setParentId(Long.parseLong(parentId));
        taskExecutionDto.setStlReadyGroupId(parseGroupId(stlReadyJob.getBillingPeriod(), stlReadyJob.getProcessType(), parentId));
        taskExecutionDto.setRunDateTime(DateUtil.convertToDate(stlReadyJob.getMaxJobExecStartTime()));
        taskExecutionDto.setRegionGroup(stlReadyJob.getRegionGroup());

        // all queried stlReadyJob instance are filtered for 'COMPLETED' job runs
        taskExecutionDto.setStatus(convertStatus(BatchStatus.COMPLETED, "SETTLEMENT"));
        taskExecutionDto.setStlReadyStatus(BatchStatus.COMPLETED);

        taskExecutionDto.setBillPeriodStartDate(DateUtil.extractDateFromBillingPeriod(billingPeriod, "startDate"));
        taskExecutionDto.setBillPeriodEndDate(DateUtil.extractDateFromBillingPeriod(billingPeriod, "endDate"));

        taskExecutionDto.setDailyDate(DateUtil.extractDateFromBillingPeriod(billingPeriod, "dailyDate"));

        taskExecutionDto.setProcessType(stlReadyJob.getProcessType());

        return taskExecutionDto;
    }

    List<JobInstance> findJobInstancesByNameAndProcessTypeAndParentIdAndRegionGroup(final String jobNamePrefix,
                                                                                    final MeterProcessType processType,
                                                                                    final Long parentId,
                                                                                    final String regionGroup) {
        List<String> processTypes = new ArrayList<>();
        processTypes.add(processType.name());

        // also add ADJUSTED processType for child job instances with same parentId as FINAL job instance
        if (processType == FINAL) {
            processTypes.add(ADJUSTED.name());
        }

        return dataFlowJdbcJobExecutionDao.findJobInstancesByNameAndProcessTypeAndParentIdAndRegionGroup(jobNamePrefix, processTypes,
                String.valueOf(parentId), regionGroup);
    }

    JobExecution getJobExecutionFromJobInstance(final JobInstance jobInstance) {
        List<JobExecution> jobExecutions = getJobExecutionsNoStepContext(jobInstance);

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
            Long genInputWsRunId = genInputWsJobParameters.getLong(RUN_ID);

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

            String segregateNss = Optional.ofNullable(
                    batchJobAddtlParamsRepository.findByRunIdAndKey(genInputWsRunId, "segregateNssByLlccPay"))
                    .map(BatchJobAddtlParams::getStringVal).orElse(null);
            partialCalcDto.setSegregateNss(segregateNss);

            // for skiplogs use
            partialCalcDto.setTaskSummaryList(showSummaryWithLabel(genWsJobExec, getInputWorkSpaceStepsForSkipLogs()));

            jobCalculationDtoList.add(partialCalcDto);

            stlJobGroupDto.setJobCalculationDtos(jobCalculationDtoList);
            stlJobGroupDto.setGroupId(groupId);
            stlJobGroupDto.setRegionGroup(taskExecutionDto.getRegionGroup());

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
            Long calcRunId = calcJobParameters.getLong(RUN_ID);

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

            String segregateNss = Optional.ofNullable(batchJobAddtlParamsRepository.findByRunIdAndKey(calcRunId, "segregateNssByLlccPay"))
                    .map(BatchJobAddtlParams::getStringVal).orElse(null);
            partialCalcDto.setSegregateNss(segregateNss);

            partialCalcDto.setTaskSummaryList(showSummaryWithLabel(stlCalcJobExec, getCalculateStepsForSkipLogs()));

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

            // for showing generate monthly summary
            if (currentBatchStatus == BatchStatus.COMPLETED) {
                stlJobGroupDto.setHasCompletedCalc(true);
            }

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

            Date latestJobExecStartDate = stlJobGroupDto.getLatestJobExecStartDate();
            if (latestJobExecStartDate == null || !latestJobExecStartDate.after(tagJobExecution.getStartTime())) {
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

            Date latestJobExecStartDate = stlJobGroupDto.getLatestJobExecStartDate();
            if (latestJobExecStartDate == null || !latestJobExecStartDate.after(generationJobExecution.getStartTime())) {
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
    void initializeEnergyBillStatementFileGen(final List<JobInstance> fileGenJobInstances,
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
            stlJobGroupDto.setEnergyBillStatementGenerationStatus(currentStatus);
            stlJobGroupDto.setGroupId(groupId);
            stlJobGroupDto.setRunId(generationJobParameters.getLong(RUN_ID));
            stlJobGroupDto.setRunStartDateTime(generationJobExecution.getStartTime());
            stlJobGroupDto.setRunEndDateTimeFileEnergyBillStatementTa(generationJobExecution.getEndTime());

            Date latestJobExecStartDate = stlJobGroupDto.getLatestJobExecStartDate();
            if (latestJobExecStartDate == null || !latestJobExecStartDate.after(generationJobExecution.getStartTime())) {
                updateProgress(generationJobExecution, stlJobGroupDto);
            }

            stlJobGroupDtoMap.put(groupId, stlJobGroupDto);

            Optional.ofNullable(generationJobExecution.getExecutionContext()
                    .get(INVOICE_GENERATION_FILENAME_KEY)).ifPresent(val ->
                    stlJobGroupDto.setEnergyBillStatementGenFolder((String) val));

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

        SortedSet<LocalDate> remainingCalcDates = DateUtil.createRange(billPeriodStart, billPeriodEnd);

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

        SortedSet<LocalDate> remainingGenInputWsDates = DateUtil.createRange(billPeriodStart, billPeriodEnd);

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

    void determineStlJobGroupDtoStatus(final StlJobGroupDto stlJobGroupDto, final boolean isDaily, final Date billPeriodStart,
                                       final Date billPeriodEnd) {
        List<StlJobStage> excludedJobStages = Arrays.asList(CALCULATE_LR, FINALIZE_LR);
        stlJobGroupDto.getSortedJobCalculationDtos().stream().filter(dto -> !excludedJobStages.contains(dto.getJobStage()))
                .findFirst().ifPresent(jobDto -> {

            if (jobDto.getJobExecStatus().isRunning() || isDaily) {
                stlJobGroupDto.setStatus(jobDto.getStatus());
            } else {
                boolean startAndEndDateIsEqual = billPeriodStart.equals(jobDto.getStartDate()) && billPeriodEnd.equals(jobDto.getEndDate());

                // special rules for generate input ws and calculations
                switch (jobDto.getJobStage()) {
                    case GENERATE_IWS:
                        if (stlJobGroupDto.getRemainingDatesGenInputWs().isEmpty() || startAndEndDateIsEqual) {
                            stlJobGroupDto.setStatus(convertStatus(jobDto.getJobExecStatus(), FULL + GENERATE_IWS.getLabel()));
                        } else {
                            stlJobGroupDto.setStatus(convertStatus(jobDto.getJobExecStatus(), PARTIAL + GENERATE_IWS.getLabel()));
                        }
                        break;
                    case CALCULATE_STL:
                        if (stlJobGroupDto.getRemainingDatesCalc().isEmpty() || startAndEndDateIsEqual) {
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

        DateUtil.createRange(billPeriodStart, billPeriodEnd).forEach(tradingDate -> stlReadyDateMap.put(tradingDate, LocalDateTime.MIN));

        // create map of updated trading dates from stlReady per day
        stlReadyJobs.forEach(stlReadyJob -> {
            Date stlReadyStartDate = DateUtil.convertToDate(stlReadyJob.getStartDate());
            Date stlReadyEndDate = DateUtil.convertToDate(stlReadyJob.getEndDate());

            SortedSet<LocalDate> stlReadyDates = DateUtil.createRange(stlReadyStartDate, stlReadyEndDate);

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
        filteredGenInputWsDtosAsc.forEach(genInputWsDto -> DateUtil.createRange(genInputWsDto.getStartDate(), genInputWsDto.getEndDate())
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

    void determineIfJobsAreLocked(final SettlementTaskExecutionDto taskExecutionDto, final String billingPeriodStr) {

        LocalDateTime billPeriodStart = DateUtil.convertToLocalDateTime(taskExecutionDto.getBillPeriodStartDate());
        LocalDateTime billPeriodEnd = DateUtil.convertToLocalDateTime(taskExecutionDto.getBillPeriodEndDate());
        String regionGroup = taskExecutionDto.getRegionGroup();

        final MeterProcessType processType = taskExecutionDto.getProcessType();

        if (processType == PRELIM) {
            boolean prelimIsLocked = settlementJobLockRepository.billingPeriodIsFinalized(billPeriodStart, billPeriodEnd,
                    PRELIM.name(), getStlCalculationType().name(), regionGroup);

            taskExecutionDto.getParentStlJobGroupDto().setLocked(prelimIsLocked);

        } else {

            List<SettlementJobLock> stlJobLocks = settlementJobLockRepository
                    .findByStartDateAndEndDateAndStlCalculationTypeAndRegionGroupAndProcessTypeIn(billPeriodStart, billPeriodEnd,
                            getStlCalculationType(), regionGroup, Arrays.asList(FINAL, ADJUSTED));

            for (StlJobGroupDto stlJobGroupDto : taskExecutionDto.getStlJobGroupDtoMap().values()) {

                // 1: lock stlJobGroupDto if it's already finalized
                stlJobLocks.stream().filter(stlJobLock -> stlJobLock.getGroupId().equals(stlJobGroupDto.getGroupId())
                        && stlJobLock.getParentJobId().equals(taskExecutionDto.getParentId()))
                        .findFirst().ifPresent(stlLock -> stlJobGroupDto.setLocked(stlLock.isLocked()));

                Optional<SettlementJobLock> latestAdjustedStlLock = stlJobLocks.stream()
                        .filter(stlJobLock -> stlJobLock.getProcessType() == ADJUSTED && stlJobLock.isLocked() &&
                                // do not include child jobs with FINAL parent since their parentId is equal to the billingPeriod
                                !Objects.equals(stlJobLock.getParentJobId(), Long.valueOf(billingPeriodStr)))
                        .sorted(Collections.reverseOrder(Comparator.comparing(SettlementJobLock::getLockDate)))
                        .findFirst();

                // 2: for ADJUSTED parent / header stlJobGroupDto and not yet locked
                if (ADJUSTED.equals(processType) && stlJobGroupDto.isHeader() && !stlJobGroupDto.isLocked()) {
                    stlJobGroupDto.setLocked(true);

                    // 2.1: release lock if FINAL is already locked
                    stlJobLocks.stream().filter(stlJobLock -> stlJobLock.getProcessType() == FINAL && stlJobLock.isLocked())
                            .findFirst().ifPresent(finalStlLock -> stlJobGroupDto.setLocked(false));

                    // 2.2: lock if the latest finalized Adjusted Stl run's parent id is more recent than the header's parent id
                    latestAdjustedStlLock.ifPresent(stlJobLock ->
                            stlJobGroupDto.setLocked(taskExecutionDto.getParentId() < stlJobLock.getParentJobId()));
                }

                // 3: for child runs of FINAL / ADJUSTED and not yet locked  (note all child runs are of ADJUSTED type)
                if (!stlJobGroupDto.isHeader() && !stlJobGroupDto.isLocked()) {
                    stlJobGroupDto.setLocked(true);

                    List<Long> childGroupIds = taskExecutionDto.getStlJobGroupDtoMap().values().stream()
                            .filter(dto -> !dto.isHeader())
                            .map(dto -> Long.valueOf(dto.getGroupId()))
                            .collect(Collectors.toList());

                    List<Long> finalizedGroupIdsOfChildJobs = stlJobLocks.stream()
                            .filter(lock -> childGroupIds.contains(Long.valueOf(lock.getGroupId())) && lock.isLocked())
                            .map(lock -> Long.valueOf(lock.getGroupId()))
                            .collect(Collectors.toList());

                    // 3.1: since groupId of child runs are timestamp-based, lock if a more recent child run is finalized
                    stlJobGroupDto.setLocked(finalizedGroupIdsOfChildJobs.stream()
                            .anyMatch(finalizedGroupId -> finalizedGroupId > Long.valueOf(stlJobGroupDto.getGroupId())));

                    // 3.2: lock if the latest finalized Adjusted Stl run's parent id is more recent than the child's parent id
                    latestAdjustedStlLock.ifPresent(stlJobLock -> {
                        if (stlJobLock.getGroupId().startsWith(billingPeriodStr) &&
                                !Objects.equals(taskExecutionDto.getParentStlJobGroupDto().getGroupId(), stlJobLock.getGroupId())) {
                            // latest tagged ADJUSTED run is meter-triggered and is different from the child's parent
                            stlJobGroupDto.setLocked(true);
                        } else {
                            stlJobGroupDto.setLocked(taskExecutionDto.getParentId() < stlJobLock.getParentJobId());
                        }
                    });
                }

                // Set canRunAdjustment
                // get latest Settlement Job Lock and check if groupId matches and has not yet executed run adj job
                stlJobLocks.stream().filter(SettlementJobLock::isLocked)
                        .sorted(Collections.reverseOrder(Comparator.comparing(SettlementJobLock::getLockDate)))
                        .findFirst()
                        .ifPresent(stlJobLock -> {
                            boolean hasExecutedRunAdj;

                            Map<String, StlJobGroupDto> jobGroupDtoMap = taskExecutionDto.getStlJobGroupDtoMap();

                            if (stlJobGroupDto.isHeader()) {
                                // for parent check if there are any child runs
                                hasExecutedRunAdj = jobGroupDtoMap.size() > 1;
                            } else {
                                // for child check if there's another child with a greater groupId long value
                                // (since child group ids are timestamp based)
                                hasExecutedRunAdj = jobGroupDtoMap.values().stream().filter(jobGroupDto -> !jobGroupDto.isHeader())
                                        .anyMatch(jobGroupDto ->
                                                Long.valueOf(jobGroupDto.getGroupId()) > Long.valueOf(stlJobGroupDto.getGroupId()));
                            }

                            boolean canRunAdj = Objects.equals(stlJobLock.getGroupId(), stlJobGroupDto.getGroupId())
                                    && !hasExecutedRunAdj;

                            stlJobGroupDto.setCanRunAdjustment(canRunAdj);
                        });
            }
        }

    }

    void removeDateRangeFrom(final SortedSet<LocalDate> remainingDates, final Date calcStartDate,
                             final Date calcEndDate) {
        SortedSet<LocalDate> datesToRemove = DateUtil.createRange(calcStartDate, calcEndDate);

        datesToRemove.forEach(date -> {
            if (remainingDates.contains(date)) {
                remainingDates.remove(date);
            }
        });
    }

    void addDateRangeTo(final SortedSet<LocalDate> remainingDates, final Date calcStartDate,
                        final Date calcEndDate) {

        SortedSet<LocalDate> datesToAdd = DateUtil.createRange(calcStartDate, calcEndDate);

        datesToAdd.forEach(date -> {
            if (!remainingDates.contains(date)) {
                remainingDates.add(date);
            }
        });
    }

    /* findJobInstances methods end */

    /* launchJob methods start */

    List<String> initializeJobArguments(final TaskRunDto taskRunDto, final Long runId, final String groupId,
                                        final String processType) {
        List<String> arguments = Lists.newArrayList();
        arguments.add(concatKeyValue(PARENT_JOB, taskRunDto.getParentJob(), "long"));
        arguments.add(concatKeyValue(RUN_ID, String.valueOf(runId), "long"));
        arguments.add(concatKeyValue(GROUP_ID, groupId));
        arguments.add(concatKeyValue(USERNAME, taskRunDto.getCurrentUser()));
        arguments.add(concatKeyValue(PROCESS_TYPE, processType));
        // arguments.add(concatKeyValue(REGION_GROUP, taskRunDto.getRegionGroup(), "string"));
        BatchJobAddtlParams addtlParams = new BatchJobAddtlParams();
        addtlParams.setRunId(runId);
        addtlParams.setKey("regionGroup");
        addtlParams.setType(String.class.getSimpleName());
        addtlParams.setStringVal(taskRunDto.getRegionGroup());
        saveBatchJobAddtlParamsJdbc(addtlParams);
//        redisTemplate.opsForValue().set(String.format("%d-%s", runId, "REGION_GROUP"), taskRunDto.getRegionGroup());
        return arguments;
    }

    void validateFinalized(final String groupId, final MeterProcessType processType, final StlCalculationType calcType,
                           final String regionGroup) {
        LocalDateTime finalizedDate = settlementJobLockRepository.getLockDateByCalculationTypeGroupIdAndProcessTypeAndRegionGroup(
                groupId, calcType, processType, regionGroup);

        if (finalizedDate != null) {
            throw new RuntimeException("Launch job failed. Job was already FINALIZED on " + DateTimeUtil.formatDateTime(finalizedDate));
        }
    }

    void launchGenerateInputWorkspaceJob(final TaskRunDto taskRunDto) throws URISyntaxException {
        Preconditions.checkNotNull(taskRunDto.getRunId());
        final Long runId = taskRunDto.getRunId();

        // isNewGroup && StlCalculationType.TRADING_AMOUNTS groupId is already set during creation of job queue
        final String groupId = taskRunDto.isNewGroup() && Arrays.asList(StlCalculationType.ENERGY_MARKET_FEE,
                StlCalculationType.RESERVE_MARKET_FEE).contains(getStlCalculationType())
                ? runId.toString() : taskRunDto.getGroupId();

        final String type = taskRunDto.getMeterProcessType();
        MeterProcessType processType = MeterProcessType.valueOf(type);

        validateFinalized(groupId, processType, getStlCalculationType(), taskRunDto.getRegionGroup());

        List<String> arguments = initializeJobArguments(taskRunDto, runId, groupId, type);

        List<String> properties = Lists.newArrayList();

        LocalDateTime billPeriodStartDate = DateUtil.parseStringDateToLocalDateTime(taskRunDto.getBaseStartDate(),
                DateUtil.DEFAULT_DATE_FORMAT);
        LocalDateTime billPeriodEndDate = DateUtil.parseStringDateToLocalDateTime(taskRunDto.getBaseEndDate(),
                DateUtil.DEFAULT_DATE_FORMAT);

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
            saveSettlementJobLock(groupId, processType, taskRunDto, getStlCalculationType());
        }

        log.info("Running generate input workspace job name={}, properties={}, arguments={}", taskRunDto.getJobName(), properties, arguments);

        launchJob(SPRING_BATCH_MODULE_STL_CALC, properties, arguments);
        lockJobJdbc(taskRunDto);
    }

    void launchCalculateJob(final TaskRunDto taskRunDto) throws URISyntaxException {
        Preconditions.checkNotNull(taskRunDto.getRunId());
        final Long runId = taskRunDto.getRunId();
        final String groupId = taskRunDto.getGroupId();
        final String type = taskRunDto.getMeterProcessType();
        MeterProcessType processType = MeterProcessType.valueOf(type);

        validateFinalized(groupId, processType, getStlCalculationType(), taskRunDto.getRegionGroup());

        List<String> arguments = initializeJobArguments(taskRunDto, runId, groupId, type);

        List<String> properties = Lists.newArrayList();

        switch (processType) {
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
        lockJobJdbc(taskRunDto);
    }

    void launchFinalizeJob(final TaskRunDto taskRunDto) throws URISyntaxException {
        Preconditions.checkNotNull(taskRunDto.getRunId());
        final Long runId = taskRunDto.getRunId();
        final String groupId = taskRunDto.getGroupId();
        final String type = taskRunDto.getMeterProcessType();
        MeterProcessType processType = MeterProcessType.valueOf(type);

        validateFinalized(groupId, processType, getStlCalculationType(), taskRunDto.getRegionGroup());

        List<String> arguments = initializeJobArguments(taskRunDto, runId, groupId, type);
        arguments.add(concatKeyValue(START_DATE, taskRunDto.getBaseStartDate(), "date"));
        arguments.add(concatKeyValue(END_DATE, taskRunDto.getBaseEndDate(), "date"));

        List<String> properties = Lists.newArrayList();

        switch (processType) {
            case PRELIM:
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(
                        getPrelimTaggingProfile())));
                break;
            case FINAL:
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(
                        getFinalTaggingProfile())));
                saveFinalizeAMSadditionalParams(runId, taskRunDto);
                break;
            case ADJUSTED:
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(
                        getAdjustedTaggingProfile())));
                saveFinalizeAMSadditionalParams(runId, taskRunDto);
                break;
            case DAILY:
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(
                        getDailyTaggingProfile())));
                break;
            default:
                throw new RuntimeException("Failed to launch job. Unhandled processType: " + type);
        }

        log.info("Running finalize job name={}, properties={}, arguments={}", taskRunDto.getJobName(), properties, arguments);

        launchJob(SPRING_BATCH_MODULE_STL_CALC, properties, arguments);
        lockJobJdbc(taskRunDto);
    }

    void launchGenerateFileJob(final TaskRunDto taskRunDto) throws URISyntaxException {
        Preconditions.checkNotNull(taskRunDto.getRunId());
        final Long runId = taskRunDto.getRunId();
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
            case DAILY:
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(
                        getDailyTaggingProfile())));
                break;
            default:
                throw new RuntimeException("Failed to launch job. Unhandled processType: " + type);
        }

        log.info("Running generate file job name={}, properties={}, arguments={}", taskRunDto.getJobName(), properties, arguments);

        launchJob(SPRING_BATCH_MODULE_FILE_GEN, properties, arguments);
        lockJobJdbc(taskRunDto);
    }

    void launchGenerateBillStatementFileJob(final TaskRunDto taskRunDto) throws URISyntaxException {
        Preconditions.checkNotNull(taskRunDto.getRunId());
        final Long runId = taskRunDto.getRunId();
        final String groupId = taskRunDto.getGroupId();
        final String type = taskRunDto.getMeterProcessType();

        List<String> arguments = initializeJobArguments(taskRunDto, runId, groupId, type);
        arguments.add(concatKeyValue(START_DATE, taskRunDto.getBaseStartDate(), "date"));
        arguments.add(concatKeyValue(END_DATE, taskRunDto.getBaseEndDate(), "date"));

        List<String> properties = Lists.newArrayList();
        properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(getPrelimGenBillStatementProfile())));

        log.info("Running generate bill statement files job name={}, properties={}, arguments={}",
                taskRunDto.getJobName(), properties, arguments);

        launchJob(SPRING_BATCH_MODULE_FILE_GEN, properties, arguments);
        lockJobJdbc(taskRunDto);
    }

    void saveSettlementJobLock(String groupId, MeterProcessType processType, TaskRunDto taskRunDto,
                               StlCalculationType calculationType) {
        String regionGroup = taskRunDto.getRegionGroup();

        BooleanBuilder predicate = new BooleanBuilder();
        predicate.and(settlementJobLock.groupId.eq(groupId)
                .and(settlementJobLock.processType.eq(processType))
                .and(settlementJobLock.stlCalculationType.eq(calculationType)))
                .and(settlementJobLock.regionGroup.eq(regionGroup));

        if (!settlementJobLockRepository.exists(predicate)) {
            log.info("Creating new Settlement Job Lock. groupdId {}, stlCalculationType {}, processType: {}, regionGroup: {}",
                    groupId, calculationType, processType, regionGroup);

            MapSqlParameterSource paramSource = new MapSqlParameterSource()
                    .addValue("startDate", DateUtil.convertToDate(taskRunDto.getBaseStartDate(), "yyyy-MM-dd"))
                    .addValue("endDate", DateUtil.convertToDate(taskRunDto.getBaseEndDate(), "yyyy-MM-dd"))
                    .addValue("groupId", groupId)
                    .addValue("parentJobId", Long.valueOf(taskRunDto.getParentJob()))
                    .addValue("processType", processType.name())
                    .addValue("stlCalculationType", calculationType.name())
                    .addValue("regionGroup", regionGroup);

            String insertSql = "insert into settlement_job_lock(id, created_datetime, start_date, end_date, "
                    + " group_id, parent_job_id, process_type, stl_calculation_type, region_group, locked) "
                    + " values(nextval('hibernate_sequence'), now(), :startDate, :endDate, :groupId, :parentJobId, "
                    + " :processType, :stlCalculationType, :regionGroup, false)";

            dataflowJdbcTemplate.update(insertSql, paramSource);
        }
    }

    private void saveAdjRun(MeterProcessType type, String jobId, String groupId, LocalDateTime start, LocalDateTime end) {
        MapSqlParameterSource paramSource = new MapSqlParameterSource()
                .addValue("jobId", jobId)
                .addValue("groupId", groupId)
                .addValue("processType", type.name())
                .addValue("billPeriodStart", DateUtil.convertToDate(start))
                .addValue("billPeriodEnd", DateUtil.convertToDate(end));

        String insertSql = "insert into batch_job_adj_run(id, created_datetime, job_id, group_id, meter_process_type, "
                + " addtl_comp, billing_period_start, billing_period_end, output_ready) "
                + " values(nextval('hibernate_sequence'), now(), :jobId, :groupId, :processType, 'N', :billPeriodStart, "
                + " :billPeriodEnd, 'N')";

        dataflowJdbcTemplate.update(insertSql, paramSource);
    }

    void saveAMSadditionalParams(final Long runId, final TaskRunDto taskRunDto) {
        log.info("Saving additional AMS params. TaskRunDto: {}", taskRunDto);
        try {
            BatchJobAddtlParams batchJobAddtlParamsInvoiceDate = new BatchJobAddtlParams();
            batchJobAddtlParamsInvoiceDate.setRunId(runId);
            batchJobAddtlParamsInvoiceDate.setType("DATE");
            batchJobAddtlParamsInvoiceDate.setKey(AMS_INVOICE_DATE);
            batchJobAddtlParamsInvoiceDate.setDateVal(DateUtil.getStartRangeDate(taskRunDto.getAmsInvoiceDate()));
            saveBatchJobAddtlParamsJdbc(batchJobAddtlParamsInvoiceDate);

            BatchJobAddtlParams batchJobAddtlParamsDueDate = new BatchJobAddtlParams();
            batchJobAddtlParamsDueDate.setRunId(runId);
            batchJobAddtlParamsDueDate.setType("DATE");
            batchJobAddtlParamsDueDate.setKey(AMS_DUE_DATE);
            batchJobAddtlParamsDueDate.setDateVal(DateUtil.getStartRangeDate(taskRunDto.getAmsDueDate()));
            saveBatchJobAddtlParamsJdbc(batchJobAddtlParamsDueDate);

            switch (getStlCalculationType()) {
                case TRADING_AMOUNTS:
                    BatchJobAddtlParams batchJobAddtlParamsRemarksInv = new BatchJobAddtlParams();
                    batchJobAddtlParamsRemarksInv.setRunId(runId);
                    batchJobAddtlParamsRemarksInv.setType("STRING");
                    batchJobAddtlParamsRemarksInv.setKey(AMS_REMARKS_INV);
                    batchJobAddtlParamsRemarksInv.setStringVal(taskRunDto.getAmsRemarksInv());
                    saveBatchJobAddtlParamsJdbc(batchJobAddtlParamsRemarksInv);
                    break;
                case ENERGY_MARKET_FEE:
                case RESERVE_MARKET_FEE:
                    BatchJobAddtlParams batchJobAddtlParamsRemarksMf = new BatchJobAddtlParams();
                    batchJobAddtlParamsRemarksMf.setRunId(runId);
                    batchJobAddtlParamsRemarksMf.setType("STRING");
                    batchJobAddtlParamsRemarksMf.setKey(AMS_REMARKS_MF);
                    batchJobAddtlParamsRemarksMf.setStringVal(taskRunDto.getAmsRemarksMf());
                    saveBatchJobAddtlParamsJdbc(batchJobAddtlParamsRemarksMf);
                    break;
                default:
                    // do nothing
            }

        } catch (ParseException e) {
            log.error("Error parsing additional batch job params for AMS: {}", e);
        }
    }

    void saveFinalizeAMSadditionalParams(final Long runId, final TaskRunDto taskRunDto) {
        log.info("Saving additional AMS params. TaskRunDto: {}", taskRunDto);
        try {
            BatchJobAddtlParams batchJobAddtlParamsInvoiceDate = new BatchJobAddtlParams();
            batchJobAddtlParamsInvoiceDate.setRunId(runId);
            batchJobAddtlParamsInvoiceDate.setType("DATE");
            batchJobAddtlParamsInvoiceDate.setKey(AMS_INVOICE_DATE);
            batchJobAddtlParamsInvoiceDate.setDateVal(DateUtil.getStartRangeDate(taskRunDto.getAmsInvoiceDate()));
            saveBatchJobAddtlParamsJdbc(batchJobAddtlParamsInvoiceDate);

            BatchJobAddtlParams batchJobAddtlParamsDueDate = new BatchJobAddtlParams();
            batchJobAddtlParamsDueDate.setRunId(runId);
            batchJobAddtlParamsDueDate.setType("DATE");
            batchJobAddtlParamsDueDate.setKey(AMS_DUE_DATE);
            batchJobAddtlParamsDueDate.setDateVal(DateUtil.getStartRangeDate(taskRunDto.getAmsDueDate()));
            saveBatchJobAddtlParamsJdbc(batchJobAddtlParamsDueDate);

            BatchJobAddtlParams batchJobAddtlParamsRemarksInv = new BatchJobAddtlParams();
            batchJobAddtlParamsRemarksInv.setRunId(runId);
            batchJobAddtlParamsRemarksInv.setType("STRING");
            batchJobAddtlParamsRemarksInv.setKey(AMS_REMARKS_INV);
            batchJobAddtlParamsRemarksInv.setStringVal(taskRunDto.getAmsRemarksInv());
            saveBatchJobAddtlParamsJdbc(batchJobAddtlParamsRemarksInv);
        } catch (ParseException e) {
            log.error("Error parsing additional batch job params for AMS: {}", e);
        }
    }

    void saveAllocAdditionalParams(final Long runId, final TaskRunDto taskRunDto) {
        log.info("Saving additional Alloc params. TaskRunDto: {}", taskRunDto);
        try {
            BatchJobAddtlParams batchJobAddtlParamsInvoiceDate = new BatchJobAddtlParams();
            batchJobAddtlParamsInvoiceDate.setRunId(runId);
            batchJobAddtlParamsInvoiceDate.setType("DATE");
            batchJobAddtlParamsInvoiceDate.setKey(ALLOC_DATE);
            batchJobAddtlParamsInvoiceDate.setDateVal(DateUtil.getStartRangeDate(taskRunDto.getAllocDate()));
            saveBatchJobAddtlParamsJdbc(batchJobAddtlParamsInvoiceDate);

            BatchJobAddtlParams batchJobAddtlParamsDueDate = new BatchJobAddtlParams();
            batchJobAddtlParamsDueDate.setRunId(runId);
            batchJobAddtlParamsDueDate.setType("DATE");
            batchJobAddtlParamsDueDate.setKey(ALLOC_DUE_DATE);
            batchJobAddtlParamsDueDate.setDateVal(DateUtil.getStartRangeDate(taskRunDto.getAllocDueDate()));
            saveBatchJobAddtlParamsJdbc(batchJobAddtlParamsDueDate);

            BatchJobAddtlParams batchJobAddtlParamsRemarks = new BatchJobAddtlParams();
            batchJobAddtlParamsRemarks.setRunId(runId);
            batchJobAddtlParamsRemarks.setType("STRING");
            batchJobAddtlParamsRemarks.setKey(ALLOC_REMARKS);
            batchJobAddtlParamsRemarks.setStringVal(taskRunDto.getAllocRemarks());
            saveBatchJobAddtlParamsJdbc(batchJobAddtlParamsRemarks);
        } catch (ParseException e) {
            log.error("Error parsing additional batch job params for Alloc: {}", e);
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
