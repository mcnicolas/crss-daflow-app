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
import com.pemc.crss.shared.core.dataflow.entity.BatchJobAdjRun;
import com.pemc.crss.shared.core.dataflow.entity.SettlementJobLock;
import com.pemc.crss.shared.core.dataflow.reference.StlCalculationType;
import com.pemc.crss.shared.core.dataflow.repository.BatchJobAdjRunRepository;
import com.pemc.crss.shared.core.dataflow.repository.SettlementJobLockRepository;
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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
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

import static com.pemc.crss.shared.commons.reference.MeterProcessType.*;
import static com.pemc.crss.shared.core.dataflow.entity.QSettlementJobLock.settlementJobLock;

@Slf4j
public abstract class StlTaskExecutionServiceImpl extends AbstractTaskExecutionService {

    static final String SPRING_BATCH_MODULE_STL_CALC = "crss-settlement-task-calculation";
    static final String SPRING_BATCH_MODULE_FILE_GEN = "crss-settlement-task-invoice-generation";

    static final String STAGE_PARTIAL_GENERATE_INPUT_WS = "PARTIAL-GENERATE-INPUT-WORKSPACE";
    static final String STATUS_FULL_GENERATE_INPUT_WS = "FULL-GENERATE-INPUT-WORKSPACE";

    static final String STAGE_PARTIAL_CALC = "PARTIAL-CALCULATION";
    static final String STATUS_FULL_STL_CALC = "FULL-SETTLEMENT-CALCULATION";

    private static final String STAGE_TAGGING = "TAGGING";

    // from batch_job_execution_context
    private static final String INVOICE_GENERATION_FILENAME_KEY = "INVOICE_GENERATION_FILENAME";

    @Autowired
    private BatchJobAdjRunRepository batchJobAdjRunRepository;

    @Autowired
    private SettlementJobLockRepository settlementJobLockRepository;

    // Abstract Methods

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
                                                                      final String parentId) {
        List<String> processTypes = new ArrayList<>();
        processTypes.add(processType.name());

        // also add ADJUSTED processType for child job instances with same parentId as FINAL job instance
        if (processType == FINAL) {
            processTypes.add(ADJUSTED.name());
        }

        return dataFlowJdbcJobExecutionDao.findJobInstancesByNameAndProcessTypeAndParentId(jobNamePrefix, processTypes, parentId);
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

        Map<String, SortedSet<LocalDate>> remainingDatesMap = new HashMap<>();

        for (JobInstance genWsStlJobInstance : generateInputWsJobInstances) {

            JobExecution genWsJobExec = getJobExecutionFromJobInstance(genWsStlJobInstance);
            boolean isDaily = taskExecutionDto.getProcessType().equals(DAILY);

            Date billPeriodStartDate = taskExecutionDto.getBillPeriodStartDate();
            Date billPeriodEndDate = taskExecutionDto.getBillPeriodEndDate();

            BatchStatus currentBatchStatus = genWsJobExec.getStatus();
            JobParameters genInputWsJobParameters = genWsJobExec.getJobParameters();
            String groupId = genInputWsJobParameters.getString(GROUP_ID);
            Date genInputWsStartDate = genInputWsJobParameters.getDate(START_DATE);
            Date genInputWsEndDate = genInputWsJobParameters.getDate(END_DATE);

            if (!isDaily && !remainingDatesMap.containsKey(groupId)) {
                remainingDatesMap.put(groupId, createRange(billPeriodStartDate, billPeriodEndDate));
            }

            final StlJobGroupDto stlJobGroupDto = stlJobGroupDtoMap.getOrDefault(groupId, new StlJobGroupDto());
            stlJobGroupDto.setRemainingDatesMapGenIw(remainingDatesMap);

            if (currentBatchStatus.isRunning()) {
                // for validation of gmr calculation in case stl amt is recalculated
                stlJobGroupDto.setRunningGenInputWorkspace(true);
            }

            boolean fullGenInputWs = billPeriodStartDate != null && billPeriodEndDate != null
                    && genInputWsStartDate.compareTo(billPeriodStartDate) == 0 && genInputWsEndDate.compareTo(billPeriodEndDate) == 0;

            final String jobGenInputWsStatus = fullGenInputWs
                    ? convertStatus(currentBatchStatus, STATUS_FULL_GENERATE_INPUT_WS)
                    : convertStatus(currentBatchStatus, STAGE_PARTIAL_GENERATE_INPUT_WS);

            List<JobCalculationDto> jobCalculationDtoList = stlJobGroupDto.getJobCalculationDtos();

            if (jobCalculationDtoList.isEmpty()) {
                stlJobGroupDto.setRunStartDateTime(genWsJobExec.getStartTime());
                stlJobGroupDto.setRunEndDateTime(genWsJobExec.getEndTime());

                // get first stl-calc item's status
                stlJobGroupDto.setStatus(jobGenInputWsStatus);
            } else {
                String latestStatus = getLatestJobCalcStatusByStage(stlJobGroupDto, STAGE_PARTIAL_GENERATE_INPUT_WS);
                // get latest status first
                stlJobGroupDto.setStatus(latestStatus);
            }

            JobCalculationDto partialCalcDto = new JobCalculationDto(genWsJobExec.getStartTime(),
                    genWsJobExec.getEndTime(), genInputWsStartDate, genInputWsEndDate,
                    jobGenInputWsStatus, STAGE_PARTIAL_GENERATE_INPUT_WS);

            // for skiplogs use
            partialCalcDto.setTaskSummaryList(showSummary(genWsJobExec, getInputWorkSpaceStepsForSkipLogs()));

            jobCalculationDtoList.add(partialCalcDto);

            if (!isDaily && BatchStatus.COMPLETED == currentBatchStatus
                    && stlJobGroupDto.getRemainingDatesMapGenIw().containsKey(groupId)) {
                removeDateRangeFrom(stlJobGroupDto.getRemainingDatesMapGenIw().get(groupId), genInputWsStartDate, genInputWsEndDate);
            }

            stlJobGroupDto.setJobCalculationDtos(jobCalculationDtoList);
            stlJobGroupDto.setGroupId(groupId);

            Date latestJobExecStartDate = stlJobGroupDto.getLatestJobExecStartDate();
            if (latestJobExecStartDate == null || !latestJobExecStartDate.after(genWsJobExec.getStartTime())) {
                updateProgress(genWsJobExec, stlJobGroupDto);
            }

            Date maxPartialGenInputWsDate = stlJobGroupDto.getJobCalculationDtos().stream()
                    .filter(jobCalc -> jobCalc.getJobStage().equals(STAGE_PARTIAL_GENERATE_INPUT_WS))
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

        Map<String, SortedSet<LocalDate>> remainingDatesMap = new HashMap<>();

        for (JobInstance stlCalcJobInstance : stlCalculationJobInstances) {

            JobExecution stlCalcJobExec = getJobExecutionFromJobInstance(stlCalcJobInstance);
            boolean isDaily = taskExecutionDto.getProcessType().equals(DAILY);

            Date billPeriodStartDate = taskExecutionDto.getBillPeriodStartDate();
            Date billPeriodEndDate = taskExecutionDto.getBillPeriodEndDate();

            BatchStatus currentBatchStatus = stlCalcJobExec.getStatus();
            JobParameters calcJobParameters = stlCalcJobExec.getJobParameters();
            String groupId = calcJobParameters.getString(GROUP_ID);
            Date calcStartDate = calcJobParameters.getDate(START_DATE);
            Date calcEndDate = calcJobParameters.getDate(END_DATE);

            if (!isDaily && !remainingDatesMap.containsKey(groupId)) {
                remainingDatesMap.put(groupId, createRange(billPeriodStartDate, billPeriodEndDate));
            }

            final StlJobGroupDto stlJobGroupDto = stlJobGroupDtoMap.getOrDefault(groupId, new StlJobGroupDto());
            stlJobGroupDto.setRemainingDatesMapCalc(remainingDatesMap);

            if (currentBatchStatus.isRunning()) {
                // for validation of gmr calculation in case stl amt is recalculated
                stlJobGroupDto.setRunningStlCalculation(true);
            }

            boolean fullCalculation = billPeriodStartDate != null && billPeriodEndDate != null
                    && calcStartDate.compareTo(billPeriodStartDate) == 0 && calcEndDate.compareTo(billPeriodEndDate) == 0;

            final String jobCalcStatus = fullCalculation
                    ? convertStatus(currentBatchStatus, STATUS_FULL_STL_CALC)
                    : convertStatus(currentBatchStatus, STAGE_PARTIAL_CALC);

            List<JobCalculationDto> jobCalculationDtoList = stlJobGroupDto.getJobCalculationDtos();

            JobCalculationDto partialCalcDto = new JobCalculationDto(stlCalcJobExec.getStartTime(),
                    stlCalcJobExec.getEndTime(), calcStartDate, calcEndDate,
                    jobCalcStatus, STAGE_PARTIAL_CALC);

            partialCalcDto.setTaskSummaryList(showSummary(stlCalcJobExec, getCalculateStepsForSkipLogs()));

            jobCalculationDtoList.add(partialCalcDto);

            if (!isDaily && BatchStatus.COMPLETED == currentBatchStatus
                    && stlJobGroupDto.getRemainingDatesMapCalc().containsKey(groupId)) {
                removeDateRangeFrom(stlJobGroupDto.getRemainingDatesMapCalc().get(groupId), calcStartDate, calcEndDate);
            }

            stlJobGroupDto.setJobCalculationDtos(jobCalculationDtoList);
            stlJobGroupDto.setGroupId(groupId);

            // set latest status regardless if gen input ws / calculate
            stlJobGroupDto.setStatus(getLatestJobCalcStatus(stlJobGroupDto));

            Date latestJobExecStartDate = stlJobGroupDto.getLatestJobExecStartDate();
            if (latestJobExecStartDate == null || !latestJobExecStartDate.after(stlCalcJobExec.getStartTime())) {
                updateProgress(stlCalcJobExec, stlJobGroupDto);
            }

            Date maxPartialCalcDate = stlJobGroupDto.getJobCalculationDtos().stream()
                    .filter(jobCalc -> jobCalc.getJobStage().equals(STAGE_PARTIAL_CALC))
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
            stlJobGroupDto.setStatus(convertStatus(currentStatus, STAGE_TAGGING));
            stlJobGroupDto.setTaggingStatus(currentStatus);
            stlJobGroupDto.setGroupId(groupId);

            JobCalculationDto finalizeJobDto = new JobCalculationDto(tagJobExecution.getStartTime(),
                    tagJobExecution.getEndTime(), tagStartDate, tagEndDate,
                    convertStatus(currentStatus, STAGE_TAGGING), STAGE_TAGGING);

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


    private SortedSet<LocalDate> createRange(Date start, Date end) {
        if (start == null || end == null) {
            return new TreeSet<>();
        }
        SortedSet<LocalDate> localDates = new TreeSet<>();
        LocalDate currentDate = start.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
        LocalDate endDate = end.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();

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

    void determineIfJobsAreLocked(final SettlementTaskExecutionDto taskExecutionDto, final StlCalculationType stlCalculationType) {

        List<SettlementJobLock> stlJobLocks = settlementJobLockRepository.findByStartDateAndEndDateAndStlCalculationTypeAndProcessTypeIn(
                DateUtil.convertToLocalDateTime(taskExecutionDto.getBillPeriodStartDate()),
                DateUtil.convertToLocalDateTime(taskExecutionDto.getBillPeriodEndDate()),
                stlCalculationType, Arrays.asList(FINAL, ADJUSTED));

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

            // Job can run adjustment when all jobs are locked
            if (stlJobLocks.stream().allMatch(SettlementJobLock::isLocked)) {
                // get latest Settlement Job Lock check if groupId matches
                stlJobLocks.stream().sorted(Collections.reverseOrder(Comparator.comparing(SettlementJobLock::getLockDate)))
                    .findFirst().ifPresent(stlJobLock -> stlJobGroupDto
                        .setCanRunAdjustment(Objects.equals(stlJobLock.getGroupId(), stlJobGroupDto.getGroupId()))
                    );
            }
        }

    }

    String getLatestJobCalcStatusByStage(StlJobGroupDto stlJobGroupDto, String stage) {
        return stlJobGroupDto.getSortedJobCalculationDtos().stream()
                .filter(stlJob -> stlJob.getJobStage().equals(stage))
                .map(JobCalculationDto::getStatus).findFirst().get();
    }

    String getLatestJobCalcStatus(StlJobGroupDto stlJobGroupDto) {
        return !stlJobGroupDto.getSortedJobCalculationDtos().isEmpty()
                ? stlJobGroupDto.getSortedJobCalculationDtos().get(0).getStatus()
                : null;
    }

    private void removeDateRangeFrom(SortedSet<LocalDate> remainingDates, Date calcStartDate, Date calcEndDate) {
        LocalDate startDate = calcStartDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
        LocalDate endDate = calcEndDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();

        LocalDate ctrDate = startDate;
        while (ctrDate.isBefore(endDate) || ctrDate.isEqual(endDate)) {
            remainingDates.remove(ctrDate);
            ctrDate = ctrDate.plusDays(1);
        }
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

    void launchGenerateInputWorkspaceJob(final TaskRunDto taskRunDto, final StlCalculationType calculationType) throws URISyntaxException {
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

                if (batchJobAdjRunRepository.countByGroupIdAndBillingPeriodStartAndBillingPeriodEnd(groupId, billPeriodStartDate, billPeriodEndDate) < 1) {
                    log.info("Saving to batchjobadjrun with groupId=[{}] and billingPeriodStart=[{}] and billingPeriodEnd=[{}]",
                            groupId, billPeriodStartDate, billPeriodEndDate);
                    saveAdjRun(FINAL, taskRunDto.getParentJob(), groupId, billPeriodStartDate, billPeriodEndDate);
                }
                break;
            case ADJUSTED:
                boolean finalBased = MeterProcessType.valueOf(taskRunDto.getBaseType()).equals(FINAL);

                if (batchJobAdjRunRepository.countByGroupIdAndBillingPeriodStartAndBillingPeriodEnd(groupId,
                        billPeriodStartDate, billPeriodEndDate) < 1) {
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
                     .and(settlementJobLock.stlCalculationType.eq(calculationType)));

            if (!settlementJobLockRepository.exists(predicate)) {
                log.info("Creating new Settlement Job Lock. groupdId {}, stlCalculationType {}, processType: {}",
                        groupId, calculationType, processType);
                SettlementJobLock jobLock = new SettlementJobLock();
                jobLock.setCreatedDatetime(LocalDateTime.now());
                jobLock.setStartDate(DateUtil.parseLocalDate(taskRunDto.getBaseStartDate(), "yyyy-MM-dd").atStartOfDay());
                jobLock.setEndDate(DateUtil.parseLocalDate(taskRunDto.getBaseEndDate(), "yyyy-MM-dd").atStartOfDay());
                jobLock.setGroupId(groupId);
                jobLock.setParentJobId(Long.valueOf(taskRunDto.getParentJob()));
                jobLock.setStlCalculationType(calculationType);
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
                break;
            case ADJUSTED:
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(
                        getAdjustedGenFileProfile())));
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
