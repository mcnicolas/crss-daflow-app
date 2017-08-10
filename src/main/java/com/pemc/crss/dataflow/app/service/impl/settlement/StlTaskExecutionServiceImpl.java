package com.pemc.crss.dataflow.app.service.impl.settlement;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.pemc.crss.dataflow.app.dto.BaseTaskExecutionDto;
import com.pemc.crss.dataflow.app.dto.JobCalculationDto;
import com.pemc.crss.dataflow.app.dto.SettlementTaskExecutionDto;
import com.pemc.crss.dataflow.app.dto.StlJobGroupDto;
import com.pemc.crss.dataflow.app.dto.TaskRunDto;
import com.pemc.crss.dataflow.app.service.impl.AbstractTaskExecutionService;
import com.pemc.crss.dataflow.app.support.PageableRequest;
import com.pemc.crss.shared.commons.reference.MeterProcessType;
import com.pemc.crss.shared.commons.reference.SettlementStepUtil;
import com.pemc.crss.shared.commons.util.DateUtil;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobAdjRun;
import com.pemc.crss.shared.core.dataflow.repository.BatchJobAdjRunRepository;
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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

@Slf4j
public abstract class StlTaskExecutionServiceImpl extends AbstractTaskExecutionService {

    private static final String STAGE_PARTIAL_GENERATE_INPUT_WS = "PARTIAL-GENERATE-INPUT-WORKSPACE";
    private static final String STATUS_FULL_GENERATE_INPUT_WS = "FULL-GENERATE-INPUT-WORKSPACE";

    private static final String STAGE_PARTIAL_CALC = "PARTIAL-CALCULATION";
    private static final String STATUS_FULL_STL_CALC = "FULL-SETTLEMENT-CALCULATION";

    @Autowired
    private BatchJobAdjRunRepository batchJobAdjRunRepository;

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

    /* findJobInstances methods start */
    List<JobInstance> findStlReadyJobInstances(final PageableRequest pageableRequest) {
        final Pageable pageable = pageableRequest.getPageable();

        return dataFlowJdbcJobExecutionDao.findStlJobInstances(pageable.getOffset(), pageable.getPageSize(), pageableRequest);
    }

    List<JobInstance> findStlJobInstancesForMarketFee(final PageableRequest pageableRequest) {
        final Pageable pageable = pageableRequest.getPageable();

        return dataFlowJdbcJobExecutionDao.findMonthlyStlReadyJobInstances(pageable.getOffset(), pageable.getPageSize(),
                pageableRequest);
    }

    List<JobExecution> getCompletedJobExecutions(final JobInstance jobInstance) {
        return getJobExecutions(jobInstance).stream().filter(jobExecution -> jobExecution.getStatus() == BatchStatus.COMPLETED)
                .collect(Collectors.toList());
    }

    SettlementTaskExecutionDto initializeTaskExecutionDto(final JobExecution jobExecution, final String parentId) {
        JobParameters jobParameters = jobExecution.getJobParameters();

        SettlementTaskExecutionDto taskExecutionDto = new SettlementTaskExecutionDto();
        taskExecutionDto.setParentId(Long.parseLong(parentId));
        taskExecutionDto.setStlReadyJobId(jobExecution.getJobId());
        taskExecutionDto.setRunDateTime(jobExecution.getStartTime());
        taskExecutionDto.setStatus(convertStatus(jobExecution.getStatus(), "SETTLEMENT"));
        taskExecutionDto.setStlReadyStatus(jobExecution.getStatus());
        taskExecutionDto.setBillPeriodStartDate(jobParameters.getDate(START_DATE));
        taskExecutionDto.setBillPeriodEndDate(jobParameters.getDate(END_DATE));
        taskExecutionDto.setDailyDate(jobParameters.getDate(DATE));

        String processType = jobParameters.getString(PROCESS_TYPE) != null ? jobParameters.getString(PROCESS_TYPE) : "DAILY";

        taskExecutionDto.setProcessType(processType);

        return taskExecutionDto;
    }

    List<JobInstance> findJobInstancesByJobNameAndParentId(final String jobName, final String parentId) {
        String calcQueryString = jobName.concat("*-").concat(parentId).concat("-*");
        return jobExplorer.findJobInstancesByJobName(calcQueryString, 0, Integer.MAX_VALUE);
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
                                     final Map<Long, StlJobGroupDto> stlJobGroupDtoMap,
                                     final SettlementTaskExecutionDto taskExecutionDto,
                                     final Long stlReadyJobId) {

        Map<Long, SortedSet<LocalDate>> remainingDatesMap = new HashMap<>();

        for (JobInstance genWsStlJobInstance : generateInputWsJobInstances) {

            JobExecution genWsJobExec = getJobExecutionFromJobInstance(genWsStlJobInstance);
            boolean isDaily = taskExecutionDto.getProcessType().equals("DAILY");

            Date billPeriodStartDate = taskExecutionDto.getBillPeriodStartDate();
            Date billPeriodEndDate = taskExecutionDto.getBillPeriodEndDate();

            BatchStatus currentBatchStatus = genWsJobExec.getStatus();
            JobParameters genInputWsJobParameters = genWsJobExec.getJobParameters();
            Long groupId = genInputWsJobParameters.getLong(GROUP_ID);
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

                // if there are no remaining dates for generate input workspace, set status to FULL even if the latest gen input ws run is PARTIAL
                Optional.ofNullable(stlJobGroupDto.getRemainingDatesMapGenIw().get(groupId)).ifPresent(remainingDates -> {
                    if (remainingDates.size() == 0) {
                        // set latest job execution status
                        BatchStatus latestJobExecutionStatus = BatchStatus.valueOf(latestStatus.split("-")[0]);
                        stlJobGroupDto.setStatus(convertStatus(latestJobExecutionStatus, STATUS_FULL_GENERATE_INPUT_WS));
                    }
                });
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

            if (stlReadyJobId.equals(groupId)) {
                stlJobGroupDto.setHeader(true);
                taskExecutionDto.setParentStlJobGroupDto(stlJobGroupDto);
            }

        }
    }

    void initializeStlCalculation(final List<JobInstance> stlCalculationJobInstances,
                                  final Map<Long, StlJobGroupDto> stlJobGroupDtoMap,
                                  final SettlementTaskExecutionDto taskExecutionDto,
                                  final Long stlReadyJobId) {

        Map<Long, SortedSet<LocalDate>> remainingDatesMap = new HashMap<>();

        for (JobInstance stlCalcJobInstance : stlCalculationJobInstances) {

            JobExecution stlCalcJobExec = getJobExecutionFromJobInstance(stlCalcJobInstance);
            boolean isDaily = taskExecutionDto.getProcessType().equals("DAILY");

            Date billPeriodStartDate = taskExecutionDto.getBillPeriodStartDate();
            Date billPeriodEndDate = taskExecutionDto.getBillPeriodEndDate();

            BatchStatus currentBatchStatus = stlCalcJobExec.getStatus();
            JobParameters calcJobParameters = stlCalcJobExec.getJobParameters();
            Long groupId = calcJobParameters.getLong(GROUP_ID);
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

            if (jobCalculationDtoList.isEmpty()) {
                stlJobGroupDto.setRunStartDateTime(stlCalcJobExec.getStartTime());
                stlJobGroupDto.setRunEndDateTime(stlCalcJobExec.getEndTime());

                // get first stl-calc item's status
                stlJobGroupDto.setStatus(jobCalcStatus);
            } else {
                String latestStatus = getLatestJobCalcStatusByStage(stlJobGroupDto, STAGE_PARTIAL_CALC);
                // get latest status first
                stlJobGroupDto.setStatus(latestStatus);

                // if there are no remaining dates for calculation, set status to FULL even if the latest calc run is PARTIAL
                Optional.ofNullable(stlJobGroupDto.getRemainingDatesMapGenIw().get(groupId)).ifPresent(remainingDates -> {
                    if (remainingDates.size() == 0) {
                        // set latest job execution status
                        BatchStatus latestJobExecutionStatus = BatchStatus.valueOf(latestStatus.split("-")[0]);
                        stlJobGroupDto.setStatus(convertStatus(latestJobExecutionStatus, STATUS_FULL_STL_CALC));
                    }
                });
            }

            JobCalculationDto partialCalcDto = new JobCalculationDto(stlCalcJobExec.getStartTime(),
                    stlCalcJobExec.getEndTime(), calcStartDate, calcEndDate,
                    jobCalcStatus, STAGE_PARTIAL_CALC);

            // TODO: deterime steps used in calculation for skip logs
            // partialCalcDto.setTaskSummaryList(showSummary(stlCalcJobExec, getInputWorkSpaceStepsForSkipLogs()));

            jobCalculationDtoList.add(partialCalcDto);

            if (!isDaily && BatchStatus.COMPLETED == currentBatchStatus
                    && stlJobGroupDto.getRemainingDatesMapGenIw().containsKey(groupId)) {
                removeDateRangeFrom(stlJobGroupDto.getRemainingDatesMapGenIw().get(groupId), calcStartDate, calcEndDate);
            }

            stlJobGroupDto.setJobCalculationDtos(jobCalculationDtoList);
            stlJobGroupDto.setGroupId(groupId);

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

            if (stlReadyJobId.equals(groupId)) {
                stlJobGroupDto.setHeader(true);
                taskExecutionDto.setParentStlJobGroupDto(stlJobGroupDto);
            }

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

    private void updateProgress(JobExecution jobExecution, StlJobGroupDto dto) {
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

    private String getLatestJobCalcStatusByStage(StlJobGroupDto stlJobGroupDto, String stage) {
        return stlJobGroupDto.getSortedJobCalculationDtos().stream()
                .filter(stlJob -> stlJob.getJobStage().equals(stage))
                .map(JobCalculationDto::getStatus).findFirst().get();
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
        Preconditions.checkNotNull(jobName);
        Preconditions.checkState(batchJobRunLockRepository.countByJobNameAndLockedIsTrue(jobName) == 0,
                "There is an existing ".concat(jobName).concat(" job running"));
    }

    private List<String> initializeJobArguments(final TaskRunDto taskRunDto, final Long runId, final Long groupId) {
        List<String> arguments = Lists.newArrayList();
        arguments.add(concatKeyValue(PARENT_JOB, taskRunDto.getParentJob(), "long"));
        arguments.add(concatKeyValue(RUN_ID, String.valueOf(runId), "long"));
        arguments.add(concatKeyValue(GROUP_ID, groupId.toString(), "long"));
        arguments.add(concatKeyValue(USERNAME, taskRunDto.getCurrentUser()));

        return arguments;
    }

    void launchGenerateInputWorkspaceJob(final TaskRunDto taskRunDto) throws URISyntaxException {
        final Long runId = System.currentTimeMillis();
        final Long groupId = taskRunDto.isNewGroup() ? runId : Long.parseLong(taskRunDto.getGroupId());
        final String type = taskRunDto.getMeterProcessType();

        List<String> arguments = initializeJobArguments(taskRunDto, runId, groupId);
        arguments.add(concatKeyValue(PROCESS_TYPE, type));

        List<String> properties = Lists.newArrayList();

        LocalDateTime billPeriodStartDate = DateUtil.parseStringDateToLocalDateTime(taskRunDto.getBaseStartDate(),
                DateUtil.DEFAULT_DATE_FORMAT);
        LocalDateTime billPeriodEndDate = DateUtil.parseStringDateToLocalDateTime(taskRunDto.getBaseEndDate(),
                DateUtil.DEFAULT_DATE_FORMAT);

        switch (type) {
            case "DAILY":
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(getDailyGenInputWorkspaceProfile())));
                arguments.add(concatKeyValue(START_DATE, taskRunDto.getTradingDate(), "date"));
                break;
            case "PRELIM":
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(getPrelimGenInputWorkspaceProfile())));
                arguments.add(concatKeyValue(START_DATE, taskRunDto.getStartDate(), "date"));
                arguments.add(concatKeyValue(END_DATE, taskRunDto.getEndDate(), "date"));
                break;
            case "FINAL":
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(getFinalGenInputWorkspaceProfile())));
                arguments.add(concatKeyValue(START_DATE, taskRunDto.getStartDate(), "date"));
                arguments.add(concatKeyValue(END_DATE, taskRunDto.getEndDate(), "date"));

                if (batchJobAdjRunRepository.countByGroupIdAndBillingPeriodStartAndBillingPeriodEnd(groupId.toString(), billPeriodStartDate, billPeriodEndDate) < 1) {
                    log.info("Saving to batchjobadjrun with groupId=[{}] and billingPeriodStart=[{}] and billingPeriodEnd=[{}]",
                            groupId.toString(), billPeriodStartDate, billPeriodEndDate);
                    saveAdjRun(MeterProcessType.FINAL, taskRunDto.getParentJob(), groupId, billPeriodStartDate, billPeriodEndDate);
                }
                break;
            case "ADJUSTED":
                boolean finalBased = "FINAL".equals(taskRunDto.getBaseType());

                if (batchJobAdjRunRepository.countByGroupIdAndBillingPeriodStartAndBillingPeriodEnd(groupId.toString(),
                        billPeriodStartDate, billPeriodEndDate) < 1) {
                    log.info("Saving to batchjobadjrun with groupId=[{}] and billingPeriodStart=[{}] and billingPeriodEnd=[{}]",
                            groupId.toString(), billPeriodStartDate, billPeriodEndDate);
                    saveAdjRun(MeterProcessType.ADJUSTED, taskRunDto.getParentJob(), groupId, billPeriodStartDate, billPeriodEndDate);
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

        log.info("Running generate input workspace job name={}, properties={}, arguments={}", taskRunDto.getJobName(), properties, arguments);

        launchJob("crss-settlement-task-calculation", properties, arguments);
        lockJob(taskRunDto);
    }

    void launchCalculateJob(final TaskRunDto taskRunDto) throws URISyntaxException {
        final Long runId = System.currentTimeMillis();
        final Long groupId = taskRunDto.isNewGroup() ? runId : Long.parseLong(taskRunDto.getGroupId());
        final String type = taskRunDto.getMeterProcessType();

        List<String> arguments = initializeJobArguments(taskRunDto, runId, groupId);
        arguments.add(concatKeyValue(PROCESS_TYPE, type));

        List<String> properties = Lists.newArrayList();

        switch (type) {
            case "DAILY":
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(getDailyCalculateProfile())));
                arguments.add(concatKeyValue(START_DATE, taskRunDto.getTradingDate(), "date"));
                break;
            case "PRELIM":
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(getPrelimCalculateProfile())));
                arguments.add(concatKeyValue(START_DATE, taskRunDto.getStartDate(), "date"));
                arguments.add(concatKeyValue(END_DATE, taskRunDto.getEndDate(), "date"));
                break;
            case "FINAL":
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(getFinalCalculateProfile())));
                arguments.add(concatKeyValue(START_DATE, taskRunDto.getStartDate(), "date"));
                arguments.add(concatKeyValue(END_DATE, taskRunDto.getEndDate(), "date"));
                break;
            case "ADJUSTED":
                boolean finalBased = "FINAL".equals(taskRunDto.getBaseType());

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

        launchJob("crss-settlement-task-calculation", properties, arguments);
        lockJob(taskRunDto);
    }

    private void saveAdjRun(MeterProcessType type, String jobId, Long groupId, LocalDateTime start, LocalDateTime end) {
        BatchJobAdjRun batchJobAdjRun = new BatchJobAdjRun();
        batchJobAdjRun.setJobId(jobId);
        batchJobAdjRun.setAdditionalCompensation(false);
        batchJobAdjRun.setGroupId(String.valueOf(groupId));
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
