package com.pemc.crss.dataflow.app.service.impl;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.pemc.crss.dataflow.app.dto.*;
import com.pemc.crss.dataflow.app.support.PageableRequest;
import com.pemc.crss.shared.commons.reference.MeterProcessType;
import com.pemc.crss.shared.commons.reference.SettlementStepUtil;
import com.pemc.crss.shared.commons.util.DateUtil;
import com.pemc.crss.shared.commons.util.TaskUtil;
import com.pemc.crss.shared.core.dataflow.entity.*;
import com.pemc.crss.shared.core.dataflow.repository.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.batch.core.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.net.URISyntaxException;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

import static java.util.stream.Collectors.toList;

@Slf4j
@Service("settlementTaskExecutionService")
@Transactional(readOnly = true, value = "transactionManager")
public class SettlementTaskExecutionServiceImpl extends AbstractTaskExecutionService {

    // Job names
    private static final String COMPUTE_STL_JOB_NAME = "computeSettlementSTL_AMT";
    private static final String COMPUTE_GMRVAT_MFEE_JOB_NAME = "computeSettlementGMR_MFEE";
    private static final String FINALIZE_JOB_NAME = "tasAsOutputReady";
    private static final String GENERATE_INVOICE_STL_JOB_NAME = "generateInvoiceSettlement";
    private static final String STL_VALIDATION_JOB_NAME = "stlValidation";

    private static final String AMS_INVOICE_DATE = "amsInvoiceDate";
    private static final String AMS_DUE_DATE = "amsDueDate";
    private static final String AMS_REMARKS_INV = "amsRemarksInv";
    private static final String AMS_REMARKS_MF = "amsRemarksMf";

    // from batch_job_execution_context
    private static final String INVOICE_GENERATION_FILENAME = "INVOICE_GENERATION_FILENAME";

    private static final String GROUP_ID = "groupId";

    private static final String STAGE_PARTIAL_CALC = "PARTIAL-CALCULATION";
    private static final String STAGE_GMR_CALC = "CALCULATION-GMR";
    private static final String STAGE_TAGGING = "TAGGING";
    private static final String STATUS_FULL_STL_CALC = "FULL-SETTLEMENT-CALCULATION";

    private static final List<String> STL_CALC_STEP_WITH_SKIP_LOGS = Arrays.asList(SettlementStepUtil.RETRIEVE_DATA_STEP,
            SettlementStepUtil.RETRIEVE_BCQ_STEP, SettlementStepUtil.CALC_SCALING_FACTOR);

    private static final List<String> STL_GMR_CALC_STEP_WITH_SKIP_LOGS = Arrays.asList(SettlementStepUtil.CALC_MARKET_FEE,
            SettlementStepUtil.CALC_RESERVE_MARKET_FEE, SettlementStepUtil.CALC_GMR_VAT);

    @Autowired
    private BatchJobAddtlParamsRepository batchJobAddtlParamsRepository;

    @Autowired
    private BatchJobAdjRunRepository batchJobAdjRunRepository;

    @Autowired
    private BatchJobAdjVatRunRepository batchJobAdjVatRunRepository;

    @Autowired
    private RunningAdjustmentLockRepository runningAdjustmentLockRepository;

    @Autowired
    private LatestAdjustmentLockRepository latestAdjustmentLockRepository;

    @Override
    public Page<StlTaskExecutionDto> findJobInstances(final PageableRequest pageableRequest) {
        final Long totalSize = dataFlowJdbcJobExecutionDao.countStlJobInstances(pageableRequest);

        final Pageable pageable = pageableRequest.getPageable();
        List<StlTaskExecutionDto> taskExecutionDtos = dataFlowJdbcJobExecutionDao.findStlJobInstances(
                pageable.getOffset(), pageable.getPageSize(), pageableRequest).stream()
                .map((JobInstance jobInstance) -> {

                    if (getJobExecutions(jobInstance).iterator().hasNext()) {
                        JobExecution jobExecution = getJobExecutions(jobInstance).iterator().next();
                        Long jobId = jobExecution.getJobId();
                        BatchStatus jobStatus = jobExecution.getStatus();
                        if (BatchStatus.COMPLETED != jobStatus) {
                            log.info("Job processStlReady with id {} removed with status: {} ", jobId, jobStatus.name());
                            return null;
                        }
                        log.info("Processing processStlReady jobId {}", jobId);

                        String parentId = jobInstance.getJobName().split("-")[1];
                        if (StringUtils.isEmpty(parentId)) {
                            log.warn("Parent id not appended for job instance id {}. Setting parent as self..", jobInstance.getId());
                            parentId = String.valueOf(jobInstance.getInstanceId());
                        }

                        JobParameters jobParameters = jobExecution.getJobParameters();
                        Date startDate = jobParameters.getDate(START_DATE);
                        Date endDate = jobParameters.getDate(END_DATE);
                        Date date = jobParameters.getDate(DATE);
                        boolean isDaily = jobParameters.getString(PROCESS_TYPE) == null;
                        log.info("Date Range -> from {} to {} | Date -> {}", startDate, endDate, date);

//                        final String billingPeriodStr = isDaily ? "" : resolveBillingPeriod(Long.valueOf(parentId));

                        StlTaskExecutionDto taskExecutionDto = new StlTaskExecutionDto();
                        taskExecutionDto.setId(Long.parseLong(parentId));
                        taskExecutionDto.setRunDateTime(jobExecution.getStartTime());
                        taskExecutionDto.setParams(Maps.transformValues(
                                jobParameters.getParameters(), JobParameter::getValue));
                        taskExecutionDto.setStatus(convertStatus(jobStatus, "SETTLEMENT"));
                        taskExecutionDto.setStlReadyStatus(jobStatus);

                        Long latestGroupId = null;
                        Long lockedGroupId = null;
                        if (startDate != null && endDate != null) {
                            Iterator<RunningAdjustmentLock> iteratorRunning = runningAdjustmentLockRepository
                                    .findByStartDateAndEndDateAndLockedIsTrue(startDate, endDate).iterator();
                            lockedGroupId = iteratorRunning.hasNext() ? iteratorRunning.next().getGroupId() : null;

                            Iterator<LatestAdjustmentLock> iteratorLatest = latestAdjustmentLockRepository
                                    .findByStartDateAndEndDateAndLockedIsTrue(startDate, endDate).iterator();
                            latestGroupId = iteratorLatest.hasNext() ? iteratorLatest.next().getGroupId() : null;
                        }

                        taskExecutionDto.setCurrentlyRunningId(lockedGroupId);
                        taskExecutionDto.setLatestAdjustmentId(latestGroupId);

                        StlJobGroupDto parentStlJobGroupDto = new StlJobGroupDto();
                        parentStlJobGroupDto.setGroupId(jobId);
                        parentStlJobGroupDto.setCurrentlyRunning(jobId.equals(lockedGroupId));
                        parentStlJobGroupDto.setLatestAdjustment(jobId.equals(latestGroupId));
//                        parentStlJobGroupDto.setBillingPeriodStr(billingPeriodStr);
                        Map<Long, StlJobGroupDto> stlJobGroupDtoMap = new HashMap<>();

                    /* CALCULATION START */
                        String calcQueryString = COMPUTE_STL_JOB_NAME.concat("*-").concat(parentId).concat("-*");
                        List<JobInstance> calcStlJobInstances = jobExplorer.findJobInstancesByJobName(calcQueryString, 0, Integer.MAX_VALUE);

                        Map<Long, SortedSet<LocalDate>> remainingDatesMap = new HashMap<>();

                        for (JobInstance calcStlJobInstance : calcStlJobInstances) {

                            Iterator<JobExecution> calcStlExecIterator = getJobExecutions(calcStlJobInstance).iterator();
                            if (calcStlExecIterator.hasNext()) {
                                JobExecution calcJobExecution = calcStlExecIterator.next();
                                BatchStatus currentStatus = calcJobExecution.getStatus();
                                JobParameters calcJobParameters = calcJobExecution.getJobParameters();
                                Long groupId = calcJobParameters.getLong(GROUP_ID);
                                Date calcStartDate = calcJobParameters.getDate(START_DATE);
                                Date calcEndDate = calcJobParameters.getDate(END_DATE);

                                if (!isDaily && !remainingDatesMap.containsKey(groupId)) {
                                    remainingDatesMap.put(groupId, createRange(startDate, endDate));
                                }

                                final StlJobGroupDto stlJobGroupDto = stlJobGroupDtoMap.getOrDefault(groupId, new StlJobGroupDto());
                                stlJobGroupDto.setCurrentlyRunning(groupId.equals(lockedGroupId));
                                stlJobGroupDto.setLatestAdjustment(groupId.equals(latestGroupId));
                                stlJobGroupDto.setHeader(jobId.equals(groupId));
                                stlJobGroupDto.setRemainingDatesMap(remainingDatesMap);
//                                stlJobGroupDto.setBillingPeriodStr(billingPeriodStr);

                                if (currentStatus.isRunning()) {
                                    // for validation of gmr calculation in case stl amt is recalculated
                                    stlJobGroupDto.setStlCalculation(true);
                                }


                                boolean fullCalculation = startDate != null && endDate != null
                                        && calcStartDate.compareTo(startDate) == 0 && calcEndDate.compareTo(endDate) == 0;

                                final String jobCalcStatus = fullCalculation
                                        ? convertStatus(currentStatus, STATUS_FULL_STL_CALC)
                                        : convertStatus(currentStatus, STAGE_PARTIAL_CALC);

                                List<JobCalculationDto> jobCalculationDtoList = stlJobGroupDto.getJobCalculationDtos();
                                if (jobCalculationDtoList.isEmpty()) {
                                    stlJobGroupDto.setRunStartDateTime(calcJobExecution.getStartTime());
                                    stlJobGroupDto.setRunEndDateTime(calcJobExecution.getEndTime());

                                    // get first stl-calc item's status
                                    stlJobGroupDto.setStatus(jobCalcStatus);
                                } else {
                                    // get latest status first
                                    stlJobGroupDto.setStatus(getLatestJobCalcStatusByStage(stlJobGroupDto, STAGE_PARTIAL_CALC));

                                    // if there are no remaining dates for calculation, set status to FULL even if the latest calc run is PARTIAL
                                    Optional.ofNullable(stlJobGroupDto.getRemainingDatesMap().get(groupId)).ifPresent(remainingDates -> {
                                        if (remainingDates.size() == 0) {
                                            stlJobGroupDto.setStatus(convertStatus(BatchStatus.COMPLETED, STATUS_FULL_STL_CALC));
                                        }
                                    });
                                }

                                JobCalculationDto partialCalcDto = new JobCalculationDto(calcJobExecution.getStartTime(),
                                        calcJobExecution.getEndTime(), calcStartDate, calcEndDate,
                                        jobCalcStatus, STAGE_PARTIAL_CALC);
                                partialCalcDto.setTaskSummaryList(showSummary(calcJobExecution, STL_CALC_STEP_WITH_SKIP_LOGS));
                                jobCalculationDtoList.add(partialCalcDto);

                                if (!isDaily && BatchStatus.COMPLETED == currentStatus
                                        && stlJobGroupDto.getRemainingDatesMap().containsKey(groupId)) {
                                    removeDateRangeFrom(stlJobGroupDto.getRemainingDatesMap().get(groupId), calcStartDate, calcEndDate);
                                }

                                stlJobGroupDto.setJobCalculationDtos(jobCalculationDtoList);
                                stlJobGroupDto.setGroupId(groupId);

                                Date latestJobExecStartDate = stlJobGroupDto.getLatestJobExecStartDate();
                                if (latestJobExecStartDate == null || !latestJobExecStartDate.after(calcJobExecution.getStartTime())) {
                                    updateProgress(calcJobExecution, stlJobGroupDto);
                                }

                                Date maxPartialCalcDate = stlJobGroupDto.getJobCalculationDtos().stream()
                                        .filter(jobCalc -> jobCalc.getJobStage().equals(STAGE_PARTIAL_CALC))
                                        .map(JobCalculationDto::getRunDate)
                                        .max(Date::compareTo).get();

                                stlJobGroupDto.setMaxPartialCalcRunDate(maxPartialCalcDate);

                                stlJobGroupDtoMap.put(groupId, stlJobGroupDto);

                                if (stlJobGroupDto.isHeader()) {
                                    parentStlJobGroupDto = stlJobGroupDto;
                                }
                            }
                        }
                    /* CALCULATION END */

                    /* CALCULATION GMR START */
                        String calcGmrQueryString = COMPUTE_GMRVAT_MFEE_JOB_NAME.concat("*-").concat(parentId).concat("-*");
                        List<JobInstance> calcGmrStlJobInstances = jobExplorer.findJobInstancesByJobName(calcGmrQueryString, 0, Integer.MAX_VALUE);
                        Iterator<JobInstance> calcGmrStlIterator = calcGmrStlJobInstances.iterator();
                        Set<String> calcGmrNames = Sets.newHashSet();

                        Map<String, List<JobCalculationDto>> gmrJobCalculationDtoMap = getCalcGmrJobCalculationMap(calcGmrStlJobInstances);

                        while (calcGmrStlIterator.hasNext()) {
                            JobInstance calcGmrStlJobInstance = calcGmrStlIterator.next();
                            String calcGmrStlJobName = calcGmrStlJobInstance.getJobName();
                            if (calcGmrNames.contains(calcGmrStlJobName)) {
                                continue;
                            }
                            Iterator<JobExecution> calcGmrStlExecIterator = getJobExecutions(calcGmrStlJobInstance).iterator();
                            if (calcGmrStlExecIterator.hasNext()) {
                                JobExecution calcGmrJobExecution = calcGmrStlExecIterator.next();
                                JobParameters calcGmrJobParameters = calcGmrJobExecution.getJobParameters();
                                Long groupId = calcGmrJobParameters.getLong(GROUP_ID);
                                StlJobGroupDto stlJobGroupDto = stlJobGroupDtoMap.getOrDefault(groupId, new StlJobGroupDto());
                                BatchStatus currentStatus = calcGmrJobExecution.getStatus();

                                if (currentStatus.isRunning()) {
                                    stlJobGroupDto.setStlCalculation(false);
                                }

                                if (!stlJobGroupDto.isStlCalculation()) {
                                    stlJobGroupDto.setCurrentlyRunning(groupId.equals(lockedGroupId));
                                    stlJobGroupDto.setLatestAdjustment(groupId.equals(latestGroupId));
                                    stlJobGroupDto.setHeader(jobId.equals(groupId));

                                    stlJobGroupDto.setStatus(convertStatus(currentStatus, STAGE_GMR_CALC));
                                    stlJobGroupDto.setGmrVatMFeeCalculationStatus(currentStatus);
                                    stlJobGroupDto.setGroupId(groupId);
//                                    stlJobGroupDto.setBillingPeriodStr(billingPeriodStr);
                                    stlJobGroupDto.setGmrCalcRunDate(calcGmrJobExecution.getStartTime());

                                    Optional.ofNullable(gmrJobCalculationDtoMap.get(calcGmrStlJobName)).ifPresent(
                                        jobCalcDtoList -> stlJobGroupDto.getJobCalculationDtos().addAll(jobCalcDtoList)
                                    );

                                    // change status to COMPLETED - FULL-CALCULATION if for GMR Recalculation
                                    if (stlJobGroupDto.isForGmrRecalculation()) {
                                        stlJobGroupDto.setStatus(convertStatus(BatchStatus.COMPLETED, STATUS_FULL_STL_CALC));
                                    }

                                    if (!stlJobGroupDto.getLatestJobExecStartDate().after(calcGmrJobExecution.getStartTime())) {
                                        updateProgress(calcGmrJobExecution, stlJobGroupDto);
                                    }

                                    stlJobGroupDtoMap.put(groupId, stlJobGroupDto);
                                    if (stlJobGroupDto.isHeader()) {
                                        parentStlJobGroupDto = stlJobGroupDto;
                                    }
                                }
                            }
                            calcGmrNames.add(calcGmrStlJobName);
                        }
                    /* CALCULATION GMR END */

                    /* TAGGING START */
                        String tagQueryString = FINALIZE_JOB_NAME.concat("*-").concat(parentId).concat("-*");
                        List<JobInstance> tagStlJobInstances = jobExplorer.findJobInstancesByJobName(tagQueryString, 0, Integer.MAX_VALUE);
                        Iterator<JobInstance> tagStlIterator = tagStlJobInstances.iterator();
                        Set<String> tagNames = Sets.newHashSet();
                        while (tagStlIterator.hasNext()) {
                            JobInstance tagStlJobInstance = tagStlIterator.next();
                            String tagStlJobName = tagStlJobInstance.getJobName();
                            if (tagNames.contains(tagStlJobName)) {
                                continue;
                            }
                            Iterator<JobExecution> tagStlExecIterator = getJobExecutions(tagStlJobInstance).iterator();
                            if (tagStlExecIterator.hasNext()) {
                                JobExecution tagJobExecution = tagStlExecIterator.next();
                                JobParameters tagJobParameters = tagJobExecution.getJobParameters();
                                Date tagStartDate = tagJobParameters.getDate(START_DATE);
                                Date tagEndDate = tagJobParameters.getDate(END_DATE);
                                Long groupId = tagJobParameters.getLong(GROUP_ID);

                                StlJobGroupDto stlJobGroupDto = stlJobGroupDtoMap.getOrDefault(groupId, new StlJobGroupDto());
                                stlJobGroupDto.setCurrentlyRunning(groupId.equals(lockedGroupId));
                                stlJobGroupDto.setLatestAdjustment(groupId.equals(latestGroupId));
                                stlJobGroupDto.setHeader(jobId.equals(groupId));
                                BatchStatus currentStatus = tagJobExecution.getStatus();
                                stlJobGroupDto.setStatus(convertStatus(currentStatus, STAGE_TAGGING));
                                stlJobGroupDto.setTaggingStatus(currentStatus);
                                stlJobGroupDto.setGroupId(groupId);
//                                stlJobGroupDto.setBillingPeriodStr(billingPeriodStr);

                                JobCalculationDto finalizeJobDto = new JobCalculationDto(tagJobExecution.getStartTime(),
                                        tagJobExecution.getEndTime(), tagStartDate, tagEndDate,
                                        convertStatus(currentStatus, STAGE_TAGGING), STAGE_TAGGING);

                                stlJobGroupDto.getJobCalculationDtos().add(finalizeJobDto);

                                if (!stlJobGroupDto.getLatestJobExecStartDate().after(tagJobExecution.getStartTime())) {
                                    updateProgress(tagJobExecution, stlJobGroupDto);
                                }

                                stlJobGroupDtoMap.put(groupId, stlJobGroupDto);
                                if (stlJobGroupDto.isHeader()) {
                                    parentStlJobGroupDto = stlJobGroupDto;
                                }
                            }
                            tagNames.add(tagStlJobName);
                        }
                    /* TAGGING GMR END */

                    /* OUTPUT GENERATION START */
                        String generationQueryString = GENERATE_INVOICE_STL_JOB_NAME.concat("*-").concat(parentId).concat("-*");
                        List<JobInstance> generationStlJobInstances = jobExplorer.findJobInstancesByJobName(generationQueryString, 0, Integer.MAX_VALUE);
                        Iterator<JobInstance> generationStlIterator = generationStlJobInstances.iterator();
                        Set<String> generationNames = Sets.newHashSet();
                        while (generationStlIterator.hasNext()) {
                            JobInstance generationStlJobInstance = generationStlIterator.next();
                            String generationStlJobName = generationStlJobInstance.getJobName();
                            if (generationNames.contains(generationStlJobName)) {
                                continue;
                            }
                            Iterator<JobExecution> generationStlExecIterator = getJobExecutions(generationStlJobInstance).iterator();
                            if (generationStlExecIterator.hasNext()) {
                                JobExecution generationJobExecution = generationStlExecIterator.next();
                                JobParameters generationJobParameters = generationJobExecution.getJobParameters();
                                Long groupId = generationJobParameters.getLong(GROUP_ID);
                                StlJobGroupDto stlJobGroupDto = stlJobGroupDtoMap.getOrDefault(groupId, new StlJobGroupDto());
                                stlJobGroupDto.setCurrentlyRunning(groupId.equals(lockedGroupId));
                                stlJobGroupDto.setLatestAdjustment(groupId.equals(latestGroupId));
                                stlJobGroupDto.setHeader(jobId.equals(groupId));
                                BatchStatus currentStatus = generationJobExecution.getStatus();
                                stlJobGroupDto.setInvoiceGenerationStatus(currentStatus);
                                stlJobGroupDto.setGroupId(groupId);
                                stlJobGroupDto.setRunId(generationJobParameters.getLong(RUN_ID));
                                stlJobGroupDto.setRunStartDateTime(generationJobExecution.getStartTime());
                                stlJobGroupDto.setRunEndDateTime(generationJobExecution.getEndTime());
//                                stlJobGroupDto.setBillingPeriodStr(billingPeriodStr);

                                if (!stlJobGroupDto.getLatestJobExecStartDate().after(generationJobExecution.getStartTime())) {
                                    updateProgress(generationJobExecution, stlJobGroupDto);
                                }

                                stlJobGroupDtoMap.put(groupId, stlJobGroupDto);

                                Optional.ofNullable(generationJobExecution.getExecutionContext()
                                        .get(INVOICE_GENERATION_FILENAME)).ifPresent(val ->
                                        stlJobGroupDto.setInvoiceGenFolder((String) val));

                                if (stlJobGroupDto.isHeader()) {
                                    parentStlJobGroupDto = stlJobGroupDto;
                                }
                            }
                            generationNames.add(generationStlJobName);
                        }
                    /* OUTPUT GENERATION END */

                        taskExecutionDto.setStlJobGroupDtoMap(stlJobGroupDtoMap);
                        taskExecutionDto.setParentStlJobGroupDto(parentStlJobGroupDto);
                        return taskExecutionDto;
                    } else {
                        return null;
                    }

                })
                .filter(Objects::nonNull)
                .collect(toList());

        return new PageImpl<>(taskExecutionDtos, pageable, totalSize);
    }

    private Map<String, List<JobCalculationDto>> getCalcGmrJobCalculationMap(List<JobInstance> calcGmrStlJobInstances) {
        Map<String, List<JobCalculationDto>> jobCalculationDtoMap = new HashMap<>();
        // add distinct jobNames for multiple same parentId-groupId job instances
        calcGmrStlJobInstances.stream().map(JobInstance::getJobName).distinct().forEach(calcJobInstanceName ->
                jobCalculationDtoMap.put(calcJobInstanceName, new ArrayList<>()));

        calcGmrStlJobInstances.forEach(calcGmrInstance -> getJobExecutions(calcGmrInstance).forEach(jobExecution -> {
            JobParameters calcGmrJobParameters = jobExecution.getJobParameters();
            Date calcGmrStartDate = calcGmrJobParameters.getDate(START_DATE);
            Date calcGmrEndDate = calcGmrJobParameters.getDate(END_DATE);
            JobCalculationDto gmrCalcDto = new JobCalculationDto(jobExecution.getStartTime(), jobExecution.getEndTime(),  calcGmrStartDate,
                    calcGmrEndDate, convertStatus(jobExecution.getStatus(), STAGE_GMR_CALC), STAGE_GMR_CALC);
            gmrCalcDto.setTaskSummaryList(showSummary(jobExecution, STL_GMR_CALC_STEP_WITH_SKIP_LOGS));
            jobCalculationDtoMap.get(calcGmrInstance.getJobName()).add(gmrCalcDto);
        }));

        return jobCalculationDtoMap;
    }

    private String getLatestJobCalcStatusByStage(StlJobGroupDto stlJobGroupDto, String stage) {
        return stlJobGroupDto.getSortedJobCalculationDtos().stream()
                .filter(stlJob -> stlJob.getJobStage().equals(stage))
                .map(JobCalculationDto::getStatus).findFirst().get();
    }

    @Override
    public Page<? extends BaseTaskExecutionDto> findJobInstances(Pageable pageable, String type, String status, String mode, String runStartDate,
                                                                 String tradingStartDate, String tradingEndDate, String useername) {
        return null;
    }

    @Override
    public Page<? extends BaseTaskExecutionDto> findJobInstances(Pageable pageable) {
        return null;
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
        final Long runId = System.currentTimeMillis();

        arguments.add(concatKeyValue(PARENT_JOB, taskRunDto.getParentJob(), "long"));
        arguments.add(concatKeyValue(RUN_ID, String.valueOf(runId), "long"));

        Long groupId = taskRunDto.isNewGroup() ? runId : Long.parseLong(taskRunDto.getGroupId());
        arguments.add(concatKeyValue(GROUP_ID, groupId.toString(), "long"));
        arguments.add(concatKeyValue(USERNAME, taskRunDto.getCurrentUser()));

        Date start = null;
        Date end = null;

        log.info("Running JobName=[{}], type=[{}], baseType=[{}]", taskRunDto.getJobName(), taskRunDto.getMeterProcessType(),
                taskRunDto.getBaseType());

        if (COMPUTE_STL_JOB_NAME.equals(taskRunDto.getJobName())) {
            Preconditions.checkState(batchJobRunLockRepository.countByJobNameAndLockedIsTrue(FINALIZE_JOB_NAME) == 0,
                    "There is an existing tag as output ready job running");
            Preconditions.checkState(batchJobRunLockRepository.countByJobNameAndLockedIsTrue(COMPUTE_GMRVAT_MFEE_JOB_NAME) == 0,
                    "There is an existing ".concat(COMPUTE_GMRVAT_MFEE_JOB_NAME).concat(" job running"));

            LocalDateTime baseStartDate = null;
            LocalDateTime baseEndDate = null;
            String type = taskRunDto.getMeterProcessType();
            if (type == null) {
                type = PROCESS_TYPE_DAILY;
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("dailyStlAmtsCalculation")));
                arguments.add(concatKeyValue(START_DATE, taskRunDto.getTradingDate(), "date"));
            } else {
                try {
                    baseStartDate = DateUtil.getStartRangeDate(taskRunDto.getBaseStartDate());
                    baseEndDate = DateUtil.getStartRangeDate(taskRunDto.getBaseEndDate());
                    start = DateUtil.convertToDate(baseStartDate);
                    end = DateUtil.convertToDate(baseEndDate);
                } catch (ParseException e) {
                    log.warn("Unable to parse billing date", e);
                }

                if (MeterProcessType.ADJUSTED.name().equals(type)) {
                    boolean finalBased = "FINAL".equals(taskRunDto.getBaseType());

                    if (batchJobAdjRunRepository.countByGroupIdAndBillingPeriodStartAndBillingPeriodEnd(groupId.toString(), baseStartDate, baseEndDate) < 1) {
                        log.info("Saving to batchjobadjrun and batchjobadjvatrun with groupId=[{}] and billingPeriodStart=[{}] "
                                + "and billingPeriodEnd=[{}]", groupId.toString(), baseStartDate, baseEndDate);
                        saveAdjRun(MeterProcessType.ADJUSTED, taskRunDto.getParentJob(), groupId, baseStartDate, baseEndDate);
                        saveAdjVatRun(MeterProcessType.ADJUSTED, taskRunDto.getParentJob(), groupId, baseStartDate, baseEndDate);
                    }

                    String activeProfile = finalBased ? "monthlyAdjustedStlAmtsMtrFinCalculation" : "monthlyAdjustedStlAmtsMtrAdjCalculation";
                    properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(activeProfile)));

                } else if (MeterProcessType.PRELIMINARY.name().equals(type) || MeterProcessType.PRELIM.name().equals(type)) {
                    properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyPrelimStlAmtsCalculation")));

                } else if (MeterProcessType.FINAL.name().equals(type)) {

                    if (batchJobAdjRunRepository.countByGroupIdAndBillingPeriodStartAndBillingPeriodEnd(groupId.toString(), baseStartDate, baseEndDate) < 1) {
                        log.info("Saving to batchjobadjrun and batchjobadjvatrun with groupId=[{}] and billingPeriodStart=[{}] "
                                + "and billingPeriodEnd=[{}]", groupId.toString(), baseStartDate, baseEndDate);
                        saveAdjRun(MeterProcessType.FINAL, taskRunDto.getParentJob(), groupId, baseStartDate, baseEndDate);
                        saveAdjVatRun(MeterProcessType.FINAL, taskRunDto.getParentJob(), Long.parseLong(taskRunDto.getGroupId()), baseStartDate, baseEndDate);
                    }
                    properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyFinalStlAmtsCalculation")));
                }
                arguments.add(concatKeyValue(START_DATE, taskRunDto.getStartDate(), "date"));
                arguments.add(concatKeyValue(END_DATE, taskRunDto.getEndDate(), "date"));
            }
            arguments.add(concatKeyValue(PROCESS_TYPE, type));
            jobName = "crss-settlement-task-calculation";

            if (MeterProcessType.ADJUSTED.name().equals(taskRunDto.getMeterProcessType())
                    || MeterProcessType.FINAL.name().equals(taskRunDto.getMeterProcessType())) {
                if (runningAdjustmentLockRepository.lockJob(groupId, Long.parseLong(taskRunDto.getParentJob()), start, end) == 0) {
                    RunningAdjustmentLock lock = new RunningAdjustmentLock();
                    lock.setLocked(true);
                    lock.setGroupId(groupId);
                    lock.setParentJobId(Long.parseLong(taskRunDto.getParentJob()));
                    lock.setStartDate(Date.from(baseStartDate.atZone(ZoneId.systemDefault()).toInstant()));
                    lock.setEndDate(Date.from(baseEndDate.atZone(ZoneId.systemDefault()).toInstant()));
                    runningAdjustmentLockRepository.save(lock);
                }
                if (taskRunDto.isNewGroup() && latestAdjustmentLockRepository.lockJob(groupId, Long.parseLong(taskRunDto.getParentJob()), start, end) == 0) {
                    LatestAdjustmentLock lock = new LatestAdjustmentLock();
                    lock.setLocked(true);
                    lock.setGroupId(groupId);
                    lock.setParentJobId(Long.parseLong(taskRunDto.getParentJob()));
                    lock.setStartDate(Date.from(baseStartDate.atZone(ZoneId.systemDefault()).toInstant()));
                    lock.setEndDate(Date.from(baseEndDate.atZone(ZoneId.systemDefault()).toInstant()));
                    latestAdjustmentLockRepository.save(lock);
                }
            }
        } else if (COMPUTE_GMRVAT_MFEE_JOB_NAME.equals(taskRunDto.getJobName())) {
            Preconditions.checkState(batchJobRunLockRepository.countByJobNameAndLockedIsTrue(FINALIZE_JOB_NAME) == 0,
                    "There is an existing tag as output ready job running");
            Preconditions.checkState(batchJobRunLockRepository.countByJobNameAndLockedIsTrue(COMPUTE_STL_JOB_NAME) == 0,
                    "There is an existing ".concat(COMPUTE_STL_JOB_NAME).concat(" job running"));
            validatePartialCalculation(taskRunDto);

            String type = taskRunDto.getMeterProcessType();

            BatchJobAdjRun batchJobAdjRun = batchJobAdjRunRepository.findByGroupId(taskRunDto.getGroupId());
            if (batchJobAdjRun != null) {
                type = batchJobAdjRun.getMeterProcessType().name();
            }

            Preconditions.checkState(type != null, "Cannot run GMR/Market Fee calculation job on daily basis.");
            if (MeterProcessType.ADJUSTED.name().equals(type)) {
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyAdjustedGmrVatMfeeCalculation")));
            } else if (MeterProcessType.PRELIMINARY.name().equals(type) || MeterProcessType.PRELIM.name().equals(type)) {
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyPrelimGmrVatMfeeCalculation")));
            } else if (MeterProcessType.FINAL.name().equals(type)) {
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyFinalGmrVatMfeeCalculation")));
            }

            arguments.add(concatKeyValue(START_DATE, taskRunDto.getStartDate(), "date"));
            arguments.add(concatKeyValue(END_DATE, taskRunDto.getEndDate(), "date"));
            arguments.add(concatKeyValue(PROCESS_TYPE, type));
            jobName = "crss-settlement-task-calculation";

        } else if (FINALIZE_JOB_NAME.equals(taskRunDto.getJobName())) {
            Preconditions.checkState(batchJobRunLockRepository.countByJobNameAndLockedIsTrue(COMPUTE_STL_JOB_NAME) == 0,
                    "There is an existing ".concat(COMPUTE_STL_JOB_NAME).concat(" job running"));
            Preconditions.checkState(batchJobRunLockRepository.countByJobNameAndLockedIsTrue(COMPUTE_GMRVAT_MFEE_JOB_NAME) == 0,
                    "There is an existing ".concat(COMPUTE_GMRVAT_MFEE_JOB_NAME).concat(" job running"));

            String type = taskRunDto.getMeterProcessType();

            BatchJobAdjRun batchJobAdjRun = batchJobAdjRunRepository.findByGroupId(taskRunDto.getGroupId());
            if (batchJobAdjRun != null) {
                type = batchJobAdjRun.getMeterProcessType().name();
            }

            Preconditions.checkState(type != null, "Cannot run tagging job on daily basis.");
            if (MeterProcessType.ADJUSTED.name().equals(type)) {
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyAdjustedTagging")));
            } else if (MeterProcessType.PRELIMINARY.name().equals(type) || MeterProcessType.PRELIM.name().equals(type)) {
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyPrelimTagging")));
            } else if (MeterProcessType.FINAL.name().equals(type)) {
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyFinalTagging")));
            }

            arguments.add(concatKeyValue(PROCESS_TYPE, type));
            arguments.add(concatKeyValue(START_DATE, taskRunDto.getStartDate(), "date"));
            arguments.add(concatKeyValue(END_DATE, taskRunDto.getEndDate(), "date"));
            jobName = "crss-settlement-task-calculation";

        } else if (GENERATE_INVOICE_STL_JOB_NAME.equals(taskRunDto.getJobName())) {
            Preconditions.checkState(batchJobRunLockRepository.countByJobNameAndLockedIsTrue(FINALIZE_JOB_NAME) == 0,
                    "There is an existing ".concat(FINALIZE_JOB_NAME).concat(" job running"));

            String type = taskRunDto.getMeterProcessType();
            if (MeterProcessType.ADJUSTED.name().equals(type)) {
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyAdjustedInvoiceGeneration")));
                saveAMSadditionalParams(runId, taskRunDto);
            } else if (MeterProcessType.PRELIMINARY.name().equals(type) || "PRELIM".equals(type)) {
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyPrelimInvoiceGeneration")));
            } else if (MeterProcessType.FINAL.name().equals(type)) {
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyFinalInvoiceGeneration")));
                saveAMSadditionalParams(runId, taskRunDto);
            }
            arguments.add(concatKeyValue(PROCESS_TYPE, type));
            arguments.add(concatKeyValue(START_DATE, taskRunDto.getStartDate(), "date"));
            arguments.add(concatKeyValue(END_DATE, taskRunDto.getEndDate(), "date"));
            jobName = "crss-settlement-task-invoice-generation";
        } else if (STL_VALIDATION_JOB_NAME.equals(taskRunDto.getJobName())) {
            Preconditions.checkState(batchJobRunLockRepository.countByJobNameAndLockedIsTrue(STL_VALIDATION_JOB_NAME) == 0,
                    String.format("There is an existing %s job running", STL_VALIDATION_JOB_NAME));
            String type = taskRunDto.getMeterProcessType();

            if (type == null) {
                type = PROCESS_TYPE_DAILY;
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("dailyStlValidation")));
                arguments.add(concatKeyValue(START_DATE, taskRunDto.getTradingDate(), "date"));
            } else {
                if (MeterProcessType.ADJUSTED.name().equals(type)) {
                    properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyAdjustedStlValidation")));
                } else if (MeterProcessType.PRELIMINARY.name().equals(type) || "PRELIM".equals(type)) {
                    properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyPrelimStlValidation")));
                } else if (MeterProcessType.FINAL.name().equals(type)) {
                    properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyFinalStlValidation")));
                }
                arguments.add(concatKeyValue(START_DATE, taskRunDto.getStartDate(), "date"));
                arguments.add(concatKeyValue(END_DATE, taskRunDto.getEndDate(), "date"));
            }

            arguments.add(concatKeyValue(PROCESS_TYPE, type));
            jobName = "crss-settlement-task-validation";
        }
        log.info("Running job name={}, properties={}, arguments={}", taskRunDto.getJobName(), properties, arguments);

        if (jobName != null) {
            log.info("Running job name={}, properties={}, arguments={}", taskRunDto.getJobName(), properties, arguments);
            launchJob(jobName, properties, arguments);
            lockJob(taskRunDto);
        }
    }

    private void saveAMSadditionalParams(final Long runId, final TaskRunDto taskRunDto) {
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

            BatchJobAddtlParams batchJobAddtlParamsRemarksInv = new BatchJobAddtlParams();
            batchJobAddtlParamsRemarksInv.setRunId(runId);
            batchJobAddtlParamsRemarksInv.setType("STRING");
            batchJobAddtlParamsRemarksInv.setKey(AMS_REMARKS_INV);
            batchJobAddtlParamsRemarksInv.setStringVal(taskRunDto.getAmsRemarksInv());
            batchJobAddtlParamsRepository.save(batchJobAddtlParamsRemarksInv);

            BatchJobAddtlParams batchJobAddtlParamsRemarksMf = new BatchJobAddtlParams();
            batchJobAddtlParamsRemarksMf.setRunId(runId);
            batchJobAddtlParamsRemarksMf.setType("STRING");
            batchJobAddtlParamsRemarksMf.setKey(AMS_REMARKS_MF);
            batchJobAddtlParamsRemarksMf.setStringVal(taskRunDto.getAmsRemarksMf());
            batchJobAddtlParamsRepository.save(batchJobAddtlParamsRemarksMf);
        } catch (ParseException e) {
            log.error("Error parsing additional batch job params for AMS: {}", e);
        }
    }

    @Override
    public void relaunchFailedJob(long jobId) throws URISyntaxException {

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

    private void removeDateRangeFrom(SortedSet<LocalDate> remainingDates, Date calcStartDate, Date calcEndDate) {
        LocalDate startDate = calcStartDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
        LocalDate endDate = calcEndDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();

        LocalDate ctrDate = startDate;
        while (ctrDate.isBefore(endDate) || ctrDate.isEqual(endDate)) {
            remainingDates.remove(ctrDate);
            ctrDate = ctrDate.plusDays(1);
        }
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

    private String resolveBillingPeriod(Long parentId) {
        JobInstance jobInstance = jobExplorer.getJobInstance(parentId);
        if (jobInstance != null) {
            Optional<JobExecution> jobExecutionOpt = getJobExecutions(jobInstance).stream().findFirst();

            if (jobExecutionOpt.isPresent()) {
                Long runId = jobExecutionOpt.get().getJobParameters().getLong(TaskUtil.RUN_ID);
                BatchJobAddtlParams addtlParamsBP = batchJobAddtlParamsRepository.findByRunIdAndKey(runId, "billingPeriodId");
                Long billingPeriodId = addtlParamsBP != null ? addtlParamsBP.getLongVal() : null;
                BatchJobAddtlParams addtlParamsSM = batchJobAddtlParamsRepository.findByRunIdAndKey(runId, "supplyMonth");
                String supplyMonth = addtlParamsSM != null ? addtlParamsSM.getStringVal() : null;

                if (StringUtils.isNotEmpty(supplyMonth) && billingPeriodId != null) {
                    return billingPeriodId + " - " + supplyMonth;
                }
            }
        }

        return "";
    }

    private void validatePartialCalculation(final TaskRunDto taskRunDto) {
        String parentGroup = taskRunDto.getParentJob() + "-" + taskRunDto.getGroupId();

        MeterProcessType type = MeterProcessType.valueOf(taskRunDto.getMeterProcessType());

        BatchJobAdjRun batchJobAdjRun = batchJobAdjRunRepository.findByGroupId(taskRunDto.getGroupId());
        if (batchJobAdjRun != null) {
            type = batchJobAdjRun.getMeterProcessType();
        }

        List<JobExecution> jobExecutions = dataFlowJdbcJobExecutionDao.findStlCalcJobInstances(parentGroup, type, taskRunDto.getStartDate(), taskRunDto.getEndDate());
        Preconditions.checkState(jobExecutions.size() > 0, "There should be a completed ".concat(COMPUTE_STL_JOB_NAME).concat(" job for "));

        Date start = null;
        Date end = null;
        try {
            start = DateUtil.convertToDate(DateUtil.getStartRangeDate(taskRunDto.getStartDate()));
            end = DateUtil.convertToDate(DateUtil.getStartRangeDate(taskRunDto.getEndDate()));
        } catch (ParseException e) {
            log.error("Unable to convert: string -> date -> localDate ", e);
        }

        SortedSet<LocalDate> remainingDate = createRange(start, end);
        jobExecutions.forEach(jobExecution -> {
            JobParameters jobParameters = jobExecution.getJobParameters();
            removeDateRangeFrom(remainingDate, jobParameters.getDate(START_DATE), jobParameters.getDate(END_DATE));
        });
        Preconditions.checkState(remainingDate.size() < 1, "Incomplete calculation is not allowed for job: ".concat(COMPUTE_GMRVAT_MFEE_JOB_NAME));
    }

    private void saveAdjRun(MeterProcessType type, String jobId, Long groupId, LocalDateTime start, LocalDateTime end) {
        BatchJobAdjRun batchJobAdjRun = new BatchJobAdjRun();
        batchJobAdjRun.setJobId(jobId);
        batchJobAdjRun.setGroupId(String.valueOf(groupId));
        batchJobAdjRun.setMeterProcessType(type);
        batchJobAdjRun.setBillingPeriodStart(start);
        batchJobAdjRun.setBillingPeriodEnd(end);
        batchJobAdjRun.setOutputReady(false);
        batchJobAdjRunRepository.save(batchJobAdjRun);
    }

    private void saveAdjVatRun(MeterProcessType type, String jobId, Long groupId, LocalDateTime start, LocalDateTime end) {
        BatchJobAdjVatRun adjVatRun = new BatchJobAdjVatRun();
        adjVatRun.setAdditionalCompensation(false);
        adjVatRun.setJobId(jobId);
        adjVatRun.setGroupId(String.valueOf(groupId));
        adjVatRun.setMeterProcessType(type);
        adjVatRun.setBillingPeriodStart(start);
        adjVatRun.setBillingPeriodEnd(end);
        adjVatRun.setOutputReady(false);

        batchJobAdjVatRunRepository.save(adjVatRun);
    }

}
