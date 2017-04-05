package com.pemc.crss.dataflow.app.service.impl;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.pemc.crss.dataflow.app.dto.*;
import com.pemc.crss.shared.commons.reference.MeterProcessType;
import com.pemc.crss.shared.commons.reference.SettlementStepUtil;
import com.pemc.crss.shared.commons.util.DateUtil;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobAddtlParams;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobAdjRun;
import com.pemc.crss.shared.core.dataflow.entity.LatestAdjustmentLock;
import com.pemc.crss.shared.core.dataflow.entity.RunningAdjustmentLock;
import com.pemc.crss.shared.core.dataflow.repository.BatchJobAddtlParamsRepository;
import com.pemc.crss.shared.core.dataflow.repository.BatchJobAdjRunRepository;
import com.pemc.crss.shared.core.dataflow.repository.LatestAdjustmentLockRepository;
import com.pemc.crss.shared.core.dataflow.repository.RunningAdjustmentLockRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private static final Logger LOG = LoggerFactory.getLogger(SettlementTaskExecutionServiceImpl.class);

    // Job names
    private static final String STL_READY_JOB_NAME = "processStlReady";
    private static final String COMPUTE_STL_JOB_NAME = "computeSettlementSTL_AMT";
    private static final String FINALIZE_STL_JOB_NAME = "tasAsOutputReadySTL_AMT";
    private static final String COMPUTE_GMRVAT_MFEE_JOB_NAME = "computeSettlementGMR_MFEE";
    private static final String FINALIZE_GMRVAT_MFEE_JOB_NAME = "tasAsOutputReadyGMR_MFEE";
    private static final String GENERATE_INVOICE_STL_JOB_NAME = "generateInvoiceSettlement";

    private static final String AMS_INVOICE_DATE = "amsInvoiceDate";
    private static final String AMS_DUE_DATE = "amsDueDate";
    private static final String AMS_REMARKS_INV = "amsRemarksInv";
    private static final String AMS_REMARKS_MF = "amsRemarksMf";

    // from batch_job_execution_context
    private static final String INVOICE_GENERATION_FILENAME = "INVOICE_GENERATION_FILENAME";

    private static final String GROUP_ID = "groupId";

    @Autowired
    private BatchJobAddtlParamsRepository batchJobAddtlParamsRepository;

    @Autowired
    private BatchJobAdjRunRepository batchJobAdjRunRepository;

    @Autowired
    private RunningAdjustmentLockRepository runningAdjustmentLockRepository;

    @Autowired
    private LatestAdjustmentLockRepository latestAdjustmentLockRepository;

    @Override
    public Page<StlTaskExecutionDto> findJobInstances(Pageable pageable) {

        List<StlTaskExecutionDto> taskExecutionDtos = jobExplorer.findJobInstancesByJobName(STL_READY_JOB_NAME.concat("*"),
                pageable.getOffset(), pageable.getPageSize()).stream()
                .map((JobInstance jobInstance) -> {

                    if (getJobExecutions(jobInstance).iterator().hasNext()) {
                        JobExecution jobExecution = getJobExecutions(jobInstance).iterator().next();
                        Long jobId = jobExecution.getJobId();
                        BatchStatus jobStatus = jobExecution.getStatus();
                        if (BatchStatus.COMPLETED != jobStatus) {
                            LOG.debug("Job processStlReady with id {} removed with status: {} ", jobId, jobStatus.name());
                            return null;
                        }
                        LOG.debug("Processing processStlReady jobId {}", jobId);

                        String parentId = jobInstance.getJobName().split("-")[1];
                        if (StringUtils.isEmpty(parentId)) {
                            LOG.warn("Parent id not appended for job instance id {}. Setting parent as self..", jobInstance.getId());
                            parentId = String.valueOf(jobInstance.getInstanceId());
                        }
                        JobParameters jobParameters = jobExecution.getJobParameters();
                        Date startDate = jobParameters.getDate(START_DATE);
                        Date endDate = jobParameters.getDate(END_DATE);
                        Date date = jobParameters.getDate(DATE);
                        boolean isDaily = jobParameters.getString(PROCESS_TYPE) == null;
                        LOG.debug("Date Range -> from {} to {} | Date -> {}", startDate, endDate, date);

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
                        Map<Long, StlJobGroupDto> stlJobGroupDtoMap = new HashMap<>();
//                    Date recentJobEndTime = Date.from(Instant.MIN);

                    /* CALCULATION START */
                        String calcStatusSuffix = "PARTIAL-CALCULATION";
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

                                List<PartialCalculationDto> partialCalculationDtoList = stlJobGroupDto.getPartialCalculationDtos();
                                if (partialCalculationDtoList == null) {
                                    partialCalculationDtoList = Lists.newArrayList();
                                    stlJobGroupDto.setRunStartDateTime(calcJobExecution.getStartTime());
                                    stlJobGroupDto.setRunEndDateTime(calcJobExecution.getEndTime());
                                }
                                PartialCalculationDto dto = new PartialCalculationDto();
                                dto.setStatus(convertStatus(currentStatus, calcStatusSuffix));
                                dto.setBillingStart(calcStartDate);
                                dto.setBillingEnd(calcEndDate);
                                dto.setRunDate(calcJobExecution.getStartTime());
                                dto.setRunEndDate(calcJobExecution.getEndTime());
                                partialCalculationDtoList.add(dto);

                                if (!isDaily && BatchStatus.COMPLETED == currentStatus
                                    && stlJobGroupDto.getRemainingDatesMap().containsKey(groupId)) {
                                    removeDateRangeFrom(stlJobGroupDto.getRemainingDatesMap().get(groupId), calcStartDate, calcEndDate);
                                }

                                stlJobGroupDto.setPartialCalculationDtos(partialCalculationDtoList);
                                stlJobGroupDto.setStatus(convertStatus(isDaily ? currentStatus : jobStatus, calcStatusSuffix));
                                stlJobGroupDto.setGroupId(groupId);

                                Date latestJobExecStartDate = stlJobGroupDto.getLatestJobExecStartDate();
                                if (latestJobExecStartDate == null || !latestJobExecStartDate.after(calcJobExecution.getStartTime())) {
                                    updateProgress(calcJobExecution, stlJobGroupDto);
                                }

                                stlJobGroupDtoMap.put(groupId, stlJobGroupDto);

                                if (stlJobGroupDto.isHeader()) {
                                    parentStlJobGroupDto = stlJobGroupDto;
                                }
                            }
                        }
                    /* CALCULATION END */

                    /* CALCULATION TAGGING START */
                        String calcTagStatusSuffix = "CALCULATION-TAGGING";
                        String calcTagQueryString = FINALIZE_STL_JOB_NAME.concat("*-").concat(parentId).concat("-*");
                        List<JobInstance> calcTagStlJobInstances = jobExplorer.findJobInstancesByJobName(calcTagQueryString, 0, Integer.MAX_VALUE);
                        Iterator<JobInstance> calcTagStlIterator = calcTagStlJobInstances.iterator();
                        Set<String> calcTagNames = Sets.newHashSet();
                        while (calcTagStlIterator.hasNext()) {
                            JobInstance calcTagStlJobInstance = calcTagStlIterator.next();
                            String calcTagStlJobName = calcTagStlJobInstance.getJobName();
                            if (calcTagNames.contains(calcTagStlJobName)) {
                                continue;
                            }
                            Iterator<JobExecution> calcTagStlExecIterator = getJobExecutions(calcTagStlJobInstance).iterator();
                            if (calcTagStlExecIterator.hasNext()) {
                                JobExecution calcTagJobExecution = calcTagStlExecIterator.next();
                                JobParameters calcTagJobParameters = calcTagJobExecution.getJobParameters();
                                Long groupId = calcTagJobParameters.getLong(GROUP_ID);
                                StlJobGroupDto stlJobGroupDto = stlJobGroupDtoMap.getOrDefault(groupId, new StlJobGroupDto());
                                stlJobGroupDto.setCurrentlyRunning(groupId.equals(lockedGroupId));
                                stlJobGroupDto.setLatestAdjustment(groupId.equals(latestGroupId));
                                stlJobGroupDto.setHeader(jobId.equals(groupId));
                                BatchStatus currentStatus = calcTagJobExecution.getStatus();
                                stlJobGroupDto.setStatus(convertStatus(currentStatus, calcTagStatusSuffix));
                                stlJobGroupDto.setStlAmtTaggingStatus(currentStatus);
                                stlJobGroupDto.setGroupId(groupId);

                                if (!stlJobGroupDto.getLatestJobExecStartDate().after(calcTagJobExecution.getStartTime())) {
                                    updateProgress(calcTagJobExecution, stlJobGroupDto);
                                }

                                stlJobGroupDtoMap.put(groupId, stlJobGroupDto);
                                if (stlJobGroupDto.isHeader()) {
                                    parentStlJobGroupDto = stlJobGroupDto;
                                }
                            }
                            calcTagNames.add(calcTagStlJobName);
                        }
                    /* CALCULATION TAGGING END */

                    /* CALCULATION GMR START */
                        String calcGmrStatusSuffix = "CALCULATION-GMR";
                        String calcGmrQueryString = COMPUTE_GMRVAT_MFEE_JOB_NAME.concat("*-").concat(parentId).concat("-*");
                        List<JobInstance> calcGmrStlJobInstances = jobExplorer.findJobInstancesByJobName(calcGmrQueryString, 0, Integer.MAX_VALUE);
                        Iterator<JobInstance> calcGmrStlIterator = calcGmrStlJobInstances.iterator();
                        Set<String> calcGmrNames = Sets.newHashSet();
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
                                stlJobGroupDto.setCurrentlyRunning(groupId.equals(lockedGroupId));
                                stlJobGroupDto.setLatestAdjustment(groupId.equals(latestGroupId));
                                stlJobGroupDto.setHeader(jobId.equals(groupId));
                                BatchStatus currentStatus = calcGmrJobExecution.getStatus();
                                stlJobGroupDto.setStatus(convertStatus(currentStatus, calcGmrStatusSuffix));
                                stlJobGroupDto.setGmrVatMFeeCalculationStatus(currentStatus);
                                stlJobGroupDto.setGroupId(groupId);

                                if (!stlJobGroupDto.getLatestJobExecStartDate().after(calcGmrJobExecution.getStartTime())) {
                                    updateProgress(calcGmrJobExecution, stlJobGroupDto);
                                }

                                stlJobGroupDtoMap.put(groupId, stlJobGroupDto);
                                if (stlJobGroupDto.isHeader()) {
                                    parentStlJobGroupDto = stlJobGroupDto;
                                }
                            }
                            calcGmrNames.add(calcGmrStlJobName);
                        }
                    /* CALCULATION GMR END */

                    /* TAGGING GMR START */
                        String tagGmrStatusSuffix = "TAGGING-GMR";
                        String tagGmrQueryString = FINALIZE_GMRVAT_MFEE_JOB_NAME.concat("*-").concat(parentId).concat("-*");
                        List<JobInstance> tagGmrStlJobInstances = jobExplorer.findJobInstancesByJobName(tagGmrQueryString, 0, Integer.MAX_VALUE);
                        Iterator<JobInstance> tagGmrStlIterator = tagGmrStlJobInstances.iterator();
                        Set<String> tagGmrNames = Sets.newHashSet();
                        while (tagGmrStlIterator.hasNext()) {
                            JobInstance tagGmrStlJobInstance = tagGmrStlIterator.next();
                            String tagGmrStlJobName = tagGmrStlJobInstance.getJobName();
                            if (tagGmrNames.contains(tagGmrStlJobName)) {
                                continue;
                            }
                            Iterator<JobExecution> tagGmrStlExecIterator = getJobExecutions(tagGmrStlJobInstance).iterator();
                            if (tagGmrStlExecIterator.hasNext()) {
                                JobExecution tagGmrJobExecution = tagGmrStlExecIterator.next();
                                JobParameters tagGmrJobParameters = tagGmrJobExecution.getJobParameters();
                                Long groupId = tagGmrJobParameters.getLong(GROUP_ID);
                                StlJobGroupDto stlJobGroupDto = stlJobGroupDtoMap.getOrDefault(groupId, new StlJobGroupDto());
                                stlJobGroupDto.setCurrentlyRunning(groupId.equals(lockedGroupId));
                                stlJobGroupDto.setLatestAdjustment(groupId.equals(latestGroupId));
                                stlJobGroupDto.setHeader(jobId.equals(groupId));
                                BatchStatus currentStatus = tagGmrJobExecution.getStatus();
                                stlJobGroupDto.setStatus(convertStatus(currentStatus, tagGmrStatusSuffix));
                                stlJobGroupDto.setGmrVatMFeeTaggingStatus(currentStatus);
                                stlJobGroupDto.setGroupId(groupId);

                                if (!stlJobGroupDto.getLatestJobExecStartDate().after(tagGmrJobExecution.getStartTime())) {
                                    updateProgress(tagGmrJobExecution, stlJobGroupDto);
                                }

                                stlJobGroupDtoMap.put(groupId, stlJobGroupDto);
                                if (stlJobGroupDto.isHeader()) {
                                    parentStlJobGroupDto = stlJobGroupDto;
                                }
                            }
                            tagGmrNames.add(tagGmrStlJobName);
                        }
                    /* TAGGING GMR END */

                    /* OUTPUT GENERATION START */
                        String generationStatusSuffix = "TAGGING-GMR";
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

        return new PageImpl<>(taskExecutionDtos, pageable, taskExecutionDtos.size());
    }

    @Override
    public Page<? extends BaseTaskExecutionDto> findJobInstances(Pageable pageable, String type, String status, String mode, String runStartDate,
                                                                 String tradingStartDate, String tradingEndDate, String useername) {
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

        Date start = null;
        Date end = null;

        if (COMPUTE_STL_JOB_NAME.equals(taskRunDto.getJobName())) {

            Preconditions.checkState(batchJobRunLockRepository.countByJobNameAndLockedIsTrue(FINALIZE_STL_JOB_NAME) == 0,
                    "There is an existing ".concat(FINALIZE_STL_JOB_NAME).concat(" job running"));

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
                    LOG.warn("Unable to parse billing date", e);
                }

                if (MeterProcessType.ADJUSTED.name().equals(type)) {
                    boolean finalBased = "FINAL".equals(taskRunDto.getBaseType());

                    if (batchJobAdjRunRepository.countByGroupIdAndBillingPeriodStartAndBillingPeriodEnd(groupId.toString(), baseStartDate, baseEndDate) < 1) {
                        BatchJobAdjRun batchJobAdjRun = new BatchJobAdjRun();
                        batchJobAdjRun.setBillingPeriodStart(baseStartDate);
                        batchJobAdjRun.setBillingPeriodEnd(baseEndDate);
                        batchJobAdjRun.setGroupId(groupId.toString());
                        batchJobAdjRun.setJobId(taskRunDto.getParentJob());
                        batchJobAdjRun.setMeterProcessType(MeterProcessType.ADJUSTED);
                        batchJobAdjRunRepository.save(batchJobAdjRun);
                    }

                    if (finalBased) {
                        properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyAdjustedStlAmtsMtrFinCalculation")));
                    } else {
                        properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyAdjustedStlAmtsMtrAdjCalculation")));
                    }
                } else if (MeterProcessType.PRELIMINARY.name().equals(type) || MeterProcessType.PRELIM.name().equals(type)) {
                    properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyPrelimStlAmtsCalculation")));
                } else if (MeterProcessType.FINAL.name().equals(type)) {
                    if (batchJobAdjRunRepository.countByGroupIdAndBillingPeriodStartAndBillingPeriodEnd(groupId.toString(), baseStartDate, baseEndDate) < 1) {
                        BatchJobAdjRun batchJobAdjRun = new BatchJobAdjRun();
                        batchJobAdjRun.setBillingPeriodStart(baseStartDate);
                        batchJobAdjRun.setBillingPeriodEnd(baseEndDate);
                        batchJobAdjRun.setGroupId(taskRunDto.getGroupId());
                        batchJobAdjRun.setJobId(taskRunDto.getParentJob());
                        batchJobAdjRun.setMeterProcessType(MeterProcessType.FINAL);
                        batchJobAdjRunRepository.save(batchJobAdjRun);
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
        } else if (FINALIZE_STL_JOB_NAME.equals(taskRunDto.getJobName())) {

            Preconditions.checkState(batchJobRunLockRepository.countByJobNameAndLockedIsTrue(COMPUTE_STL_JOB_NAME) == 0,
                    "There is an existing ".concat(COMPUTE_STL_JOB_NAME).concat(" job running"));

            String type = taskRunDto.getMeterProcessType();

            BatchJobAdjRun batchJobAdjRun = batchJobAdjRunRepository.findByGroupId(taskRunDto.getGroupId());
            if (batchJobAdjRun != null) {
                type = batchJobAdjRun.getMeterProcessType().name();
            }

            if (type == null) {
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("dailyStlAmtsTagging")));
            } else if (MeterProcessType.ADJUSTED.name().equals(type)) {
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyAdjustedStlAmtsTagging")));
            } else if (MeterProcessType.PRELIMINARY.name().equals(type) || "PRELIM".equals(type)) {
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyPrelimStlAmtsTagging")));
            } else if (MeterProcessType.FINAL.name().equals(type)) {
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyFinalStlAmtsTagging")));
            }
            arguments.add(concatKeyValue(PROCESS_TYPE, type));
            arguments.add(concatKeyValue(START_DATE, taskRunDto.getStartDate(), "date"));
            arguments.add(concatKeyValue(END_DATE, taskRunDto.getEndDate(), "date"));
            jobName = "crss-settlement-task-calculation";

        } else if (COMPUTE_GMRVAT_MFEE_JOB_NAME.equals(taskRunDto.getJobName())) {

            Preconditions.checkState(batchJobRunLockRepository.countByJobNameAndLockedIsTrue(FINALIZE_GMRVAT_MFEE_JOB_NAME) == 0,
                    "There is an existing ".concat(FINALIZE_GMRVAT_MFEE_JOB_NAME).concat(" job running"));

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

        } else if (FINALIZE_GMRVAT_MFEE_JOB_NAME.equals(taskRunDto.getJobName())) {

            Preconditions.checkState(batchJobRunLockRepository.countByJobNameAndLockedIsTrue(COMPUTE_GMRVAT_MFEE_JOB_NAME) == 0,
                    "There is an existing ".concat(COMPUTE_GMRVAT_MFEE_JOB_NAME).concat(" job running"));

            String type = taskRunDto.getMeterProcessType();

            BatchJobAdjRun batchJobAdjRun = batchJobAdjRunRepository.findByGroupId(taskRunDto.getGroupId());
            if (batchJobAdjRun != null) {
                type = batchJobAdjRun.getMeterProcessType().name();
            }

            Preconditions.checkState(type != null, "Cannot run GMR/Market Fee tagging job on daily basis.");
            if (MeterProcessType.ADJUSTED.name().equals(type)) {
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyAdjustedGmrVatMfeeTagging")));
            } else if (MeterProcessType.PRELIMINARY.name().equals(type) || MeterProcessType.PRELIM.name().equals(type)) {
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyPrelimGmrVatMfeeTagging")));
            } else if (MeterProcessType.FINAL.name().equals(type)) {
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyFinalGmrVatMfeeTagging")));
            }

            arguments.add(concatKeyValue(PROCESS_TYPE, type));
            arguments.add(concatKeyValue(START_DATE, taskRunDto.getStartDate(), "date"));
            arguments.add(concatKeyValue(END_DATE, taskRunDto.getEndDate(), "date"));
            jobName = "crss-settlement-task-calculation";

        } else if (GENERATE_INVOICE_STL_JOB_NAME.equals(taskRunDto.getJobName())) {

            Preconditions.checkState(batchJobRunLockRepository.countByJobNameAndLockedIsTrue(FINALIZE_GMRVAT_MFEE_JOB_NAME) == 0,
                    "There is an existing ".concat(FINALIZE_GMRVAT_MFEE_JOB_NAME).concat(" job running"));

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
        }
        LOG.debug("Running job name={}, properties={}, arguments={}", taskRunDto.getJobName(), properties, arguments);

        if (jobName != null) {
            LOG.debug("Running job name={}, properties={}, arguments={}", taskRunDto.getJobName(), properties, arguments);
            launchJob(jobName, properties, arguments);
            lockJob(taskRunDto);
        }
    }

    private void saveAMSadditionalParams(final Long runId, final TaskRunDto taskRunDto) {
        LOG.debug("Saving additional AMS params. TaskRunDto: {}", taskRunDto);
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
            LOG.error("Error parsing additional batch job params for AMS: {}", e);
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
                            LOG.warn("Step name {} not existing in current mapping.", stepName);
                        }
                    });
        }
        dto.setRunningSteps(runningTasks);
        dto.setLatestJobExecStartDate(jobExecution.getStartTime());
        dto.setLatestJobExecEndDate(jobExecution.getEndTime());
    }
}
