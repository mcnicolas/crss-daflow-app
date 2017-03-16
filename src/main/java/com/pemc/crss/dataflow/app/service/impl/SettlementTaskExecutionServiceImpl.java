package com.pemc.crss.dataflow.app.service.impl;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.pemc.crss.dataflow.app.dto.PartialCalculationDto;
import com.pemc.crss.dataflow.app.dto.StlJobGroupDto;
import com.pemc.crss.dataflow.app.dto.StlTaskExecutionDto;
import com.pemc.crss.dataflow.app.dto.TaskRunDto;
import com.pemc.crss.shared.commons.reference.MeterProcessType;
import com.pemc.crss.shared.commons.util.DateUtil;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobAddtlParams;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobAdjRun;
import com.pemc.crss.shared.core.dataflow.entity.LatestAdjustmentLock;
import com.pemc.crss.shared.core.dataflow.entity.RunningAdjustmentLock;
import com.pemc.crss.shared.core.dataflow.repository.BatchJobAddtlParamsRepository;
import com.pemc.crss.shared.core.dataflow.repository.BatchJobAdjRunRepository;
import com.pemc.crss.shared.core.dataflow.repository.LatestAdjustmentLockRepository;
import com.pemc.crss.shared.core.dataflow.repository.RunningAdjustmentLockRepository;
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

@Service("settlementTaskExecutionService")
@Transactional(readOnly = true, value = "transactionManager")
public class SettlementTaskExecutionServiceImpl extends AbstractTaskExecutionService {

    private static final Logger LOG = LoggerFactory.getLogger(SettlementTaskExecutionServiceImpl.class);

    // Job names
    private static final String STL_READY_JOB_NAME = "processStlReady";
    private static final String COMPUTE_STL_JOB_NAME = "computeSettlementStlAmts";
    private static final String FINALIZE_STL_JOB_NAME = "tasAsOutputReadyStlAmts";
    private static final String COMPUTE_GMRVAT_MFEE_JOB_NAME = "computeSettlementGmrVatMfee";
    private static final String FINALIZE_GMRVAT_MFEE_JOB_NAME = "tasAsOutputReadyGmrVatMfee";
    private static final String GENERATE_INVOICE_STL_JOB_NAME = "generateInvoiceSettlement";

    private static final String AMS_INVOICE_DATE = "amsInvoiceDate";
    private static final String AMS_DUE_DATE = "amsDueDate";
    private static final String AMS_REMARKS_INV = "amsRemarksInv";
    private static final String AMS_REMARKS_MF = "amsRemarksMf";

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
                    JobExecution jobExecution = getJobExecutions(jobInstance).iterator().next();
                    Long jobId = jobExecution.getJobId();
                    BatchStatus jobStatus = jobExecution.getStatus();
                    LOG.debug("Processing processStlReady jobId {}", jobId);

                    String parentId = jobInstance.getJobName().split("-")[1];
                    if (StringUtils.isEmpty(parentId)) {
                        LOG.warn("Parent id not appended for job instance id {}. Setting parent as self..", jobInstance.getId());
                        parentId = String.valueOf(jobInstance.getInstanceId());
                    }
                    JobParameters jobParameters = jobExecution.getJobParameters();
                    Date startDate = jobParameters.getDate(START_DATE);
                    Date endDate = jobParameters.getDate(END_DATE);
                    LOG.debug("Date Range -> from {} to {}", startDate, endDate);

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
                    SortedSet<LocalDate> remainingDates = createRange(startDate, endDate);
                    Iterator<JobInstance> calcStlIterator = calcStlJobInstances.iterator();
                    while (calcStlIterator.hasNext()) {
                        JobInstance calcStlJobInstance = calcStlIterator.next();
                        String calcStlJobName = calcStlJobInstance.getJobName();
                        JobExecution calcJobExecution = getJobExecutions(calcStlJobInstance).iterator().next();
                        BatchStatus currentStatus = calcJobExecution.getStatus();
                        JobParameters calcJobParameters = calcJobExecution.getJobParameters();
                        Long groupId = calcJobParameters.getLong(GROUP_ID);
                        Date calcStartDate = calcJobParameters.getDate(START_DATE);
                        Date calcEndDate = calcJobParameters.getDate(END_DATE);

                        StlJobGroupDto stlJobGroupDto;
                        if (stlJobGroupDtoMap.get(groupId) == null) {
                            stlJobGroupDto = new StlJobGroupDto();
                            stlJobGroupDto.setRemainingDates(new TreeSet<>(remainingDates));
                        } else {
                            stlJobGroupDto = stlJobGroupDtoMap.get(groupId);
                        }
//                        StlJobGroupDto stlJobGroupDto = stlJobGroupDtoMap.getOrDefault(groupId, new StlJobGroupDto());
                        stlJobGroupDto.setCurrentlyRunning(groupId.equals(lockedGroupId));
                        stlJobGroupDto.setLatestAdjustment(jobId.equals(latestGroupId));
                        stlJobGroupDto.setHeader(jobId.equals(groupId));
                        stlJobGroupDto.setRemainingDates(remainingDates);

                        List<PartialCalculationDto> partialCalculationDtoList = stlJobGroupDto.getPartialCalculationDtos();
                        if (partialCalculationDtoList == null) {
                            partialCalculationDtoList = Lists.newArrayList();
                            stlJobGroupDto.setRunStartDateTime(calcStartDate);
                            stlJobGroupDto.setRunEndDateTime(calcEndDate);
                        }
                        PartialCalculationDto dto = new PartialCalculationDto();
                        dto.setStatus(convertStatus(currentStatus, calcStatusSuffix));
                        dto.setBillingStart(calcStartDate);
                        dto.setBillingEnd(calcEndDate);
                        dto.setRunDate(calcJobExecution.getStartTime());
                        dto.setRunEndDate(calcJobExecution.getEndTime());
                        partialCalculationDtoList.add(dto);

                        removeDateRangeFrom(stlJobGroupDto.getRemainingDates(), calcStartDate, calcEndDate);

                        stlJobGroupDto.setPartialCalculationDtos(partialCalculationDtoList);
                        stlJobGroupDto.setStatus(convertStatus(jobStatus, calcStatusSuffix));
                        stlJobGroupDto.setGroupId(groupId);
                        stlJobGroupDtoMap.put(groupId, stlJobGroupDto);

                        if (stlJobGroupDto.isHeader()) {
                            parentStlJobGroupDto = stlJobGroupDto;
                        }
                    }
                    /* CALCULATION END */

                    /* CALCULATION TAGGING START */
                    String calcTagStatusSuffix = "CALCULATION-TAGGING";
                    String calcTagQueryString = FINALIZE_STL_JOB_NAME.concat("*-").concat(parentId).concat("-*");
                    List<JobInstance> calcTagStlJobInstances = jobExplorer.findJobInstancesByJobName(calcTagQueryString, 0, Integer.MAX_VALUE);
                    Iterator<JobInstance> calcTagStlIterator = calcTagStlJobInstances.iterator();
                    while (calcTagStlIterator.hasNext()) {
                        JobInstance calcTagStlJobInstance = calcTagStlIterator.next();
                        String calcTagStlJobName = calcTagStlJobInstance.getJobName();
                        JobExecution calcTagJobExecution = getJobExecutions(calcTagStlJobInstance).iterator().next();
                        JobParameters calcTagJobParameters = calcTagJobExecution.getJobParameters();
                        Long groupId = calcTagJobParameters.getLong(GROUP_ID);
                        StlJobGroupDto stlJobGroupDto = stlJobGroupDtoMap.getOrDefault(groupId, new StlJobGroupDto());
                        stlJobGroupDto.setCurrentlyRunning(groupId.equals(lockedGroupId));
                        stlJobGroupDto.setLatestAdjustment(jobId.equals(latestGroupId));
                        stlJobGroupDto.setHeader(jobId.equals(groupId));
                        BatchStatus currentStatus = calcTagJobExecution.getStatus();
                        stlJobGroupDto.setStatus(convertStatus(currentStatus, calcTagStatusSuffix));
                        stlJobGroupDto.setStlAmtTaggingStatus(currentStatus);
                        stlJobGroupDto.setGroupId(groupId);
                        stlJobGroupDtoMap.put(groupId, stlJobGroupDto);
                        if (stlJobGroupDto.isHeader()) {
                            parentStlJobGroupDto = stlJobGroupDto;
                        }
                    }
                    /* CALCULATION TAGGING END */

                    /* CALCULATION GMR START */
                    String calcGmrStatusSuffix = "CALCULATION-GMR";
                    String calcGmrQueryString = COMPUTE_GMRVAT_MFEE_JOB_NAME.concat("*-").concat(parentId).concat("-*");
                    List<JobInstance> calcGmrStlJobInstances = jobExplorer.findJobInstancesByJobName(calcGmrQueryString, 0, Integer.MAX_VALUE);
                    Iterator<JobInstance> calcGmrStlIterator = calcGmrStlJobInstances.iterator();
                    while (calcGmrStlIterator.hasNext()) {
                        JobInstance calcGmrStlJobInstance = calcGmrStlIterator.next();
                        String calcGmrStlJobName = calcGmrStlJobInstance.getJobName();
                        JobExecution calcGmrJobExecution = getJobExecutions(calcGmrStlJobInstance).iterator().next();
                        JobParameters calcGmrJobParameters = calcGmrJobExecution.getJobParameters();
                        Long groupId = calcGmrJobParameters.getLong(GROUP_ID);
                        StlJobGroupDto stlJobGroupDto = stlJobGroupDtoMap.getOrDefault(groupId, new StlJobGroupDto());
                        stlJobGroupDto.setCurrentlyRunning(groupId.equals(lockedGroupId));
                        stlJobGroupDto.setLatestAdjustment(jobId.equals(latestGroupId));
                        stlJobGroupDto.setHeader(jobId.equals(groupId));
                        BatchStatus currentStatus = calcGmrJobExecution.getStatus();
                        stlJobGroupDto.setStatus(convertStatus(currentStatus, calcGmrStatusSuffix));
                        stlJobGroupDto.setGmrVatMFeeCalculationStatus(currentStatus);
                        stlJobGroupDto.setGroupId(groupId);
                        stlJobGroupDtoMap.put(groupId, stlJobGroupDto);
                        if (stlJobGroupDto.isHeader()) {
                            parentStlJobGroupDto = stlJobGroupDto;
                        }
                    }
                    /* CALCULATION GMR END */

                    /* TAGGING GMR START */
                    String tagGmrStatusSuffix = "TAGGING-GMR";
                    String tagGmrQueryString = FINALIZE_GMRVAT_MFEE_JOB_NAME.concat("*-").concat(parentId).concat("-*");
                    List<JobInstance> tagGmrStlJobInstances = jobExplorer.findJobInstancesByJobName(tagGmrQueryString, 0, Integer.MAX_VALUE);
                    Iterator<JobInstance> tagGmrStlIterator = tagGmrStlJobInstances.iterator();
                    while (tagGmrStlIterator.hasNext()) {
                        JobInstance tagGmrStlJobInstance = tagGmrStlIterator.next();
                        String tagGmrStlJobName = tagGmrStlJobInstance.getJobName();
                        JobExecution tagGmrJobExecution = getJobExecutions(tagGmrStlJobInstance).iterator().next();
                        JobParameters tagGmrJobParameters = tagGmrJobExecution.getJobParameters();
                        Long groupId = tagGmrJobParameters.getLong(GROUP_ID);
                        StlJobGroupDto stlJobGroupDto = stlJobGroupDtoMap.getOrDefault(groupId, new StlJobGroupDto());
                        stlJobGroupDto.setCurrentlyRunning(groupId.equals(lockedGroupId));
                        stlJobGroupDto.setLatestAdjustment(jobId.equals(latestGroupId));
                        stlJobGroupDto.setHeader(jobId.equals(groupId));
                        BatchStatus currentStatus = tagGmrJobExecution.getStatus();
                        stlJobGroupDto.setStatus(convertStatus(currentStatus, tagGmrStatusSuffix));
                        stlJobGroupDto.setGmrVatMFeeTaggingStatus(currentStatus);
                        stlJobGroupDto.setGroupId(groupId);
                        stlJobGroupDtoMap.put(groupId, stlJobGroupDto);
                        if (stlJobGroupDto.isHeader()) {
                            parentStlJobGroupDto = stlJobGroupDto;
                        }
                    }
                    /* TAGGING GMR END */

                    /* OUTPUT GENERATION START */
                    String generationStatusSuffix = "TAGGING-GMR";
                    String generationQueryString = GENERATE_INVOICE_STL_JOB_NAME.concat("*-").concat(parentId).concat("-*");
                    List<JobInstance> generationStlJobInstances = jobExplorer.findJobInstancesByJobName(generationQueryString, 0, Integer.MAX_VALUE);
                    Iterator<JobInstance> generationStlIterator = generationStlJobInstances.iterator();
                    while (generationStlIterator.hasNext()) {
                        JobInstance generationStlJobInstance = generationStlIterator.next();
                        String generationStlJobName = generationStlJobInstance.getJobName();
                        JobExecution generationJobExecution = getJobExecutions(generationStlJobInstance).iterator().next();
                        JobParameters generationJobParameters = generationJobExecution.getJobParameters();
                        Long groupId = generationJobParameters.getLong(GROUP_ID);
                        StlJobGroupDto stlJobGroupDto = stlJobGroupDtoMap.getOrDefault(groupId, new StlJobGroupDto());
                        stlJobGroupDto.setCurrentlyRunning(groupId.equals(lockedGroupId));
                        stlJobGroupDto.setLatestAdjustment(jobId.equals(latestGroupId));
                        stlJobGroupDto.setHeader(jobId.equals(groupId));
                        BatchStatus currentStatus = generationJobExecution.getStatus();
                        stlJobGroupDto.setStatus(convertStatus(currentStatus, generationStatusSuffix));
                        stlJobGroupDto.setInvoiceGenerationStatus(currentStatus);
                        stlJobGroupDto.setGroupId(groupId);
                        stlJobGroupDtoMap.put(groupId, stlJobGroupDto);
                        if (stlJobGroupDto.isHeader()) {
                            parentStlJobGroupDto = stlJobGroupDto;
                        }
                    }
                    /* OUTPUT GENERATION END */

                    taskExecutionDto.setStlJobGroupDtoMap(stlJobGroupDtoMap);
                    taskExecutionDto.setParentStlJobGroupDto(parentStlJobGroupDto);
                    return taskExecutionDto;

                }).collect(toList());

        return new PageImpl<>(taskExecutionDtos, pageable, taskExecutionDtos.size());
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
        Long timestamp = System.currentTimeMillis();

        arguments.add(concatKeyValue(PARENT_JOB, taskRunDto.getParentJob(), "long"));
        arguments.add(concatKeyValue(RUN_ID, String.valueOf(timestamp), "long"));

        Long groupId = taskRunDto.isNewGroup() ? timestamp : Long.parseLong(taskRunDto.getGroupId());
        arguments.add(concatKeyValue(GROUP_ID, groupId.toString(), "long"));

        Date start = null;
        Date end = null;

        if (COMPUTE_STL_JOB_NAME.equals(taskRunDto.getJobName())) {
            String type = taskRunDto.getMeterProcessType();
            if (type == null) {
                type = PROCESS_TYPE_DAILY;
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("dailyStlAmtsCalculation")));
                arguments.add(concatKeyValue(START_DATE, taskRunDto.getTradingDate(), "date"));
            } else {
                if (MeterProcessType.ADJUSTED.name().equals(type)) {
                    boolean finalBased = "FINAL".equals(taskRunDto.getBaseType());
                    LocalDateTime baseStartDate = null;
                    LocalDateTime baseEndDate = null;
                    try {
                        baseStartDate = DateUtil.getStartRangeDate(taskRunDto.getBaseStartDate());
                        baseEndDate = DateUtil.getStartRangeDate(taskRunDto.getBaseEndDate());
                    } catch (ParseException e) {
                        LOG.warn("Unable to parse billing date", e);
                    }

                    if (taskRunDto.isHeader() && batchJobAdjRunRepository.countByGroupIdAndBillingPeriodStartAndBillingPeriodEnd(taskRunDto.getGroupId(), baseStartDate, baseEndDate) < 1) {
                        BatchJobAdjRun first = new BatchJobAdjRun();
                        first.setBillingPeriodStart(baseStartDate);
                        first.setBillingPeriodEnd(baseEndDate);
                        first.setGroupId(taskRunDto.getGroupId());
                        first.setJobId(taskRunDto.getParentJob());
                        first.setMeterProcessType(finalBased ? MeterProcessType.FINAL : MeterProcessType.ADJUSTED);
                        batchJobAdjRunRepository.save(first);
                    }
                    if (batchJobAdjRunRepository.countByGroupIdAndBillingPeriodStartAndBillingPeriodEnd(groupId.toString(), baseStartDate, baseEndDate) < 1) {
                        BatchJobAdjRun latest = new BatchJobAdjRun();
                        latest.setBillingPeriodStart(baseStartDate);
                        latest.setBillingPeriodEnd(baseEndDate);
                        latest.setGroupId(groupId.toString());
                        latest.setJobId(taskRunDto.getParentJob());
                        latest.setMeterProcessType(finalBased ? MeterProcessType.FINAL : MeterProcessType.ADJUSTED);
                        batchJobAdjRunRepository.save(latest);
                    }

                    if (finalBased) {
                        properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyAdjustedStlAmtsMtrFinCalculation")));
                    } else {
                        properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyAdjustedStlAmtsMtrAdjCalculation")));
                    }
                } else if (MeterProcessType.PRELIMINARY.name().equals(type) || MeterProcessType.PRELIM.name().equals(type)) {
                    properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyPrelimStlAmtsCalculation")));
                } else if (MeterProcessType.FINAL.name().equals(type)) {
                    properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyFinalStlAmtsCalculation")));
                }
                arguments.add(concatKeyValue(START_DATE, taskRunDto.getStartDate(), "date"));
                arguments.add(concatKeyValue(END_DATE, taskRunDto.getEndDate(), "date"));
            }
            arguments.add(concatKeyValue(PROCESS_TYPE, type));
//            taskRunDto.setJobName(COMPUTE_STL_JOB_NAME);
            jobName = "crss-settlement-task-calculation";
        } else if (FINALIZE_STL_JOB_NAME.equals(taskRunDto.getJobName())) {
            String type = taskRunDto.getMeterProcessType();
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
//            taskRunDto.setJobName(FINALIZE_STL_JOB_NAME);
            jobName = "crss-settlement-task-calculation";
        } else if (COMPUTE_GMRVAT_MFEE_JOB_NAME.equals(taskRunDto.getJobName())) {
            String type = taskRunDto.getMeterProcessType();
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
//            taskRunDto.setJobName(COMPUTE_GMRVAT_MFEE_JOB_NAME);
            jobName = "crss-settlement-task-calculation";
        } else if (FINALIZE_GMRVAT_MFEE_JOB_NAME.equals(taskRunDto.getJobName())) {
            String type = taskRunDto.getMeterProcessType();
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
//            taskRunDto.setJobName(FINALIZE_GMRVAT_MFEE_JOB_NAME);
            jobName = "crss-settlement-task-calculation";
        } else if (GENERATE_INVOICE_STL_JOB_NAME.equals(taskRunDto.getJobName())) {
            String type = taskRunDto.getMeterProcessType();
            if (MeterProcessType.ADJUSTED.name().equals(type)) {
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyAdjustedInvoiceGeneration")));
            } else if (MeterProcessType.PRELIMINARY.name().equals(type) || "PRELIM".equals(type)) {
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyPrelimInvoiceGeneration")));
            } else if (MeterProcessType.FINAL.name().equals(type)) {
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyFinalInvoiceGeneration")));
                final Long runId = System.currentTimeMillis();

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
                }
            }
            arguments.add(concatKeyValue(PROCESS_TYPE, type));
            arguments.add(concatKeyValue(START_DATE, taskRunDto.getStartDate(), "date"));
            arguments.add(concatKeyValue(END_DATE, taskRunDto.getEndDate(), "date"));
//            taskRunDto.setJobName(GENERATE_INVOICE_STL_JOB_NAME);
            jobName = "crss-settlement-task-invoice-generation";
        }
        LOG.debug("Running job name={}, properties={}, arguments={}", taskRunDto.getJobName(), properties, arguments);

        if ((MeterProcessType.ADJUSTED.name().equals(taskRunDto.getMeterProcessType())
                || MeterProcessType.ADJUSTED.name().equals(taskRunDto.getMeterProcessType()))
                && (runningAdjustmentLockRepository.lockJob(Long.parseLong(taskRunDto.getParentJob()), groupId, start, end) == 0)) {
            RunningAdjustmentLock lock = new RunningAdjustmentLock();
            lock.setLocked(true);
            lock.setGroupId(groupId);
            lock.setParentJobId(Long.parseLong(taskRunDto.getParentJob()));
            lock.setStartDate(start);
            lock.setEndDate(end);
            runningAdjustmentLockRepository.save(lock);
        }

        if (jobName != null) {
            LOG.debug("Running job name={}, properties={}, arguments={}", taskRunDto.getJobName(), properties, arguments);
            doLaunchAndLockJob(taskRunDto, jobName, properties, arguments);
        }
    }

    private SortedSet<LocalDate> createRange(Date start, Date end) {
        if (start == null || end == null) {
            return null;
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
}
