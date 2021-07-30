package com.pemc.crss.dataflow.app.service.impl;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.pemc.crss.dataflow.app.dto.*;
import com.pemc.crss.dataflow.app.dto.parent.GroupTaskExecutionDto;
import com.pemc.crss.dataflow.app.dto.parent.StubTaskExecutionDto;
import com.pemc.crss.dataflow.app.exception.LaunchJobException;
import com.pemc.crss.dataflow.app.support.PageableRequest;
import com.pemc.crss.shared.commons.reference.MeterProcessType;
import com.pemc.crss.shared.commons.reference.PricingCondition;
import com.pemc.crss.shared.commons.reference.StlAddtlCompStepUtil;
import com.pemc.crss.shared.commons.util.DateUtil;
import com.pemc.crss.shared.commons.util.TaskUtil;
import com.pemc.crss.shared.core.dataflow.entity.AddtlCompParams;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobAddtlParams;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobAdjRun;
import com.pemc.crss.shared.core.dataflow.reference.AddtlCompJobName;
import com.pemc.crss.shared.core.dataflow.reference.AddtlCompJobProfile;
import com.pemc.crss.shared.core.dataflow.reference.StlCalculationType;
import com.pemc.crss.shared.core.dataflow.repository.AddtlCompParamsRepository;
import com.pemc.crss.shared.core.dataflow.repository.BatchJobAdjRunRepository;
import com.pemc.crss.shared.core.dataflow.repository.SettlementJobLockRepository;
import com.pemc.crss.shared.core.dataflow.repository.ViewTxnOutputAddtlCompensationRepository;
import com.pemc.crss.shared.core.dataflow.service.BatchJobAddtlParamsService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameters;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static com.pemc.crss.shared.commons.util.TaskUtil.*;
import static com.pemc.crss.shared.core.dataflow.reference.AddtlCompJobName.*;
import static com.pemc.crss.shared.core.dataflow.reference.AddtlCompJobProfile.*;

@Slf4j
@Service("addtlCompensationExecutionService")
@Transactional
public class AddtlCompensationExecutionServiceImpl extends AbstractTaskExecutionService {

    private static final String ADDTL_COMP_TASK_NAME = "crss-settlement-task-calculation-addtlcomp";
    private static final String ADDTL_COMP_FILE_GEN_TASK_NAME = "crss-settlement-task-file-gen-addtlcomp";

    private static final String AC_FILE_GEN_FOLDERNAME = "AC_FILE_GEN_FOLDERNAME";
    private static final List<String> AC_CALC_STEP_LIST = Arrays.asList(StlAddtlCompStepUtil.CALC_ADDTL_COMP_STEP,
            StlAddtlCompStepUtil.CALC_ADDTL_COMP_ALLOC_STEP, StlAddtlCompStepUtil.CALC_ADDTL_COMP_VAT_STEP,
            StlAddtlCompStepUtil.CALC_ADDTL_COMP_VAT_ALLOC_STEP);
    private static final List<String> AC_CALC_GMR_VAT_STEP_LIST = Collections.singletonList(StlAddtlCompStepUtil.CALC_ADDTL_COMP_GMR_STEP);
    //TODO: add finalize ac steps
    private static final List<String> AC_FINALIZE_STEP_LIST = Collections.emptyList();

    private static final long ADDTL_COMP_MONTH_VALIDITY = 24;

    @Autowired
    private BatchJobAdjRunRepository batchJobAdjRunRepository;

    @Autowired
    private AddtlCompParamsRepository addtlCompParamsRepository;

    @Autowired
    private BatchJobAddtlParamsService batchJobAddtlParamsService;

    @Autowired
    private SettlementJobLockRepository settlementJobLockRepository;

    @Autowired
    private ViewTxnOutputAddtlCompensationRepository viewTxnOutputAddtlCompensationRepository;

    @Value("${cfg.ams.remarks-max-length}")
    private Long maxRemarksLength;

    @Value("${cfg.ams.invoice-date-restrict-days}")
    private Long amsInvoiceDateRestrictDays;

    @Value("${cfg.ams.due-date-restrict-days}")
    private Long amsDueDateRestrictDays;

    @Value("${cfg.ams.addtl-comp-max-number-installment}")
    private Long addtlCompMaxNoInstallments;

    @Override
    public Page<? extends BaseTaskExecutionDto> findJobInstances(Pageable pageable, String type, String status, String mode, String runStartDate, String tradingStartDate, String tradingEndDate, String username) {
        return null;
    }

    @Override
    public Page<? extends BaseTaskExecutionDto> findJobInstances(PageableRequest pageableRequest) {
        final Pageable pageable = pageableRequest.getPageable();
        final Map<String, String> mapParams = pageableRequest.getMapParams();

        Long totalSize = dataFlowJdbcJobExecutionDao.countDistinctAddtlCompJobInstances(mapParams);
        List<AddtlCompensationExecutionDto> addtlCompensationExecutionDtoList = getAddtlCompensationExecutionDtoList(pageable, mapParams);

        return new PageImpl<>(addtlCompensationExecutionDtoList, pageable, totalSize);
    }

    public List<AddtlCompensationExecutionDto> getAddtlCompensationExecutionDtoList(Pageable pageable, Map<String, String> mapParams) {
        log.info("getAddtlCompensationExecutionDtoList({}, {})", pageable, mapParams);
        String status = Optional.ofNullable(mapParams.get("status")).orElse("ALL");

        return dataFlowJdbcJobExecutionDao
                .findDistinctAddtlCompJobInstances(pageable.getOffset(), pageable.getPageSize(), mapParams)
                .stream()
                .map(distinctAddtlCompDto -> {
                    distinctAddtlCompDto.setMaxAmsRemarksLength(maxRemarksLength);
                    distinctAddtlCompDto.setAmsInvoiceDateRestrictDays(amsInvoiceDateRestrictDays);
                    distinctAddtlCompDto.setAmsDueDateRestrictDays(amsDueDateRestrictDays);
                    distinctAddtlCompDto.setAddtlCompMaxNoInstallments(addtlCompMaxNoInstallments);
                    AddtlCompensationExecutionDto addtlCompensationExecutionDto = new AddtlCompensationExecutionDto();
                    addtlCompensationExecutionDto.setDistinctAddtlCompDto(distinctAddtlCompDto);
                    List<AddtlCompensationExecDetailsDto> addtlCompensationExecDetailsDtos = Lists.newArrayList();

                    dataFlowJdbcJobExecutionDao
                            .findAddtlCompJobInstances(0, Integer.MAX_VALUE, distinctAddtlCompDto)
                            .forEach(jobInstance -> {
                                List<JobExecution> jobExecutionList = getJobExecutions(jobInstance);
                                if (!jobExecutionList.isEmpty()) {
                                    JobExecution jobExecution = jobExecutionList.get(0);
                                    JobParameters parameters = jobExecution.getJobParameters();

                                    String groupId = parameters.getString(GROUP_ID);

                                    String acBillingId = batchJobAddtlParamsService.getBatchJobAddtlParamsStringVal(parameters.getLong(RUN_ID), AC_BILLING_ID);
                                    String acMtn = batchJobAddtlParamsService.getBatchJobAddtlParamsStringVal(parameters.getLong(RUN_ID), AC_MTN);
                                    Double acApprovedRate = batchJobAddtlParamsService.getBatchJobAddtlParamsDoubleVal(parameters.getLong(RUN_ID), AC_APPROVED_RATE);

                                    AddtlCompensationExecDetailsDto addtlCompensationExecDetailsDto = new AddtlCompensationExecDetailsDto();
                                    Long runId = parameters.getLong(RUN_ID);

                                    addtlCompensationExecDetailsDto.setRunId(runId);
                                    addtlCompensationExecDetailsDto.setBillingId(acBillingId);
                                    addtlCompensationExecDetailsDto.setMtn(acMtn);
                                    addtlCompensationExecDetailsDto.setApprovedRate(acApprovedRate != null ? BigDecimal.valueOf(acApprovedRate) : BigDecimal.ZERO);
                                    addtlCompensationExecDetailsDto.setStatus(jobExecution.getStatus().name());

                                    List<TaskSummaryDto> taskSummaryDtos = showSummary(jobExecution, AC_CALC_STEP_LIST).stream().map(taskSummaryDto -> {
                                        int processedCount = viewTxnOutputAddtlCompensationRepository.findByAcGroupIdAndAcJobId(groupId, runId).size();
                                        taskSummaryDto.setProcessedCount(processedCount);
                                        return taskSummaryDto;
                                    }).collect(Collectors.toList());

                                    addtlCompensationExecDetailsDto.setTaskSummaryList(taskSummaryDtos);
                                    addtlCompensationExecDetailsDto.setRunningSteps(getProgress(jobExecution));
                                    addtlCompensationExecDetailsDto.setRunStartDate(DateUtil.convertToLocalDateTime(jobExecution.getStartTime()));
                                    addtlCompensationExecDetailsDto.setRunEndDate(DateUtil.convertToLocalDateTime(jobExecution.getEndTime()));

                                    // add run id for completed ac runs
                                    if (Objects.equals(addtlCompensationExecDetailsDto.getStatus(), BatchStatus.COMPLETED.toString())) {
                                        distinctAddtlCompDto.getSuccessfullAcRuns().add(addtlCompensationExecDetailsDto.getRunId());
                                    }

                                    Optional.ofNullable(getLatestCalcGmrVatAcJob(groupId)).ifPresent(calcGmrVatJobExec -> {
                                        BatchStatus jobStatus = calcGmrVatJobExec.getStatus();
                                        distinctAddtlCompDto.setCalcGmrVatStatus(jobStatus);
                                        distinctAddtlCompDto.setCalcGmrVatAcRunSummary(
                                                showSummary(calcGmrVatJobExec, AC_CALC_GMR_VAT_STEP_LIST));
                                        distinctAddtlCompDto.setCalcGmrVatRunningSteps(getProgress(calcGmrVatJobExec));
                                        distinctAddtlCompDto.setCalcGmrStartDate(DateUtil.convertToLocalDateTime(calcGmrVatJobExec.getStartTime()));
                                        distinctAddtlCompDto.setCalcGmrEndDate(DateUtil.convertToLocalDateTime(calcGmrVatJobExec.getEndTime()));
                                        distinctAddtlCompDto.setCurrentStatus(convertStatus(jobStatus, "CALCULATE-GMR"));
                                    });

                                    Optional.ofNullable(getLatestFinalizeAcJob(groupId)).ifPresent(finalizeJobExec -> {
                                        BatchStatus jobStatus = finalizeJobExec.getStatus();
                                        distinctAddtlCompDto.setTaggingStatus(jobStatus);
                                        distinctAddtlCompDto.setFinalizeAcRunSummary(
                                                showSummary(finalizeJobExec, AC_FINALIZE_STEP_LIST));
                                        distinctAddtlCompDto.setFinalizeRunningSteps(getProgress(finalizeJobExec));
                                        distinctAddtlCompDto.setCurrentStatus(convertStatus(jobStatus, "FINALIZE"));
                                        if (jobStatus == BatchStatus.COMPLETED) {
                                            distinctAddtlCompDto.setLocked(true);
                                        }
                                    });

                                    distinctAddtlCompDto.setGroupId(groupId);

                                    // do not include finalize ac jobs
                                    if (!jobInstance.getJobName().startsWith(AC_CALC_GMR_BASE_NAME)) {
                                        addtlCompensationExecDetailsDtos.add(addtlCompensationExecDetailsDto);
                                    }
                                }
                            });

                    if (distinctAddtlCompDto != null && distinctAddtlCompDto.getTaggingStatus() != null
                            && distinctAddtlCompDto.getTaggingStatus().equals(BatchStatus.COMPLETED)) {
                        // get generated AC files folder name
                        Optional<JobInstance> genFileJobInstanceOpt = jobExplorer.findJobInstancesByJobName(
                                AC_GEN_FILE + distinctAddtlCompDto.getGroupId(),
                                0, Integer.MAX_VALUE).stream().findFirst();

                        genFileJobInstanceOpt.ifPresent(jobInstance -> getJobExecutions(jobInstance)
                                .forEach(jobExecution -> {
                                    distinctAddtlCompDto.setGenFileStatus(jobExecution.getStatus());
                                    distinctAddtlCompDto.setGenFileEndTime(
                                            DateUtil.convertToString(jobExecution.getEndTime(), DateUtil.DEFAULT_DATETIME_FORMAT));

                                    Optional.ofNullable(jobExecution.getExecutionContext().get(AC_FILE_GEN_FOLDERNAME))
                                            .ifPresent(val -> distinctAddtlCompDto.setGenFileFolderName((String) val));

                                    // always get the steps of the latest generate files job execution
                                    distinctAddtlCompDto.setGenerateFileRunningSteps(getProgress(jobExecution));
                                })
                        );
                    }

                    addtlCompensationExecutionDto.setAddtlCompensationExecDetailsDtos(addtlCompensationExecDetailsDtos);
                    return addtlCompensationExecutionDto;
                })
                .collect(Collectors.toList());
    }

    @Override
    public void relaunchFailedJob(long jobId) throws URISyntaxException {

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
    public Page<? extends StubTaskExecutionDto> findJobInstancesByBillingPeriodAndProcessType(Pageable pageable, String billingPeriod, String processType, Long adjNo) {
        return null;
    }

    @Override
    public void launchJob(TaskRunDto taskRunDto) throws URISyntaxException, LaunchJobException {
        switch (taskRunDto.getJobName()) {
            case AddtlCompJobName.AC_CALC:
                launchAddtlCompensation(taskRunDto);
                break;
            case AddtlCompJobName.AC_CALC_GMR_BASE_NAME:
                calcGmrVatAc(taskRunDto);
                break;
            case AddtlCompJobName.AC_FINALIZE_BASE_NAME:
                finalizeAc(taskRunDto);
                break;
            case AddtlCompJobName.AC_GEN_FILE:
                generateFilesAc(taskRunDto);
                break;
            default:
                throw new RuntimeException("Job launch failed. Unhandled Job Name: " + taskRunDto.getJobName());
        }
    }

    private String buildGroupId(TaskRunDto taskRunDto) throws URISyntaxException {

        String groupId = (taskRunDto.getBillingStartDate() + taskRunDto.getBillingEndDate() + taskRunDto.getPricingCondition()
        ).replaceAll("-", "");

        List<JobInstance> finalizeJobInstances = dataFlowJdbcJobExecutionDao.findAddtlCompCompleteFinalizeInstances(0,
                Integer.MAX_VALUE, taskRunDto.getBillingStartDate(), taskRunDto.getBillingEndDate(), taskRunDto.getPricingCondition());

        if (org.apache.commons.collections4.CollectionUtils.isNotEmpty(finalizeJobInstances) &&
                finalizeJobInstances.size() > 0) {
            groupId = groupId.concat(String.valueOf(finalizeJobInstances.size()));
        }

        return groupId; // produces: YYYYMMDDYYYYMMDDPC ex: 2017012602170225AP
    }

    public void validateAddtlCompDtos(List<AddtlCompensationRunDto> addtlCompensationRunDtos) {

        // get first instance for billing period dates
        AddtlCompensationRunDto addtlCompensationDto = addtlCompensationRunDtos.get(0);
        String startDate = addtlCompensationDto.getBillingStartDate();
        String endDate = addtlCompensationDto.getBillingEndDate();

        boolean hasAdjusted = billingPeriodIsFinalized(startDate, endDate, MeterProcessType.ADJUSTED);
        boolean hasFinal = billingPeriodIsFinalized(startDate, endDate, MeterProcessType.FINAL);

        Preconditions.checkState((hasAdjusted || hasFinal),
                "GMR should be finalized for billing period [".concat(startDate).concat(" to ").concat(endDate).concat("]"));

        checkTimeValidity(endDate);

        for (AddtlCompensationRunDto runDto : addtlCompensationRunDtos) {
            checkDuplicate(runDto);
        }
    }

    private void launchAddtlCompensation(TaskRunDto taskRunDto) throws URISyntaxException, LaunchJobException {
        String startDate = taskRunDto.getBillingStartDate();
        String endDate = taskRunDto.getBillingEndDate();
        String groupId = buildGroupId(taskRunDto);

        saveAddtlCompParam(taskRunDto, groupId);

        List<String> properties = Lists.newArrayList();
        List<String> arguments = Lists.newArrayList();

        final Long runId = taskRunDto.getRunId();
        arguments.add(concatKeyValue(RUN_ID, String.valueOf(runId), "long"));
        arguments.add(concatKeyValue(PARENT_JOB, String.valueOf(runId), "long"));
        arguments.add(concatKeyValue(GROUP_ID, groupId));
        arguments.add(concatKeyValue(START_DATE, startDate, "date"));
        arguments.add(concatKeyValue(END_DATE, endDate, "date"));
        arguments.add(concatKeyValue(AC_PRICING_CONDITION, taskRunDto.getPricingCondition()));
        arguments.add(concatKeyValue(USERNAME, taskRunDto.getCurrentUser()));
        saveAddltCompCalcAdditionalParams(runId, taskRunDto);

        boolean hasAdjusted = billingPeriodIsFinalized(startDate, endDate, MeterProcessType.ADJUSTED);
        properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(
                hasAdjusted ? AddtlCompJobProfile.AC_CALC_ADJ_PROFILE : AddtlCompJobProfile.AC_CALC_FINAL_PROFILE)));

        log.debug("Running job name={}, properties={}, arguments={}", AC_CALC, properties, arguments);
        launchJob(ADDTL_COMP_TASK_NAME, properties, arguments);
    }

    private void calcGmrVatAc(TaskRunDto taskRunDto) throws URISyntaxException, LaunchJobException {
        String startDate = taskRunDto.getStartDate();
        String endDate = taskRunDto.getEndDate();
        String pricingCondition = taskRunDto.getPricingCondition();

        boolean hasAdjusted = billingPeriodIsFinalized(startDate, endDate, MeterProcessType.ADJUSTED);

        List<String> properties = Lists.newArrayList();
        List<String> arguments = Lists.newArrayList();

        final Long runId = taskRunDto.getRunId();
        arguments.add(concatKeyValue(RUN_ID, String.valueOf(runId), "long"));
        arguments.add(concatKeyValue(GROUP_ID, taskRunDto.getGroupId()));
        arguments.add(concatKeyValue(START_DATE, startDate, "date"));
        arguments.add(concatKeyValue(END_DATE, endDate, "date"));
        arguments.add(concatKeyValue(AC_PRICING_CONDITION, pricingCondition));
        arguments.add(concatKeyValue(USERNAME, taskRunDto.getCurrentUser()));

        String jobName = determineJobAndSetProfile(hasAdjusted, taskRunDto, properties);
        saveAdjRun(taskRunDto, jobName, hasAdjusted);

        log.debug("Running job name={}, properties={}, arguments={}", ADDTL_COMP_TASK_NAME, properties, arguments);
        launchJob(ADDTL_COMP_TASK_NAME, properties, arguments);
    }

    private void finalizeAc(TaskRunDto taskRunDto) throws URISyntaxException, LaunchJobException {
        String startDate = taskRunDto.getStartDate();
        String endDate = taskRunDto.getEndDate();
        String pricingCondition = taskRunDto.getPricingCondition();

        List<String> properties = Lists.newArrayList();
        List<String> arguments = Lists.newArrayList();

        final Long runId = taskRunDto.getRunId();
        arguments.add(concatKeyValue(RUN_ID, String.valueOf(runId), "long"));
        arguments.add(concatKeyValue(GROUP_ID, taskRunDto.getGroupId()));
        arguments.add(concatKeyValue(START_DATE, startDate, "date"));
        arguments.add(concatKeyValue(END_DATE, endDate, "date"));
        arguments.add(concatKeyValue(AC_PRICING_CONDITION, pricingCondition));
        arguments.add(concatKeyValue(USERNAME, taskRunDto.getCurrentUser()));

        PricingCondition pc = PricingCondition.valueOf(pricingCondition);
        String finalizeProfile;

        switch (pc) {
            case AP:
            case SEC:
            case PSM:
            case MOT:
                finalizeProfile = AddtlCompJobProfile.AC_FINALIZE_PROFILE_AP_SEC;
                saveAMSadditionalParamsApSec(runId, taskRunDto);
                break;
            case  MRU:
                finalizeProfile = AddtlCompJobProfile.AC_FINALIZE_PROFILE_MRU;
                saveAMSadditionalParamsMru(runId, taskRunDto);
                break;
            default:
                throw new RuntimeException("Unsupported Pricing Condition: " + pc);
        }

        properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(finalizeProfile)));

        log.debug("Running job name={}, properties={}, arguments={}", ADDTL_COMP_TASK_NAME, properties, arguments);
        launchJob(ADDTL_COMP_TASK_NAME, properties, arguments);
    }

    private void saveAdjRun(TaskRunDto taskRunDto, String jobName, boolean hasAdjusted) {
        LocalDateTime start = null;
        LocalDateTime end = null;

        try {
            start = DateUtil.getStartRangeDate(taskRunDto.getStartDate());
            end = DateUtil.getStartRangeDate(taskRunDto.getEndDate());
        } catch (ParseException e) {
            e.printStackTrace();
        }

        BatchJobAdjRun adjVatRun = new BatchJobAdjRun();
        adjVatRun.setAdditionalCompensation(true);
        adjVatRun.setJobId(null);
        adjVatRun.setGroupId(taskRunDto.getGroupId());
        MeterProcessType processType = determineProcessType(jobName);

        if (processType == null) {
            processType = hasAdjusted ? MeterProcessType.ADJUSTED : MeterProcessType.FINAL;
        }

        adjVatRun.setMeterProcessType(processType);
        adjVatRun.setBillingPeriodStart(start);
        adjVatRun.setBillingPeriodEnd(end);
        adjVatRun.setOutputReady(false);

        if (batchJobAdjRunRepository.countByGroupIdAndBillingPeriodStartAndBillingPeriodEnd(adjVatRun.getGroupId(),
                start, end) < 1) {
            saveBatchJobAdjRun(adjVatRun);
        }
    }

    private String determineJobAndSetProfile(final boolean hasAdjusted, TaskRunDto taskRunDto, List<String> properties) throws URISyntaxException {
        LocalDateTime start = null;
        LocalDateTime end = null;

        try {
            start = DateUtil.getStartRangeDate(taskRunDto.getStartDate());
            end = DateUtil.getStartRangeDate(taskRunDto.getEndDate());
        } catch (ParseException e) {
            e.printStackTrace();
        }

        String result;
        if (hasAdjusted) {
            result = batchJobAdjRunRepository.isLatestFinalizedBillingPeriodAc(start, end);
            if (result != null && result.equals("Y")) {
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(AC_CALC_GMR_AC_PROFILE)));
                return AC_CALC_GMR_AC;
            } else {
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(AC_CALC_GMR_ADJ_PROFILE)));
                return AC_CALC_GMR_ADJ;
            }
        } else {
            result = batchJobAdjRunRepository.findLatestFinalizedAcByBillingPeriod(start, end);
            if (result == null) {
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(AC_CALC_GMR_FINAL_PROFILE)));
                return AC_CALC_GMR_FINAL;
            } else {
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(AC_CALC_GMR_AC_PROFILE)));
                return AC_CALC_GMR_AC;
            }
        }
    }

    private MeterProcessType determineProcessType(String jobName) {
        switch (jobName) {
            case AC_CALC_GMR_FINAL:
                return MeterProcessType.FINAL;
            case AC_CALC_GMR_ADJ:
                return MeterProcessType.ADJUSTED;
            default:
                return null;
        }
    }

    private void generateFilesAc(TaskRunDto taskRunDto) throws URISyntaxException, LaunchJobException {

        String startDate = taskRunDto.getStartDate();
        String endDate = taskRunDto.getEndDate();
        String pricingCondition = taskRunDto.getPricingCondition();
        String groupId = taskRunDto.getGroupId();

        List<String> properties = Lists.newArrayList();
        List<String> arguments = Lists.newArrayList();

        final Long runId = taskRunDto.getRunId();
        arguments.add(concatKeyValue(RUN_ID, String.valueOf(runId), "long"));
        arguments.add(concatKeyValue(GROUP_ID, groupId));
        arguments.add(concatKeyValue(START_DATE, startDate, "date"));
        arguments.add(concatKeyValue(END_DATE, endDate, "date"));
        arguments.add(concatKeyValue(AC_PRICING_CONDITION, pricingCondition));
        arguments.add(concatKeyValue(USERNAME, taskRunDto.getCurrentUser()));

        properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(AC_GEN_FILE_PROFILE)));

        launchJob(ADDTL_COMP_FILE_GEN_TASK_NAME, properties, arguments);
    }

    private void saveAMSadditionalParamsApSec(final Long runId, final TaskRunDto taskRunDto) {
        log.info("Saving additional AC AMS params. AddtlCompensationGenFilesDto: {}", taskRunDto);
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
            log.error("Error parsing additional batch job params for AC AMS: {}", e);
        }
    }

    private void saveAMSadditionalParamsMru(final Long runId, final TaskRunDto taskRunDto) {
        log.info("Saving additional AC AMS params. AddtlCompensationGenFilesDto: {}", taskRunDto);

        if (CollectionUtils.isEmpty(taskRunDto.getMruInstallmentParams())) {
            throw new RuntimeException("Empty Mru Installment Data!");
        }

        BatchJobAddtlParams batchJobAddtlParamsMru = new BatchJobAddtlParams();
        batchJobAddtlParamsMru.setRunId(runId);
        batchJobAddtlParamsMru.setType("LONG");
        batchJobAddtlParamsMru.setKey(AMS_MRU_INSTALLMENT);
        batchJobAddtlParamsMru.setLongVal(taskRunDto.getAmsMruInstallment());
        saveBatchJobAddtlParamsJdbc(batchJobAddtlParamsMru);

        List<MruInstallmentParam> mruInstallmentParams = taskRunDto.getMruInstallmentParams().stream()
                .sorted(Comparator.comparing(MruInstallmentParam::getInstallmentNum)).collect(Collectors.toList());

        String concatInvoiceDates = mruInstallmentParams.stream().map(MruInstallmentParam::getInvoiceDate)
                .collect(Collectors.joining(TaskUtil.MRU_PARAM_DELIMITER));

        BatchJobAddtlParams batchJobAddtlParamsInvoiceDate = new BatchJobAddtlParams();
        batchJobAddtlParamsInvoiceDate.setRunId(runId);
        batchJobAddtlParamsInvoiceDate.setType("STRING");
        batchJobAddtlParamsInvoiceDate.setKey(AMS_INVOICE_DATE);
        batchJobAddtlParamsInvoiceDate.setStringVal(concatInvoiceDates);
        saveBatchJobAddtlParamsJdbc(batchJobAddtlParamsInvoiceDate);

        String concatDueDates = mruInstallmentParams.stream().map(MruInstallmentParam::getDueDate)
                .collect(Collectors.joining(TaskUtil.MRU_PARAM_DELIMITER));

        BatchJobAddtlParams batchJobAddtlParamsDueDate = new BatchJobAddtlParams();
        batchJobAddtlParamsDueDate.setRunId(runId);
        batchJobAddtlParamsDueDate.setType("STRING");
        batchJobAddtlParamsDueDate.setKey(AMS_DUE_DATE);
        batchJobAddtlParamsDueDate.setStringVal(concatDueDates);
        saveBatchJobAddtlParamsJdbc(batchJobAddtlParamsDueDate);

        if (mruInstallmentParams.stream().anyMatch(mruParam -> StringUtils.isNotEmpty(mruParam.getRemarks())
                && mruParam.getRemarks().contains(TaskUtil.MRU_PARAM_DELIMITER))) {
            throw new RuntimeException("MRU Remarks should not contain pipe (\"|\") characters.");
        }

        String concatRemarks = mruInstallmentParams.stream().map(mruParam ->
                StringUtils.isNotEmpty(mruParam.getRemarks()) ? mruParam.getRemarks() : "")
                .collect(Collectors.joining(TaskUtil.MRU_PARAM_DELIMITER));
        BatchJobAddtlParams batchJobAddtlParamsRemarksInv = new BatchJobAddtlParams();
        batchJobAddtlParamsRemarksInv.setRunId(runId);
        batchJobAddtlParamsRemarksInv.setType("STRING");
        batchJobAddtlParamsRemarksInv.setKey(AMS_REMARKS_INV);
        batchJobAddtlParamsRemarksInv.setStringVal(concatRemarks);
        saveBatchJobAddtlParamsJdbc(batchJobAddtlParamsRemarksInv);
    }

    private void saveAddltCompCalcAdditionalParams(final Long runId, final TaskRunDto taskRunDto) {
        log.debug("Saving AC Calc additional params. addtlCompensationDto: {}", taskRunDto);

        BatchJobAddtlParams batchJobAddtlParamsBillingId = new BatchJobAddtlParams();
        batchJobAddtlParamsBillingId.setRunId(runId);
        batchJobAddtlParamsBillingId.setType("STRING");
        batchJobAddtlParamsBillingId.setKey(AC_BILLING_ID);
        batchJobAddtlParamsBillingId.setStringVal(taskRunDto.getBillingId());
        saveBatchJobAddtlParamsJdbc(batchJobAddtlParamsBillingId);

        BatchJobAddtlParams batchJobAddtlParamsMtn = new BatchJobAddtlParams();
        batchJobAddtlParamsMtn.setRunId(runId);
        batchJobAddtlParamsMtn.setType("STRING");
        batchJobAddtlParamsMtn.setKey(AC_MTN);
        batchJobAddtlParamsMtn.setStringVal(taskRunDto.getMtn());
        saveBatchJobAddtlParamsJdbc(batchJobAddtlParamsMtn);

        BatchJobAddtlParams batchJobAddtlParamPricingCondition = new BatchJobAddtlParams();
        batchJobAddtlParamPricingCondition.setRunId(runId);
        batchJobAddtlParamPricingCondition.setType("STRING");
        batchJobAddtlParamPricingCondition.setKey(AC_PRICING_CONDITION);
        batchJobAddtlParamPricingCondition.setStringVal(taskRunDto.getPricingCondition());
        saveBatchJobAddtlParamsJdbc(batchJobAddtlParamPricingCondition);

        BatchJobAddtlParams batchJobAddtlParamApprovedRate = new BatchJobAddtlParams();
        batchJobAddtlParamApprovedRate.setRunId(runId);
        batchJobAddtlParamApprovedRate.setType("DOUBLE");
        batchJobAddtlParamApprovedRate.setKey(AC_APPROVED_RATE);
        batchJobAddtlParamApprovedRate.setDoubleVal(taskRunDto.getApprovedRate().doubleValue());
        saveBatchJobAddtlParamsJdbc(batchJobAddtlParamApprovedRate);
    }

    private void saveAddtlCompParam(TaskRunDto taskRunDto, String groupId) {
        LocalDateTime start = null;
        LocalDateTime end = null;

        try {
            start = DateUtil.getStartRangeDate(taskRunDto.getBillingStartDate());
            end = DateUtil.getStartRangeDate(taskRunDto.getBillingEndDate());
        } catch (ParseException e) {
            log.error("Unable to parse dates.", e);
        }

        AddtlCompParams addtlCompParams = new AddtlCompParams();
        addtlCompParams.setBillingStartDate(start);
        addtlCompParams.setBillingEndDate(end);
        addtlCompParams.setPricingCondition(taskRunDto.getPricingCondition());
        addtlCompParams.setBillingId(taskRunDto.getBillingId());
        addtlCompParams.setMtn(taskRunDto.getMtn());
        addtlCompParams.setApprovedRate(taskRunDto.getApprovedRate());
        addtlCompParams.setGroupId(groupId);
        addtlCompParams.setStatus("STARTED");

        saveAddtlCompParamJdbc(addtlCompParams);
    }

    private boolean billingPeriodIsFinalized(String startDate, String endDate, MeterProcessType processType) {
        LocalDateTime startDateTime = DateUtil.parseLocalDate(startDate, DateUtil.DEFAULT_DATE_FORMAT).atStartOfDay();
        LocalDateTime endDateTime = DateUtil.parseLocalDate(endDate, DateUtil.DEFAULT_DATE_FORMAT).atStartOfDay();

        return settlementJobLockRepository.billingPeriodIsFinalizedForAc(startDateTime, endDateTime, processType.name(),
                StlCalculationType.TRADING_AMOUNTS.name());
    }

    private void checkTimeValidity(String billingEndDate) {
        LocalDateTime endDate;
        try {
            endDate = DateUtil.getStartRangeDate(billingEndDate);
            LocalDateTime dateDeterminant = LocalDateTime.now().minusMonths(ADDTL_COMP_MONTH_VALIDITY);

            Preconditions.checkState(endDate.isAfter(dateDeterminant),
                    "Billing period is now invalid due to ".concat(String.valueOf(ADDTL_COMP_MONTH_VALIDITY)).concat("-month limit"));
        } catch (ParseException e) {
            log.error("Unable to parse endDate", e);
        }
    }

    private void checkDuplicate(AddtlCompensationRunDto addtlCompensationDto) {
        String mtn = addtlCompensationDto.getMtn();
        String pc = addtlCompensationDto.getPricingCondition();
        LocalDateTime start = null;
        LocalDateTime end = null;

        try {
            start = DateUtil.getStartRangeDate(addtlCompensationDto.getBillingStartDate());
            end = DateUtil.getStartRangeDate(addtlCompensationDto.getBillingEndDate());

        } catch (ParseException e) {
            log.error("Unable to parse dates.", e);
        }

        Long count = addtlCompParamsRepository.countByBillingStartDateAndBillingEndDateAndMtnAndPricingConditionAndStatusNot(start, end, mtn, pc, "FAILED");

        Preconditions.checkState(count == 0,
                String.format("Duplicate entry. mtn=[%s] startDate=[%s] endDate=[%s] pricingCondition=[%s]",
                        mtn, start, end, pc));
    }

    private JobExecution getLatestCalcGmrVatAcJob(String groupId) {
        List<JobInstance> taggingJobInstances = jobExplorer.findJobInstancesByJobName(AC_CALC_GMR_BASE_NAME.concat("*-").concat(groupId), 0, 1);
        if (!taggingJobInstances.isEmpty()) {
            List<JobExecution> finalizeJobExecs = getJobExecutions(taggingJobInstances.get(0));
            if (!finalizeJobExecs.isEmpty()) {
                return finalizeJobExecs.get(0);
            }
        }
        return null;
    }

    private JobExecution getLatestFinalizeAcJob(String groupId) {
        List<JobInstance> taggingJobInstances = jobExplorer.findJobInstancesByJobName(AC_FINALIZE_BASE_NAME.concat("*-").concat(groupId), 0, 1);
        if (!taggingJobInstances.isEmpty()) {
            List<JobExecution> finalizeJobExecs = getJobExecutions(taggingJobInstances.get(0));
            if (!finalizeJobExecs.isEmpty()) {
                return finalizeJobExecs.get(0);
            }
        }
        return null;
    }

    private List<String> getProgress(final JobExecution jobExecution) {
        List<String> runningTasks = Lists.newArrayList();
        if (!jobExecution.getStepExecutions().isEmpty()) {
            jobExecution.getStepExecutions().parallelStream()
                    .filter(stepExecution -> stepExecution.getStatus().isRunning())
                    .forEach(stepExecution -> {
                        Map<String, String> map = StlAddtlCompStepUtil.getProgressNameTaskMap();
                        String stepName = stepExecution.getStepName();
                        if (map.containsKey(stepName)) {
                            runningTasks.add(map.get(stepName));
                        } else {
                            log.warn("Step name {} not existing in current mapping.", stepName);
                        }
                    });
        }

        return runningTasks;
    }

}
