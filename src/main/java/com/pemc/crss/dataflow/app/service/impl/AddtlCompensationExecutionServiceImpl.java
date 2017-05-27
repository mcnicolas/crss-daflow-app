package com.pemc.crss.dataflow.app.service.impl;

import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.security.Principal;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameters;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.pemc.crss.dataflow.app.dto.AddtlCompensationExecDetailsDto;
import com.pemc.crss.dataflow.app.dto.AddtlCompensationExecutionDto;
import com.pemc.crss.dataflow.app.dto.AddtlCompensationFinalizeDto;
import com.pemc.crss.dataflow.app.dto.AddtlCompensationGenFilesDto;
import com.pemc.crss.dataflow.app.dto.AddtlCompensationRunDto;
import com.pemc.crss.dataflow.app.dto.AddtlCompensationRunListDto;
import com.pemc.crss.dataflow.app.dto.BaseTaskExecutionDto;
import com.pemc.crss.dataflow.app.dto.TaskRunDto;
import com.pemc.crss.dataflow.app.support.PageableRequest;
import com.pemc.crss.dataflow.app.util.SecurityUtil;
import com.pemc.crss.shared.commons.reference.MeterProcessType;
import com.pemc.crss.shared.commons.util.DateUtil;
import com.pemc.crss.shared.core.dataflow.entity.AddtlCompParams;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobAddtlParams;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobAdjVatRun;
import com.pemc.crss.shared.core.dataflow.repository.AddtlCompParamsRepository;
import com.pemc.crss.shared.core.dataflow.repository.BatchJobAddtlParamsRepository;
import com.pemc.crss.shared.core.dataflow.repository.BatchJobAdjVatRunRepository;

import static com.pemc.crss.shared.commons.util.TaskUtil.*;

@Slf4j
@Service("addtlCompensationExecutionService")
@Transactional
public class AddtlCompensationExecutionServiceImpl extends AbstractTaskExecutionService {

    private static final String ADDTL_COMP_JOB_NAME = "calculateAddtlComp";
    private static final String ADDTL_COMP_GMR_BASE_JOB_NAME = "calculateAddtlCompGmrVat";
    private static final String ADDTL_COMP_GMR_AC_JOB_NAME = "calculateAddtlCompGmrVatAc";
    private static final String ADDTL_COMP_GMR_ADJ_JOB_NAME = "calculateAddtlCompGmrVatAdjusted";
    private static final String ADDTL_COMP_GMR_FINAL_JOB_NAME = "calculateAddtlCompGmrVatFinal";
    private static final String GENERATE_ADDTL_COMP_JOB_NAME = "generateAddtlCompFiles";

    private static final String ADDTL_COMP_TASK_NAME = "crss-settlement-task-calculation-addtlcomp";
    private static final String ADDTL_COMP_FILE_GEN_TASK_NAME = "crss-settlement-task-file-gen-addtlcomp";

    private static final String AC_FILE_GEN_FOLDERNAME = "AC_FILE_GEN_FOLDERNAME";
    private static final List<String> AC_FILTER_STEP_LIST = Arrays.asList("calculateAddtlCompStep",
            "calculateAddtlCompAllocStep", "calculateAddtlCompVatStep", "calculateAddtlCompVatAllocStep");

    private static final long ADDTL_COMP_MONTH_VALIDITY = 24;

    @Autowired
    private BatchJobAdjVatRunRepository batchJobAdjVatRunRepository;

    @Autowired
    private AddtlCompParamsRepository addtlCompParamsRepository;

    @Autowired
    private BatchJobAddtlParamsRepository batchJobAddtlParamsRepository;

    @Override
    public Page<? extends BaseTaskExecutionDto> findJobInstances(Pageable pageable, String type, String status, String mode, String runStartDate, String tradingStartDate, String tradingEndDate, String username) {
        return null;
    }

    @Override
    public Page<? extends BaseTaskExecutionDto> findJobInstances(PageableRequest pageableRequest) {
        final Pageable pageable = pageableRequest.getPageable();
        Long totalSize = dataFlowJdbcJobExecutionDao.countDistinctAddtlCompJobInstances();

        List<AddtlCompensationExecutionDto> addtlCompensationExecutionDtoList = dataFlowJdbcJobExecutionDao
                .findDistinctAddtlCompJobInstances(pageable.getOffset(), pageable.getPageSize())
                .stream()
                .map(distinctAddtlCompDto -> {
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

                                    AddtlCompensationExecDetailsDto addtlCompensationExecDetailsDto = new AddtlCompensationExecDetailsDto();
                                    addtlCompensationExecDetailsDto.setRunId(parameters.getLong(RUN_ID));
                                    addtlCompensationExecDetailsDto.setBillingId(parameters.getString(AC_BILLING_ID));
                                    addtlCompensationExecDetailsDto.setMtn(parameters.getString(AC_MTN));
                                    addtlCompensationExecDetailsDto.setApprovedRate(BigDecimal.valueOf(parameters.getDouble(AC_APPROVED_RATE)));
                                    addtlCompensationExecDetailsDto.setStatus(jobExecution.getStatus().name());
                                    addtlCompensationExecDetailsDto.setTaskSummaryList(showSummary(jobExecution, AC_FILTER_STEP_LIST));

                                    BatchStatus taggingStatus = extractTaggingStatus(groupId);

                                    distinctAddtlCompDto.setTaggingStatus(taggingStatus);
                                    distinctAddtlCompDto.setGroupId(groupId);

                                    // do not include finalize ac jobs
                                    if (!jobInstance.getJobName().startsWith(ADDTL_COMP_GMR_BASE_JOB_NAME)) {
                                        addtlCompensationExecDetailsDtos.add(addtlCompensationExecDetailsDto);
                                    }
                                }
                            });

                    if (distinctAddtlCompDto != null && distinctAddtlCompDto.getTaggingStatus() != null
                            && distinctAddtlCompDto.getTaggingStatus().equals(BatchStatus.COMPLETED)) {
                        // get generated AC files folder name
                        Optional<JobInstance> genFileJobInstanceOpt = jobExplorer.findJobInstancesByJobName(
                                GENERATE_ADDTL_COMP_JOB_NAME + "-" + distinctAddtlCompDto.getGroupId(),
                                0, Integer.MAX_VALUE).stream().findFirst();

                        genFileJobInstanceOpt.ifPresent(jobInstance -> getJobExecutions(jobInstance)
                                .forEach(jobExecution -> {
                                    distinctAddtlCompDto.setGenFileStatus(jobExecution.getStatus());
                                    distinctAddtlCompDto.setGenFileEndTime(
                                            DateUtil.convertToString(jobExecution.getEndTime(), DateUtil.DEFAULT_DATETIME_FORMAT));

                                    Optional.ofNullable(jobExecution.getExecutionContext().get(AC_FILE_GEN_FOLDERNAME))
                                            .ifPresent(val -> distinctAddtlCompDto.setGenFileFolderName((String) val));
                                })
                        );
                    }

                    addtlCompensationExecutionDto.setAddtlCompensationExecDetailsDtos(addtlCompensationExecDetailsDtos);
                    return addtlCompensationExecutionDto;
                }).collect(Collectors.toList());

        return new PageImpl<>(addtlCompensationExecutionDtoList, pageable, totalSize);
    }

    @Override
    public void relaunchFailedJob(long jobId) throws URISyntaxException {

    }

    @Override
    public Page<? extends BaseTaskExecutionDto> findJobInstances(Pageable pageable) {
        return null;
    }

    @Override
    public void launchJob(TaskRunDto taskRunDto) throws URISyntaxException {

    }

    public void launchGroupAddtlCompensation(AddtlCompensationRunListDto addtlCompensationRunDtos, Principal principal) throws URISyntaxException {
        if (!CollectionUtils.isEmpty(addtlCompensationRunDtos.getAddtlCompensationRunDtos())) {

            // only get the first instance since all of them have the same start / end / pc
            AddtlCompensationRunDto addtlCompRunDto = addtlCompensationRunDtos.getAddtlCompensationRunDtos().get(0);
            final String groupId = buildGroupId(addtlCompRunDto);

            for (AddtlCompensationRunDto addtlCompensationRunDto : addtlCompensationRunDtos.getAddtlCompensationRunDtos()) {
                addtlCompensationRunDto.setCurrentUser(SecurityUtil.getCurrentUser(principal));
                launchAddtlCompensation(addtlCompensationRunDto, groupId);
            }

            lockJob(ADDTL_COMP_JOB_NAME);
        }
    }

    public void launchAddtlCompensation(AddtlCompensationRunDto addtlCompensationDto) throws URISyntaxException {
        launchAddtlCompensation(addtlCompensationDto, buildGroupId(addtlCompensationDto));
    }

    private String buildGroupId(AddtlCompensationRunDto runDto) {
        String groupId = runDto.getBillingStartDate() + runDto.getBillingEndDate() + runDto.getPricingCondition();
        return groupId.replaceAll("-",""); // produces: YYYYMMDDYYYYMMDDPC ex: 2017012602170225AP
    }

    public void launchAddtlCompensation(AddtlCompensationRunDto addtlCompensationDto, String groupId) throws URISyntaxException {
        Preconditions.checkState(batchJobRunLockRepository.countByJobNameAndLockedIsTrue(ADDTL_COMP_JOB_NAME) == 0,
                "There is an existing ".concat(ADDTL_COMP_JOB_NAME).concat(" job running"));

        List<JobInstance> finalizeJobInstances = dataFlowJdbcJobExecutionDao.findAddtlCompCompleteFinalizeInstances(0,
                Integer.MAX_VALUE, addtlCompensationDto.getBillingStartDate(),
                addtlCompensationDto.getBillingEndDate(), addtlCompensationDto.getPricingCondition());

        Preconditions.checkState(finalizeJobInstances.isEmpty(), String.format("Additional Compensation for billing period "
                + " [%s to %s] with %s pricing condition has already been finalized.", addtlCompensationDto.getBillingStartDate(),
                addtlCompensationDto.getBillingEndDate(), addtlCompensationDto.getPricingCondition()));

        String startDate = addtlCompensationDto.getBillingStartDate();
        String endDate = addtlCompensationDto.getBillingEndDate();

        boolean hasAdjusted = dataFlowJdbcJobExecutionDao.countFinalizeJobInstances(MeterProcessType.ADJUSTED, startDate, endDate) > 0;
        Preconditions.checkState(hasAdjusted || dataFlowJdbcJobExecutionDao.countFinalizeJobInstances(MeterProcessType.FINAL, startDate, endDate) > 0,
                "GMR should be finalized for billing period [".concat(startDate).concat(" to ").concat(endDate).concat("]"));

        checkTimeValidity(endDate);
        checkDuplicate(addtlCompensationDto);
        saveAddtlCompParam(addtlCompensationDto, groupId);

        List<String> properties = Lists.newArrayList();
        List<String> arguments = Lists.newArrayList();

        final Long runId = System.currentTimeMillis();
        arguments.add(concatKeyValue(RUN_ID, String.valueOf(runId), "long"));
        arguments.add(concatKeyValue(PARENT_JOB, String.valueOf(runId), "long"));
        arguments.add(concatKeyValue(GROUP_ID, groupId));
        arguments.add(concatKeyValue(START_DATE, startDate, "date"));
        arguments.add(concatKeyValue(END_DATE, endDate, "date"));
        arguments.add(concatKeyValue(AC_PRICING_CONDITION, addtlCompensationDto.getPricingCondition()));
        arguments.add(concatKeyValue(USERNAME, addtlCompensationDto.getCurrentUser()));
        saveAddltCompCalcAdditionalParams(runId, addtlCompensationDto);

        properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(
                hasAdjusted ? "monthlyAdjustedAddtlCompCalculation" : "monthlyFinalAddtlCompCalculation")));

        log.debug("Running job name={}, properties={}, arguments={}", ADDTL_COMP_JOB_NAME, properties, arguments);
        launchJob(ADDTL_COMP_TASK_NAME, properties, arguments);
    }

    public void finalizeAC(AddtlCompensationFinalizeDto addtlCompensationFinalizeDto) throws URISyntaxException {
        Preconditions.checkState(batchJobRunLockRepository.countByJobNameAndLockedIsTrue(ADDTL_COMP_GMR_BASE_JOB_NAME) == 0,
                "There is an existing ".concat(ADDTL_COMP_GMR_BASE_JOB_NAME).concat(" job running"));
        String startDate = addtlCompensationFinalizeDto.getStartDate();
        String endDate = addtlCompensationFinalizeDto.getEndDate();
        String pricingCondition = addtlCompensationFinalizeDto.getPricingCondition();

        boolean hasAdjusted = dataFlowJdbcJobExecutionDao.countFinalizeJobInstances(MeterProcessType.ADJUSTED, startDate, endDate) > 0;

        List<String> properties = Lists.newArrayList();
        List<String> arguments = Lists.newArrayList();

        final Long runId = System.currentTimeMillis();
        arguments.add(concatKeyValue(RUN_ID, String.valueOf(runId), "long"));
        arguments.add(concatKeyValue(GROUP_ID, addtlCompensationFinalizeDto.getGroupId()));
        arguments.add(concatKeyValue(START_DATE, startDate, "date"));
        arguments.add(concatKeyValue(END_DATE, endDate, "date"));
        arguments.add(concatKeyValue(AC_PRICING_CONDITION, pricingCondition));

        String jobName = determineJobAndSetProfile(hasAdjusted, addtlCompensationFinalizeDto, properties);
        saveAdjVatRun(addtlCompensationFinalizeDto, jobName);

        log.debug("Running job name={}, properties={}, arguments={}", ADDTL_COMP_TASK_NAME, properties, arguments);
        launchJob(ADDTL_COMP_TASK_NAME, properties, arguments);
        lockJob(ADDTL_COMP_GMR_BASE_JOB_NAME);
    }

    private void saveAdjVatRun(AddtlCompensationFinalizeDto addtlCompensationFinalizeDto, String jobName) {
        LocalDateTime start = null;
        LocalDateTime end = null;

        try {
            start = DateUtil.getStartRangeDate(addtlCompensationFinalizeDto.getStartDate());
            end = DateUtil.getStartRangeDate(addtlCompensationFinalizeDto.getEndDate());
        } catch (ParseException e) {
            e.printStackTrace();
        }

        // jobId is null for finalize job
        BatchJobAdjVatRun adjVatRun = new BatchJobAdjVatRun();
        adjVatRun.setAdditionalCompensation(true);
        adjVatRun.setJobId(null);
        adjVatRun.setGroupId(addtlCompensationFinalizeDto.getGroupId());
        adjVatRun.setMeterProcessType(determineProcessType(jobName));
        adjVatRun.setBillingPeriodStart(start);
        adjVatRun.setBillingPeriodEnd(end);
        adjVatRun.setOutputReady(false);

        batchJobAdjVatRunRepository.save(adjVatRun);
    }

    private String determineJobAndSetProfile(final boolean hasAdjusted, AddtlCompensationFinalizeDto addtlCompensationFinalizeDto, List<String> properties) throws URISyntaxException {
        LocalDateTime start = null;
        LocalDateTime end = null;

        try {
            start = DateUtil.getStartRangeDate(addtlCompensationFinalizeDto.getStartDate());
            end = DateUtil.getStartRangeDate(addtlCompensationFinalizeDto.getEndDate());
        } catch (ParseException e) {
            e.printStackTrace();
        }

        String result;
        if (hasAdjusted) {
            result = batchJobAdjVatRunRepository.isLatestFinalizedBillingPeriodAc(start, end);
            if (result != null && result.equals("Y")) {
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("addtlCompGmrVatAcCalculation")));
                return ADDTL_COMP_GMR_AC_JOB_NAME;
            } else {
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyAdjustedAddtlCompGmrVatCalculation")));
                return ADDTL_COMP_GMR_ADJ_JOB_NAME;
            }
        } else {
            result = batchJobAdjVatRunRepository.findLatestFinalizedAcByBillingPeriod(start, end);
            if (result == null) {
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyFinalAddtlCompGmrVatCalculation")));
                return ADDTL_COMP_GMR_FINAL_JOB_NAME;
            } else {
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("addtlCompGmrVatAcCalculation")));
                return ADDTL_COMP_GMR_AC_JOB_NAME;
            }
        }
    }

    private MeterProcessType determineProcessType(String jobName) {
        switch (jobName){
            case ADDTL_COMP_GMR_FINAL_JOB_NAME:
                return MeterProcessType.FINAL;
            case ADDTL_COMP_GMR_ADJ_JOB_NAME:
                return MeterProcessType.ADJUSTED;
            default:
                return null;
        }
    }

    public void generateFilesAc(AddtlCompensationGenFilesDto genFilesDto) throws URISyntaxException {
        Preconditions.checkState(batchJobRunLockRepository.countByJobNameAndLockedIsTrue(GENERATE_ADDTL_COMP_JOB_NAME) == 0,
                "There is an existing ".concat(GENERATE_ADDTL_COMP_JOB_NAME).concat(" job running"));
        String startDate = genFilesDto.getStartDate();
        String endDate = genFilesDto.getEndDate();
        String pricingCondition = genFilesDto.getPricingCondition();
        String groupId = genFilesDto.getGroupId();

        List<String> properties = Lists.newArrayList();
        List<String> arguments = Lists.newArrayList();

        final Long runId = System.currentTimeMillis();
        arguments.add(concatKeyValue(RUN_ID, String.valueOf(runId), "long"));
        arguments.add(concatKeyValue(GROUP_ID, groupId));
        arguments.add(concatKeyValue(START_DATE, startDate, "date"));
        arguments.add(concatKeyValue(END_DATE, endDate, "date"));
        arguments.add(concatKeyValue(AC_PRICING_CONDITION, pricingCondition));

        properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("addtlCompFileGeneration")));

        saveAMSadditionalParams(runId, genFilesDto);

        launchJob(ADDTL_COMP_FILE_GEN_TASK_NAME, properties, arguments);
        lockJob(GENERATE_ADDTL_COMP_JOB_NAME);
    }

    private void saveAMSadditionalParams(final Long runId, final AddtlCompensationGenFilesDto genFilesDto) {
        log.info("Saving additional AC AMS params. AddtlCompensationGenFilesDto: {}", genFilesDto);
        try {
            BatchJobAddtlParams batchJobAddtlParamsInvoiceDate = new BatchJobAddtlParams();
            batchJobAddtlParamsInvoiceDate.setRunId(runId);
            batchJobAddtlParamsInvoiceDate.setType("DATE");
            batchJobAddtlParamsInvoiceDate.setKey(AMS_INVOICE_DATE);
            batchJobAddtlParamsInvoiceDate.setDateVal(DateUtil.getStartRangeDate(genFilesDto.getAmsInvoiceDate()));
            batchJobAddtlParamsRepository.save(batchJobAddtlParamsInvoiceDate);

            BatchJobAddtlParams batchJobAddtlParamsDueDate = new BatchJobAddtlParams();
            batchJobAddtlParamsDueDate.setRunId(runId);
            batchJobAddtlParamsDueDate.setType("DATE");
            batchJobAddtlParamsDueDate.setKey(AMS_DUE_DATE);
            batchJobAddtlParamsDueDate.setDateVal(DateUtil.getStartRangeDate(genFilesDto.getAmsDueDate()));
            batchJobAddtlParamsRepository.save(batchJobAddtlParamsDueDate);

            BatchJobAddtlParams batchJobAddtlParamsRemarksInv = new BatchJobAddtlParams();
            batchJobAddtlParamsRemarksInv.setRunId(runId);
            batchJobAddtlParamsRemarksInv.setType("STRING");
            batchJobAddtlParamsRemarksInv.setKey(AMS_REMARKS_INV);
            batchJobAddtlParamsRemarksInv.setStringVal(genFilesDto.getAmsRemarksInv());
            batchJobAddtlParamsRepository.save(batchJobAddtlParamsRemarksInv);

        } catch (ParseException e) {
            log.error("Error parsing additional batch job params for AC AMS: {}", e);
        }
    }

    private void saveAddltCompCalcAdditionalParams(final Long runId, final AddtlCompensationRunDto addtlCompensationDto) {
        log.debug("Saving AC Calc additional params. addtlCompensationDto: {}", addtlCompensationDto);

        BatchJobAddtlParams batchJobAddtlParamsBillingId = new BatchJobAddtlParams();
        batchJobAddtlParamsBillingId.setRunId(runId);
        batchJobAddtlParamsBillingId.setType("STRING");
        batchJobAddtlParamsBillingId.setKey(AC_BILLING_ID);
        batchJobAddtlParamsBillingId.setStringVal(addtlCompensationDto.getBillingId());
        batchJobAddtlParamsRepository.save(batchJobAddtlParamsBillingId);

        BatchJobAddtlParams batchJobAddtlParamsMtn = new BatchJobAddtlParams();
        batchJobAddtlParamsMtn.setRunId(runId);
        batchJobAddtlParamsMtn.setType("STRING");
        batchJobAddtlParamsMtn.setKey(AC_MTN);
        batchJobAddtlParamsMtn.setStringVal(addtlCompensationDto.getMtn());
        batchJobAddtlParamsRepository.save(batchJobAddtlParamsMtn);

        BatchJobAddtlParams batchJobAddtlParamPricingCondition = new BatchJobAddtlParams();
        batchJobAddtlParamPricingCondition.setRunId(runId);
        batchJobAddtlParamPricingCondition.setType("STRING");
        batchJobAddtlParamPricingCondition.setKey(AC_PRICING_CONDITION);
        batchJobAddtlParamPricingCondition.setStringVal(addtlCompensationDto.getPricingCondition());
        batchJobAddtlParamsRepository.save(batchJobAddtlParamPricingCondition);

        BatchJobAddtlParams batchJobAddtlParamApprovedRate = new BatchJobAddtlParams();
        batchJobAddtlParamApprovedRate.setRunId(runId);
        batchJobAddtlParamApprovedRate.setType("DOUBLE");
        batchJobAddtlParamApprovedRate.setKey(AC_APPROVED_RATE);
        batchJobAddtlParamApprovedRate.setDoubleVal(addtlCompensationDto.getApprovedRate().doubleValue());
        batchJobAddtlParamsRepository.save(batchJobAddtlParamApprovedRate);

    }

    private void saveAddtlCompParam(AddtlCompensationRunDto addtlCompensationDto, String groupId) {
        LocalDateTime start = null;
        LocalDateTime end = null;

        try {
            start = DateUtil.getStartRangeDate(addtlCompensationDto.getBillingStartDate());
            end = DateUtil.getStartRangeDate(addtlCompensationDto.getBillingEndDate());
        } catch (ParseException e) {
            log.error("Unable to parse dates.", e);
        }

        AddtlCompParams addtlCompParams = new AddtlCompParams();
        addtlCompParams.setBillingStartDate(start);
        addtlCompParams.setBillingEndDate(end);
        addtlCompParams.setPricingCondition(addtlCompensationDto.getPricingCondition());
        addtlCompParams.setBillingId(addtlCompensationDto.getBillingId());
        addtlCompParams.setMtn(addtlCompensationDto.getMtn());
        addtlCompParams.setApprovedRate(addtlCompensationDto.getApprovedRate());
        addtlCompParams.setGroupId(groupId);
        addtlCompParams.setStatus("STARTED");

        addtlCompParamsRepository.save(addtlCompParams);
    }

    private void lockJob(String jobName) {
        TaskRunDto runDto = new TaskRunDto();
        runDto.setJobName(jobName);
        lockJob(runDto);
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

    private BatchStatus extractTaggingStatus(String groupId) {
        List<JobInstance> taggingJobInstances = jobExplorer.findJobInstancesByJobName(ADDTL_COMP_GMR_BASE_JOB_NAME.concat("*-").concat(groupId), 0, 1);
        if (!taggingJobInstances.isEmpty()) {
            List<JobExecution> finalizeJobExecs = getJobExecutions(taggingJobInstances.get(0));
            if (!finalizeJobExecs.isEmpty()) {
                JobExecution finalizeJobExec = finalizeJobExecs.get(0);
                return finalizeJobExec.getStatus();
            }
        }
        return null;
    }

}
