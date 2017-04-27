package com.pemc.crss.dataflow.app.service.impl;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.pemc.crss.dataflow.app.dto.AddtlCompensationExecutionDto;
import com.pemc.crss.dataflow.app.dto.AddtlCompensationRunDto;
import com.pemc.crss.dataflow.app.dto.BaseTaskExecutionDto;
import com.pemc.crss.dataflow.app.dto.TaskRunDto;
import com.pemc.crss.dataflow.app.support.PageableRequest;
import com.pemc.crss.shared.commons.reference.MeterProcessType;
import com.pemc.crss.shared.commons.util.DateUtil;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobRunLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.net.URISyntaxException;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

@Service("addtlCompensationExecutionService")
@Transactional(readOnly = true, value = "transactionManager")
public class AddtlCompensationExecutionServiceImpl extends AbstractTaskExecutionService {

    private static final Logger LOG = LoggerFactory.getLogger(AddtlCompensationExecutionServiceImpl.class);

    private static final String ADDTL_COMP_JOB_NAME = "calculateAddtlComp";
    private static final String ADDTL_COMP_TASK_NAME = "crss-settlement-task-calculation";

    private static final String PARAM_BILLING_ID = "acBillingId";
    private static final String PARAM_MTN = "acMtn";
    private static final String PARAM_APPROVED_RATE = "acApprovedRate";
    private static final String PARAM_BILLING_START_DATE = "startDate";
    private static final String PARAM_BILLING_END_DATE = "endDate";
    private static final String PARAM_PRICING_CONDITION = "acPricingCondition";

    private static final long ADDTL_COMP_MONTH_VALIDITY = 24;

    @Override
    public Page<? extends BaseTaskExecutionDto> findJobInstances(Pageable pageable, String type, String status, String mode, String runStartDate, String tradingStartDate, String tradingEndDate, String username) {
        return null;
    }

    @Override
    public Page<? extends BaseTaskExecutionDto> findJobInstances(PageableRequest pageableRequest) {
        final Pageable pageable = pageableRequest.getPageable();
        int totalSize = pageable.getPageSize();
        try {
            totalSize = jobExplorer.getJobInstanceCount(ADDTL_COMP_JOB_NAME);
        } catch (NoSuchJobException e) {
            LOG.warn("Unable to retrieve job count for: " + ADDTL_COMP_JOB_NAME, e);
        }

        List<AddtlCompensationExecutionDto> addtlCompensationExecutionDtoList = jobExplorer
                .findJobInstancesByJobName(ADDTL_COMP_JOB_NAME, pageable.getOffset(), pageable.getPageSize())
                .stream()
                .map(jobInstance -> {
                    List<JobExecution> addtlCompensationExecutionList = getJobExecutions(jobInstance);
                    Iterator<JobExecution> addtlCompensationExecutionIterator = addtlCompensationExecutionList.iterator();
                    while (addtlCompensationExecutionIterator.hasNext()) {
                        JobExecution addtlCompensationExecution = addtlCompensationExecutionIterator.next();
                        JobParameters addtlCompensationParameters = addtlCompensationExecution.getJobParameters();

                        AddtlCompensationExecutionDto addtlCompensationExecutionDto = new AddtlCompensationExecutionDto();
                        addtlCompensationExecutionDto.setId(jobInstance.getInstanceId());
                        addtlCompensationExecutionDto.setStatus(addtlCompensationExecution.getStatus().name());
                        addtlCompensationExecutionDto.setParams(Maps.transformValues(addtlCompensationParameters.getParameters(), JobParameter::getValue));
                        addtlCompensationExecutionDto.setExitMessage(null);
                        addtlCompensationExecutionDto.setProgress(null);
                        addtlCompensationExecutionDto.setStatusDetails(null);
                        addtlCompensationExecutionDto.setRunDateTime(addtlCompensationExecution.getStartTime());

                        return addtlCompensationExecutionDto;
                    }

                    return null;
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

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

    public void launchAddtlCompensation(AddtlCompensationRunDto addtlCompensationDto) throws URISyntaxException {
        Preconditions.checkNotNull(ADDTL_COMP_JOB_NAME);
        Preconditions.checkState(batchJobRunLockRepository.countByJobNameAndLockedIsTrue(ADDTL_COMP_JOB_NAME) == 0,
                "There is an existing ".concat(ADDTL_COMP_JOB_NAME).concat(" job running"));

        List<String> properties = Lists.newArrayList();
        List<String> arguments = Lists.newArrayList();
        String startDate = addtlCompensationDto.getBillingStartDate();
        String endDate = addtlCompensationDto.getBillingEndDate();

        final Long runId = System.currentTimeMillis();
        arguments.add(concatKeyValue(RUN_ID, String.valueOf(runId), "long"));
        arguments.add(concatKeyValue(PARAM_BILLING_ID, addtlCompensationDto.getBillingId()));
        arguments.add(concatKeyValue(PARAM_MTN, addtlCompensationDto.getMtn()));
        arguments.add(concatKeyValue(PARAM_APPROVED_RATE, addtlCompensationDto.getApprovedRate().toString(), "long"));
        arguments.add(concatKeyValue(PARAM_BILLING_START_DATE, startDate, "date"));
        arguments.add(concatKeyValue(PARAM_BILLING_END_DATE, endDate, "date"));
        arguments.add(concatKeyValue(PARAM_PRICING_CONDITION, addtlCompensationDto.getPricingCondition()));

        boolean hasAdjusted = dataFlowJdbcJobExecutionDao.countFinalizeJobInstances(MeterProcessType.ADJUSTED, startDate, endDate) > 0;
        Preconditions.checkState(hasAdjusted || dataFlowJdbcJobExecutionDao.countFinalizeJobInstances(MeterProcessType.FINAL, startDate, endDate) > 0,
                "GMR should be finalized for billing period [".concat(startDate).concat(" to ").concat(endDate).concat("]"));

        checkTimeValidity(endDate);

        properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(
                hasAdjusted ? "monthlyAdjustedAddtlCompCalculation" : "monthlyFinalAddtlCompCalculation")));

        LOG.debug("Running job name={}, properties={}, arguments={}", ADDTL_COMP_JOB_NAME, properties, arguments);
        launchJob(ADDTL_COMP_TASK_NAME, properties, arguments);

        if (batchJobRunLockRepository.lockJob(ADDTL_COMP_JOB_NAME) == 0) {
            BatchJobRunLock batchJobRunLock = new BatchJobRunLock();
            batchJobRunLock.setJobName(ADDTL_COMP_JOB_NAME);
            batchJobRunLock.setLocked(true);
            batchJobRunLock.setLockedDate(new Date());
            batchJobRunLockRepository.save(batchJobRunLock);
        }
    }

    private void checkTimeValidity(String billingEndDate) {
        LocalDateTime endDate;
        try {
            endDate = DateUtil.getStartRangeDate(billingEndDate);
            LocalDateTime dateDeterminant = LocalDateTime.now().minusMonths(ADDTL_COMP_MONTH_VALIDITY);

            Preconditions.checkState(endDate.isAfter(dateDeterminant),
                    "Billing period is now invalid due to ".concat(String.valueOf(ADDTL_COMP_MONTH_VALIDITY)).concat("-month limit"));
        } catch (ParseException e) {
            LOG.error("Unable to parse endDate", e);
        }
    }

}
