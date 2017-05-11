package com.pemc.crss.dataflow.app.service.impl;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.pemc.crss.dataflow.app.dto.*;
import com.pemc.crss.dataflow.app.support.PageableRequest;
import com.pemc.crss.shared.commons.reference.MeterProcessType;
import com.pemc.crss.shared.commons.util.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

import static com.pemc.crss.shared.commons.util.TaskUtil.*;

@Service("addtlCompensationExecutionService")
@Transactional(readOnly = true, value = "transactionManager")
public class AddtlCompensationExecutionServiceImpl extends AbstractTaskExecutionService {

    private static final Logger LOG = LoggerFactory.getLogger(AddtlCompensationExecutionServiceImpl.class);

    private static final String ADDTL_COMP_JOB_NAME = "calculateAddtlComp";
    private static final String ADDTL_COMP_TASK_NAME = "crss-settlement-task-calculation-addtlcomp";

    private static final long ADDTL_COMP_MONTH_VALIDITY = 24;

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

                                    AddtlCompensationExecDetailsDto addtlCompensationExecDetailsDto = new AddtlCompensationExecDetailsDto();
                                    addtlCompensationExecDetailsDto.setRunId(parameters.getLong(RUN_ID));
                                    addtlCompensationExecDetailsDto.setBillingId(parameters.getString(AC_BILLING_ID));
                                    addtlCompensationExecDetailsDto.setMtn(parameters.getString(AC_MTN));
                                    addtlCompensationExecDetailsDto.setApprovedRate(BigDecimal.valueOf(parameters.getDouble(AC_APPROVED_RATE)));
                                    addtlCompensationExecDetailsDto.setStatus(jobExecution.getStatus().name());

                                    addtlCompensationExecDetailsDtos.add(addtlCompensationExecDetailsDto);
                                }
                            });
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

    public void launchAddtlCompensation(AddtlCompensationRunDto addtlCompensationDto) throws URISyntaxException {
        String startDate = addtlCompensationDto.getBillingStartDate();
        String endDate = addtlCompensationDto.getBillingEndDate();

        boolean hasAdjusted = dataFlowJdbcJobExecutionDao.countFinalizeJobInstances(MeterProcessType.ADJUSTED, startDate, endDate) > 0;
        Preconditions.checkState(hasAdjusted || dataFlowJdbcJobExecutionDao.countFinalizeJobInstances(MeterProcessType.FINAL, startDate, endDate) > 0,
                "GMR should be finalized for billing period [".concat(startDate).concat(" to ").concat(endDate).concat("]"));

        checkTimeValidity(endDate);

        List<String> properties = Lists.newArrayList();
        List<String> arguments = Lists.newArrayList();

        final Long runId = System.currentTimeMillis();
        arguments.add(concatKeyValue(RUN_ID, String.valueOf(runId), "long"));
        arguments.add(concatKeyValue(AC_BILLING_ID, addtlCompensationDto.getBillingId()));
        arguments.add(concatKeyValue(AC_MTN, addtlCompensationDto.getMtn()));
        arguments.add(concatKeyValue(AC_APPROVED_RATE, addtlCompensationDto.getApprovedRate().toString(), "double"));
        arguments.add(concatKeyValue(START_DATE, startDate, "date"));
        arguments.add(concatKeyValue(END_DATE, endDate, "date"));
        arguments.add(concatKeyValue(AC_PRICING_CONDITION, addtlCompensationDto.getPricingCondition()));
        arguments.add(concatKeyValue(USERNAME, addtlCompensationDto.getCurrentUser()));

        properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(
                hasAdjusted ? "monthlyAdjustedAddtlCompCalculation" : "monthlyFinalAddtlCompCalculation")));

        LOG.debug("Running job name={}, properties={}, arguments={}", ADDTL_COMP_JOB_NAME, properties, arguments);
        launchJob(ADDTL_COMP_TASK_NAME, properties, arguments);

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
