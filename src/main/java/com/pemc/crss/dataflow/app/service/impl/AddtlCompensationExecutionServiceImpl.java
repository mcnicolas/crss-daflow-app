package com.pemc.crss.dataflow.app.service.impl;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.pemc.crss.dataflow.app.dto.AddtlCompensationExecutionDto;
import com.pemc.crss.dataflow.app.dto.AddtlCompensationRunDto;
import com.pemc.crss.dataflow.app.dto.BaseTaskExecutionDto;
import com.pemc.crss.dataflow.app.dto.TaskRunDto;
import com.pemc.crss.dataflow.app.support.PageableRequest;
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
import java.util.*;
import java.util.stream.Collectors;

@Service("addtlCompensationExecutionService")
@Transactional(readOnly = true, value = "transactionManager")
public class AddtlCompensationExecutionServiceImpl extends AbstractTaskExecutionService {

    private static final Logger LOG = LoggerFactory.getLogger(AddtlCompensationExecutionServiceImpl.class);

    private static final String ADDTL_COMP_JOB_NAME = "potato";
    private static final String ADDTL_COMP_TASK_NAME = "tomato";

    private static final String PARAM_BILLING_ID = "billingId";
    private static final String PARAM_MTN = "mtn";
    private static final String PARAM_APPROVED_RATE = "approvedRate";
    private static final String PARAM_BILLING_START_DATE = "billingStartDate";
    private static final String PARAM_BILLING_END_DATE = "billingEndDate";
    private static final String PARAM_PRICING_CONDITION = "pricingCondition";

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

        // temp code start
        Map<String, Object> mapParams = Maps.newHashMap();
        mapParams.put("billingId", "GENCO1");
        mapParams.put("mtn", "GEN1");
        mapParams.put("approvedRate", 123.45);
        mapParams.put("billingStartDate", new Date());
        mapParams.put("billingEndDate", new Date());
        mapParams.put("pricingCondition", "SEC");

        AddtlCompensationExecutionDto addtlCompensationExecutionDto = new AddtlCompensationExecutionDto();
        addtlCompensationExecutionDto.setId(1L);
        addtlCompensationExecutionDto.setStatus("COMPLETED");
        addtlCompensationExecutionDto.setParams(mapParams);
        addtlCompensationExecutionDto.setExitMessage("exit message");
        addtlCompensationExecutionDto.setProgress(null);
        addtlCompensationExecutionDto.setStatusDetails("status details");
        addtlCompensationExecutionDto.setRunDateTime(new Date());

        return new PageImpl<>(Lists.newArrayList(addtlCompensationExecutionDto), pageable, 1);
        // temp code end

//        return new PageImpl<>(addtlCompensationExecutionDtoList, pageable, totalSize);
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

        final Long runId = System.currentTimeMillis();
        arguments.add(concatKeyValue(RUN_ID, String.valueOf(runId), "long"));
        arguments.add(concatKeyValue(PARAM_BILLING_ID, addtlCompensationDto.getBillingId()));
        arguments.add(concatKeyValue(PARAM_MTN, addtlCompensationDto.getMtn()));
        arguments.add(concatKeyValue(PARAM_APPROVED_RATE, addtlCompensationDto.getApprovedRate().toString(), "long"));
        arguments.add(concatKeyValue(PARAM_BILLING_START_DATE, addtlCompensationDto.getBillingStartDate(), "date"));
        arguments.add(concatKeyValue(PARAM_BILLING_END_DATE, addtlCompensationDto.getBillingEndDate(), "date"));
        arguments.add(concatKeyValue(PARAM_PRICING_CONDITION, addtlCompensationDto.getPricingCondition()));

        properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("sampleProfile")));

        LOG.debug("Running job name={}, properties={}, arguments={}", ADDTL_COMP_JOB_NAME, properties, arguments);
//        launchJob(ADDTL_COMP_TASK_NAME, properties, arguments);
//
//        if (batchJobRunLockRepository.lockJob(ADDTL_COMP_JOB_NAME) == 0) {
//            BatchJobRunLock batchJobRunLock = new BatchJobRunLock();
//            batchJobRunLock.setJobName(ADDTL_COMP_JOB_NAME);
//            batchJobRunLock.setLocked(true);
//            batchJobRunLock.setLockedDate(new Date());
//            batchJobRunLockRepository.save(batchJobRunLock);
//        }
    }

}
