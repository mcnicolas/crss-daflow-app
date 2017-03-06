package com.pemc.crss.dataflow.app.service.impl;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.pemc.crss.dataflow.app.dto.TaskExecutionDto;
import com.pemc.crss.dataflow.app.dto.TaskRunDto;
import com.pemc.crss.shared.commons.reference.MeterProcessType;
import com.pemc.crss.shared.commons.util.DateUtil;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobAddtlParams;
import com.pemc.crss.shared.core.dataflow.repository.BatchJobAddtlParamsRepository;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.Date;
import java.util.List;

import static java.util.stream.Collectors.toList;

@Service("settlementTaskExecutionService")
@Transactional(readOnly = true, value = "transactionManager")
public class SettlementTaskExecutionServiceImpl extends AbstractTaskExecutionService {

    private static final Logger LOG = LoggerFactory.getLogger(SettlementTaskExecutionServiceImpl.class);

    private static final String RUN_COMPUTE_STL_JOB_NAME = "computeSettlement";
    private static final String RUN_GENERATE_INVOICE_STL_JOB_NAME = "generateInvoiceSettlement";
    private static final String RUN_FINALIZE_STL_JOB_NAME = "finalizeSettlement";
    private static final String RUN_STL_READY_JOB_NAME = "processStlReady";
    private static final String AMS_INVOICE_DATE = "amsInvoiceDate";
    private static final String AMS_DUE_DATE = "amsDueDate";
    private static final String AMS_REMARKS_INV = "amsRemarksInv";
    private static final String AMS_REMARKS_MF = "amsRemarksMf";

    @Autowired
    private BatchJobAddtlParamsRepository batchJobAddtlParamsRepository;

    @Override
    public Page<TaskExecutionDto> findJobInstances(Pageable pageable) {
        List<TaskExecutionDto> taskExecutionDtos = jobExplorer.findJobInstancesByJobName(RUN_STL_READY_JOB_NAME.concat("*"),
                pageable.getOffset(), pageable.getPageSize()).stream()
                .map((JobInstance jobInstance) -> {
                    JobExecution jobExecution = getJobExecutions(jobInstance).iterator().next();
                    BatchStatus jobStatus = jobExecution.getStatus();

                    String parentId = jobInstance.getJobName().split("-")[1];
                    if (StringUtils.isEmpty(parentId)) {
                        LOG.warn("Parent id not appended for job instance id {}. Setting parent as self..", jobInstance.getId());
                        parentId = String.valueOf(jobInstance.getInstanceId());
                    }
                    JobInstance parentJob = jobExplorer.getJobInstance(Long.parseLong(parentId));
                    JobExecution parentExecutions = getJobExecutions(parentJob).iterator().next();

                    TaskExecutionDto taskExecutionDto = new TaskExecutionDto();
                    taskExecutionDto.setId(parentJob.getId());
                    taskExecutionDto.setRunDateTime(parentExecutions.getStartTime());
                    taskExecutionDto.setParams(Maps.transformValues(
                            parentExecutions.getJobParameters().getParameters(), JobParameter::getValue));
                    taskExecutionDto.setStatus(convertStatus(jobStatus, "SETTLEMENT"));
                    taskExecutionDto.setSettlementStatus(jobStatus);

                    List<JobInstance> calculationJobs = jobExplorer.findJobInstancesByJobName(
                            RUN_COMPUTE_STL_JOB_NAME.concat("*-").concat(parentId), 0, 1);

                    Date calculationEndTime = null;
                    if (!calculationJobs.isEmpty()) {
                        JobExecution calculationJobExecution = getJobExecutions(calculationJobs.get(0)).iterator().next();
                        BatchStatus calculationStatus = calculationJobExecution.getStatus();
                        calculationEndTime = calculationJobExecution.getEndTime();


                        taskExecutionDto.setCalculationStatus(calculationStatus);

                        if (calculationStatus.isUnsuccessful()) {
                            taskExecutionDto.setExitMessage(processFailedMessage(calculationJobExecution));
                        }
                        if (!calculationStatus.isRunning() && calculationEndTime != null) {
                            taskExecutionDto.setStatusDetails("Ended as of ".concat(dateTimeFormat.format(calculationEndTime)));
                        }
                        taskExecutionDto.setStatus(convertStatus(calculationStatus, "CALCULATION"));
                    }

                    List<JobInstance> generateInvoiceJobs = jobExplorer.findJobInstancesByJobName(
                            RUN_GENERATE_INVOICE_STL_JOB_NAME.concat("*-").concat(parentId), 0, 1);

                    if (!generateInvoiceJobs.isEmpty()) {
                        JobExecution invoiceGenJobExecution = getJobExecutions(generateInvoiceJobs.get(0)).iterator().next();
                        BatchStatus invoiceGenStatus = invoiceGenJobExecution.getStatus();
                        Date invoiceGenEndDate = invoiceGenJobExecution.getEndTime();

                        if (!invoiceGenEndDate.before(calculationEndTime)) {
                            taskExecutionDto.setTaggingStatus(invoiceGenStatus);

                            if (invoiceGenStatus.isUnsuccessful()) {
                                taskExecutionDto.setExitMessage(processFailedMessage(invoiceGenJobExecution));
                            }
                            if (!invoiceGenStatus.isRunning() && invoiceGenEndDate != null) {
                                taskExecutionDto.setStatusDetails("Ended as of ".concat(dateTimeFormat.format(invoiceGenEndDate)));
                            }
                            taskExecutionDto.setStatus(convertStatus(invoiceGenStatus, "TAGGING"));
                        }
                    }

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

        if (RUN_COMPUTE_STL_JOB_NAME.equals(taskRunDto.getJobName())) {
            String type = taskRunDto.getMeterProcessType();
            if (type == null) {
                type = PROCESS_TYPE_DAILY;
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("dailyCalculation")));
                arguments.add(concatKeyValue(START_DATE, taskRunDto.getTradingDate(), "date"));
            } else {
                if (MeterProcessType.ADJUSTED.name().equals(type)) {
                    properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyAdjustedCalculation")));
                } else if (MeterProcessType.PRELIMINARY.name().equals(type) || MeterProcessType.PRELIM.name().equals(type)) {
                    properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyPrelimCalculation")));
                } else if (MeterProcessType.FINAL.name().equals(type)) {
                    properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyFinalCalculation")));
                }
                arguments.add(concatKeyValue(START_DATE, taskRunDto.getStartDate(), "date"));
                arguments.add(concatKeyValue(END_DATE, taskRunDto.getEndDate(), "date"));
            }
            arguments.add(concatKeyValue(RUN_ID, String.valueOf(System.currentTimeMillis()), "long"));
            arguments.add(concatKeyValue(PARENT_JOB, taskRunDto.getParentJob(), "long"));
            arguments.add(concatKeyValue(PROCESS_TYPE, type));
            jobName = "crss-settlement-task-calculation";
        } else if (RUN_GENERATE_INVOICE_STL_JOB_NAME.equals(taskRunDto.getJobName())) {
            String type = taskRunDto.getMeterProcessType();
            if (MeterProcessType.ADJUSTED.name().equals(type)) {
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyAdjustedInvoiceGeneration")));
            } else if (MeterProcessType.PRELIMINARY.name().equals(type) || "PRELIM".equals(type)) {
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyPrelimInvoiceGeneration")));
            } else if (MeterProcessType.FINAL.name().equals(type)) {
//                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyFinalInvoiceGeneration")));
            }
            arguments.add(concatKeyValue(RUN_ID, String.valueOf(System.currentTimeMillis()), "long"));
            arguments.add(concatKeyValue(PARENT_JOB, taskRunDto.getParentJob(), "long"));
            arguments.add(concatKeyValue(PROCESS_TYPE, type));
            arguments.add(concatKeyValue(START_DATE, taskRunDto.getStartDate(), "date"));
            arguments.add(concatKeyValue(END_DATE, taskRunDto.getEndDate(), "date"));
            jobName = "crss-settlement-task-invoice-generation";
        } else if (RUN_FINALIZE_STL_JOB_NAME.equals(taskRunDto.getJobName())) {
            String type = taskRunDto.getMeterProcessType();
            if (MeterProcessType.FINAL.name().equals(type)) {
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyFinalInvoiceGeneration")));
            }
            final Long runId = System.currentTimeMillis();
            arguments.add(concatKeyValue(RUN_ID, String.valueOf(runId), "long"));
            arguments.add(concatKeyValue(PARENT_JOB, taskRunDto.getParentJob(), "long"));
            arguments.add(concatKeyValue(PROCESS_TYPE, type));
            arguments.add(concatKeyValue(START_DATE, taskRunDto.getStartDate(), "date"));
            arguments.add(concatKeyValue(END_DATE, taskRunDto.getEndDate(), "date"));

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
            jobName = "crss-settlement-task-invoice-generation";
        }
        LOG.debug("Running job name={}, properties={}, arguments={}", taskRunDto.getJobName(), properties, arguments);

        if (jobName != null) {
            LOG.debug("Running job name={}, properties={}, arguments={}", taskRunDto.getJobName(), properties, arguments);
            doLaunchAndLockJob(taskRunDto, jobName, properties, arguments);
        }
    }


}
