package com.pemc.crss.dataflow.app.service.impl;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.pemc.crss.dataflow.app.dto.TaskExecutionDto;
import com.pemc.crss.dataflow.app.dto.TaskRunDto;
import com.pemc.crss.shared.commons.reference.MeterProcessType;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobAddtlParams;
import com.pemc.crss.shared.core.dataflow.repository.BatchJobAddtlParamsRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import static java.util.stream.Collectors.toList;

/**
 * Created on 1/22/17.
 */
@Service("meterprocessTaskExecutionService")
@Transactional(readOnly = true, value = "transactionManager")
public class MeterprocessTaskExecutionServiceImpl extends AbstractTaskExecutionService {

    private static final Logger LOG = LoggerFactory.getLogger(MeterprocessTaskExecutionServiceImpl.class);

    private static final String RUN_WESM_JOB_NAME = "computeWesmMq";
    private static final String RUN_RCOA_JOB_NAME = "computeRcoaMq";
    private static final String RUN_STL_READY_JOB_NAME = "processStlReady";
    private static final String RUN_MQ_REPORT_JOB_NAME = "geneReport";
    private static final String PARAMS_BILLING_PERIOD_ID = "billingPeriodId";
    private static final String PARAMS_BILLING_PERIOD = "billingPeriod";
    private static final String PARAMS_SUPPLY_MONTH = "supplyMonth";
    private static final String PARAMS_BILLING_PERIOD_NAME = "billingPeriodName";

    @Autowired
    private BatchJobAddtlParamsRepository batchJobAddtlParamsRepository;


    @Override
    public Page<TaskExecutionDto> findJobInstances(Pageable pageable) {
        int count = 0;

        try {
            count = jobExplorer.getJobInstanceCount(RUN_WESM_JOB_NAME.concat("Daily"));
            count += jobExplorer.getJobInstanceCount(RUN_WESM_JOB_NAME.concat("Monthly"));
        } catch (NoSuchJobException e) {
            LOG.error("Exception: " + e);
        }

        List<TaskExecutionDto> taskExecutionDtos = Lists.newArrayList();

        if (count > 0) {
            taskExecutionDtos = jobExplorer.findJobInstancesByJobName(RUN_WESM_JOB_NAME.concat("*"),
                    pageable.getOffset(), pageable.getPageSize()).stream()
                    .map((JobInstance jobInstance) -> {

                        JobExecution jobExecution = getJobExecutions(jobInstance).iterator().next();

                        TaskExecutionDto taskExecutionDto = new TaskExecutionDto();
                        taskExecutionDto.setId(jobInstance.getId());
                        taskExecutionDto.setRunDateTime(jobExecution.getStartTime());
                        taskExecutionDto.setParams(Maps.transformValues(
                                jobExecution.getJobParameters().getParameters(), JobParameter::getValue));
                        taskExecutionDto.setWesmStatus(jobExecution.getStatus());

                        if (taskExecutionDto.getWesmStatus().isRunning()) {
                            calculateProgress(jobExecution, taskExecutionDto);
                        } else if (taskExecutionDto.getWesmStatus().isUnsuccessful()) {
                            taskExecutionDto.setExitMessage(processFailedMessage(jobExecution));
                        } else if (taskExecutionDto.getWesmStatus() == BatchStatus.COMPLETED) {
                            taskExecutionDto.getSummary().put(RUN_WESM_JOB_NAME, showSummary(jobExecution));
                        }

                        taskExecutionDto.setStatus(convertStatus(taskExecutionDto.getWesmStatus(), "WESM"));

                        List<JobInstance> rcoaJobs = jobExplorer.findJobInstancesByJobName(
                                RUN_RCOA_JOB_NAME.concat("*-")
                                        .concat(jobInstance.getId().toString()), 0, 1);

                        if (!rcoaJobs.isEmpty()) {
                            JobExecution rcoaJobExecution = getJobExecutions(rcoaJobs.get(0)).iterator().next();
                            taskExecutionDto.setRcoaStatus(rcoaJobExecution.getStatus());

                            if (taskExecutionDto.getRcoaStatus().isRunning()) {
                                calculateProgress(rcoaJobExecution, taskExecutionDto);
                            } else if (taskExecutionDto.getRcoaStatus().isUnsuccessful()) {
                                taskExecutionDto.setExitMessage(processFailedMessage(rcoaJobExecution));
                            } else if (taskExecutionDto.getRcoaStatus() == BatchStatus.COMPLETED) {
                                taskExecutionDto.getSummary().put(RUN_RCOA_JOB_NAME, showSummary(rcoaJobExecution));
                            }

                            taskExecutionDto.setStatus(convertStatus(taskExecutionDto.getRcoaStatus(), "RCOA"));
                        }

                        List<JobInstance> mqReportJobs = jobExplorer.findJobInstancesByJobName(
                                RUN_MQ_REPORT_JOB_NAME.concat("*-").concat(jobInstance.getId().toString()), 0, 1);

                        if (!mqReportJobs.isEmpty()) {
                            JobExecution mqReportJobExecution = getJobExecutions(mqReportJobs.get(0)).iterator().next();
                            taskExecutionDto.setMqReportStatus(mqReportJobExecution.getStatus());

                            if (taskExecutionDto.getMqReportStatus().isUnsuccessful()) {
                                taskExecutionDto.setExitMessage(processFailedMessage(mqReportJobExecution));
                            } else if (taskExecutionDto.getMqReportStatus() == BatchStatus.COMPLETED) {
                                taskExecutionDto.getSummary().put(RUN_MQ_REPORT_JOB_NAME, showSummary(mqReportJobExecution));
                            }
                        }

                        List<JobInstance> settlementJobs = jobExplorer.findJobInstancesByJobName(
                                RUN_STL_READY_JOB_NAME.concat("*-").concat(jobInstance.getId().toString()), 0, 1);

                        if (!settlementJobs.isEmpty()) {
                            JobExecution settlementJobExecution = getJobExecutions(settlementJobs.get(0)).iterator().next();
                            taskExecutionDto.setSettlementStatus(settlementJobExecution.getStatus());

                            if (taskExecutionDto.getSettlementStatus().isUnsuccessful()) {
                                taskExecutionDto.setExitMessage(processFailedMessage(settlementJobExecution));
                            } else if (taskExecutionDto.getSettlementStatus() == BatchStatus.COMPLETED) {
                                taskExecutionDto.getSummary().put(RUN_STL_READY_JOB_NAME, showSummary(settlementJobExecution));
                            }
                            taskExecutionDto.setStatus(convertStatus(taskExecutionDto.getSettlementStatus(), "SETTLEMENT"));
                        }

                        return taskExecutionDto;

                    }).collect(toList());
        }
        return new PageImpl<>(taskExecutionDtos, pageable, count);
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

        if (RUN_WESM_JOB_NAME.equals(taskRunDto.getJobName())) {
            final Long runId = System.currentTimeMillis();
            if (PROCESS_TYPE_DAILY.equals(taskRunDto.getMeterProcessType())) {
                arguments.add(concatKeyValue(DATE, taskRunDto.getTradingDate(), "date"));
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("dailyMq")));
            } else {
                String processType = taskRunDto.getMeterProcessType();
                if (!processType.equalsIgnoreCase(MeterProcessType.PRELIM.name())) {
                    String processBefore = processType.equalsIgnoreCase(MeterProcessType.FINAL.name()) ?
                            MeterProcessType.PRELIM.name() : MeterProcessType.FINAL.name();
                    checkProcessTypeState(processBefore, taskRunDto.getStartDate(), taskRunDto.getEndDate(), RUN_WESM_JOB_NAME);
                }
                arguments.add(concatKeyValue(START_DATE, taskRunDto.getStartDate(), "date"));
                arguments.add(concatKeyValue(END_DATE, taskRunDto.getEndDate(), "date"));
                arguments.add(concatKeyValue(PROCESS_TYPE, processType));

                List<BatchJobAddtlParams> addtlParams = new ArrayList<>();

                BatchJobAddtlParams paramsBillingPeriodId = new BatchJobAddtlParams();
                paramsBillingPeriodId.setRunId(runId);
                paramsBillingPeriodId.setType("LONG");
                paramsBillingPeriodId.setKey(PARAMS_BILLING_PERIOD_ID);
                paramsBillingPeriodId.setLongVal(taskRunDto.getBillingPeriodId());
                addtlParams.add(paramsBillingPeriodId);

                BatchJobAddtlParams paramsBillingPeriod = new BatchJobAddtlParams();
                paramsBillingPeriod.setRunId(runId);
                paramsBillingPeriod.setType("LONG");
                paramsBillingPeriod.setKey(PARAMS_BILLING_PERIOD);
                paramsBillingPeriod.setLongVal(taskRunDto.getBillingPeriod());
                addtlParams.add(paramsBillingPeriod);

                BatchJobAddtlParams paramsSupplyMonth = new BatchJobAddtlParams();
                paramsSupplyMonth.setRunId(runId);
                paramsSupplyMonth.setType("STRING");
                paramsSupplyMonth.setKey(PARAMS_SUPPLY_MONTH);
                paramsSupplyMonth.setStringVal(taskRunDto.getSupplyMonth());
                addtlParams.add(paramsSupplyMonth);

                BatchJobAddtlParams paramsBillingPeriodName = new BatchJobAddtlParams();
                paramsBillingPeriodName.setRunId(runId);
                paramsBillingPeriodName.setType("STRING");
                paramsBillingPeriodName.setKey(PARAMS_BILLING_PERIOD_NAME);
                paramsBillingPeriodName.setStringVal(taskRunDto.getBillingPeriodName());
                addtlParams.add(paramsBillingPeriodName);

                batchJobAddtlParamsRepository.save(addtlParams);

                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyMq")));
            }
            arguments.add(concatKeyValue(RUN_ID, String.valueOf(runId), "long"));
            arguments.add(concatKeyValue(METER_TYPE, "MIRF_MT_WESM"));
            jobName = "crss-meterprocess-task-mqcomputation";
        } else if (taskRunDto.getParentJob() != null) {
            JobInstance jobInstance = jobExplorer.getJobInstance(Long.valueOf(taskRunDto.getParentJob()));
            JobParameters jobParameters = getJobExecutions(jobInstance).get(0).getJobParameters();
            boolean isDaily = jobParameters.getString(PROCESS_TYPE) == null;
            if (isDaily) {
                arguments.add(concatKeyValue(DATE, dateFormat.format(jobParameters.getDate(DATE)), "date"));
            } else {
                arguments.add(concatKeyValue(START_DATE, dateFormat.format(jobParameters.getDate(START_DATE)), "date"));
                arguments.add(concatKeyValue(END_DATE, dateFormat.format(jobParameters.getDate(END_DATE)), "date"));
                arguments.add(concatKeyValue(PROCESS_TYPE, jobParameters.getString(PROCESS_TYPE)));
            }
            arguments.add(concatKeyValue(PARENT_JOB, taskRunDto.getParentJob(), "long"));
            if (RUN_RCOA_JOB_NAME.equals(taskRunDto.getJobName())) {
                arguments.add(concatKeyValue(METER_TYPE, "MIRF_MT_RCOA"));
                if (isDaily) {
                    properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("dailyMq")));
                } else {
                    properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyMq")));
                }
                jobName = "crss-meterprocess-task-mqcomputation";
            } else if (RUN_STL_READY_JOB_NAME.equals(taskRunDto.getJobName())) {
                if (PROCESS_TYPE_DAILY.equals(taskRunDto.getMeterProcessType())) {
                    properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("dailyMq")));
                } else if (MeterProcessType.PRELIM.name().equals(taskRunDto.getMeterProcessType())) {
                    properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyPrelim")));
                } else if (MeterProcessType.FINAL.name().equals(taskRunDto.getMeterProcessType())) {
                    checkProcessTypeState(MeterProcessType.PRELIM.name(), dateFormat.format(jobParameters.getDate(START_DATE)),
                            dateFormat.format(jobParameters.getDate(END_DATE)), RUN_STL_READY_JOB_NAME);
                    properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyFinal")));
                } else if (MeterProcessType.ADJUSTED.name().equals(taskRunDto.getMeterProcessType())) {
                    checkProcessTypeState(MeterProcessType.FINAL.name(), dateFormat.format(jobParameters.getDate(START_DATE)),
                            dateFormat.format(jobParameters.getDate(END_DATE)), RUN_STL_READY_JOB_NAME);
                    properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyAdjusted")));
                }
                jobName = "crss-meterprocess-task-stlready";
            } else if (RUN_MQ_REPORT_JOB_NAME.equals(taskRunDto.getJobName())) {
                if (PROCESS_TYPE_DAILY.equals(taskRunDto.getMeterProcessType())) {
                    properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("dailyMqReport")));
                } else {
                    properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyMqReport")));
                }
                arguments.remove(concatKeyValue(PROCESS_TYPE, taskRunDto.getMeterProcessType()));
                jobName = "crss-meterprocess-task-mqcomputation";
            }
        }

        LOG.debug("Running job name={}, properties={}, arguments={}", taskRunDto.getJobName(), properties, arguments);

        if (jobName != null) {
            LOG.debug("Running job name={}, properties={}, arguments={}", taskRunDto.getJobName(), properties, arguments);
            doLaunchAndLockJob(taskRunDto, jobName, properties, arguments);
        }
    }


    private void checkProcessTypeState(String process, String startDate, String endDate, String jobName) {
        String errMsq = "Must run " + process + " first!";
        Preconditions.checkState(executionParamRepository.countMonthlyRun(startDate, endDate, process, jobName) > 0, errMsq);
    }


}
