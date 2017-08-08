package com.pemc.crss.dataflow.app.service.impl;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.pemc.crss.dataflow.app.dto.BaseTaskExecutionDto;
import com.pemc.crss.dataflow.app.dto.TaskExecutionDto;
import com.pemc.crss.dataflow.app.dto.TaskRunDto;
import com.pemc.crss.dataflow.app.dto.parent.GroupTaskExecutionDto;
import com.pemc.crss.dataflow.app.support.PageableRequest;
import com.pemc.crss.shared.commons.reference.MeterProcessType;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobAddtlParams;
import com.pemc.crss.shared.core.dataflow.repository.BatchJobAddtlParamsRepository;
import com.pemc.crss.shared.core.dataflow.service.BatchJobAddtlParamsService;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.LocalDateTime;
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
import java.util.*;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.StringUtils.EMPTY;

/**
 * Created on 1/22/17.
 */
@Service("meterprocessTaskExecutionService")
@Transactional(readOnly = true, value = "transactionManager")
public class MeterprocessTaskExecutionServiceImpl extends AbstractTaskExecutionService {

    private static final Logger LOG = LoggerFactory.getLogger(MeterprocessTaskExecutionServiceImpl.class);

    private static final String RUN_WESM_JOB_NAME = "computeWesmMq";
    private static final String RUN_RCOA_JOB_NAME = "computeRcoaMq";
    private static final String RUN_STL_NOT_READY_JOB_NAME = "stlNotReady";
    private static final String RUN_STL_READY_JOB_NAME = "stlReady";
    private static final String RUN_MQ_REPORT_JOB_NAME = "genReport";
    private static final String PARAMS_BILLING_PERIOD_ID = "billingPeriodId";
    private static final String PARAMS_BILLING_PERIOD = "billingPeriod";
    private static final String PARAMS_SUPPLY_MONTH = "supplyMonth";
    private static final String PARAMS_BILLING_PERIOD_NAME = "billingPeriodName";
    private static final String MQ_REPORT_STAT_AFTER_FINALIZE = "mqReportStatusAfterFinalized";

    @Autowired
    private BatchJobAddtlParamsRepository batchJobAddtlParamsRepository;

    @Autowired
    private BatchJobAddtlParamsService batchJobAddtlParamsService;


    @Override
    public Page<TaskExecutionDto> findJobInstances(Pageable pageable) {
        int count = 0;

        try {
            count = jobExplorer.getJobInstanceCount(RUN_WESM_JOB_NAME.concat("Daily"));
            count += jobExplorer.getJobInstanceCount(RUN_WESM_JOB_NAME.concat("Monthly"));
        } catch (NoSuchJobException e) {
            LOG.error("Exception: " + e);
        }

        return new PageImpl<>(getTaskExecutionDtos(count, pageable, 1), pageable, count);
    }

    @Override
    public Page<GroupTaskExecutionDto> findJobInstancesGroupByBillingPeriod(Pageable pageable) {

        List<GroupTaskExecutionDto> groupTaskExecutionDtos = Lists.newArrayList();

        int count = 0;

        try {
            count = jobExplorer.getJobInstanceCount(RUN_WESM_JOB_NAME.concat("Daily"));
            count += jobExplorer.getJobInstanceCount(RUN_WESM_JOB_NAME.concat("Monthly"));
        } catch (NoSuchJobException e) {
            LOG.error("Exception: " + e);
        }

        List<TaskExecutionDto> taskExecutionDtos = getTaskExecutionDtos(count, pageable, 3);

        taskExecutionDtos.forEach(taskExecutionDto -> {
            String processType = taskExecutionDto.getParams().getOrDefault(PROCESS_TYPE, null) != null ? (String)(((JobParameter)taskExecutionDto.getParams().getOrDefault(PROCESS_TYPE, null)).getValue()) : null;
            Date date = taskExecutionDto.getParams().getOrDefault(DATE, null) != null ? (Date)(((JobParameter) taskExecutionDto.getParams().getOrDefault(DATE, null)).getValue()) : null;
            Date startDate = taskExecutionDto.getParams().getOrDefault(START_DATE, null) != null ? (Date)(((JobParameter) taskExecutionDto.getParams().getOrDefault(START_DATE, null)).getValue()) : null;
            Date endDate = taskExecutionDto.getParams().getOrDefault(END_DATE, null) != null ? (Date)(((JobParameter) taskExecutionDto.getParams().getOrDefault(END_DATE, null)).getValue()) : null;

            GroupTaskExecutionDto groupTaskExecutionDto;

            // create daily billing period group
            if (date != null && StringUtils.isEmpty(processType)) {
                groupTaskExecutionDto = new GroupTaskExecutionDto(date, EMPTY);
            } else {
                // create monthly billing period group
                Date billingPeriodStartDate = setBillingPeriodStartDate(startDate);
                Date billingPeriodEndDate = setBillingPeriodEndDate(endDate);
                groupTaskExecutionDto = new GroupTaskExecutionDto(null, billingPeriodStartDate, billingPeriodEndDate, processType);
            }

            // check if billing period group already exists
            int index = groupTaskExecutionDtos.indexOf(groupTaskExecutionDto);
            if (index > -1) {
                // get existing billing period group
                groupTaskExecutionDtos.get(index).getTaskExecutionDtoList().add(taskExecutionDto);
            } else {
                groupTaskExecutionDto.getTaskExecutionDtoList().add(taskExecutionDto);
                // add new billing period group
                groupTaskExecutionDtos.add(groupTaskExecutionDto);
            }
        });

        return new PageImpl<>(groupTaskExecutionDtos, pageable, count);
    }

    @Override
    public Page<? extends BaseTaskExecutionDto> findJobInstances(Pageable pageable, String type, String status, String mode,
                                                                 String runStartDate, String tradingStartDate, String tradingEndDate,
                                                                 String username) {
        return null;
    }

    @Override
    public Page<? extends BaseTaskExecutionDto> findJobInstances(PageableRequest pageableRequest) {
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
        if (RUN_WESM_JOB_NAME.equals(taskRunDto.getJobName())) {
            if (PROCESS_TYPE_DAILY.equals(taskRunDto.getMeterProcessType())) {
                checkFinalizeDailyState(taskRunDto.getTradingDate());
                arguments.add(concatKeyValue(DATE, taskRunDto.getTradingDate(), PARAMS_TYPE_DATE));
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(PROFILE_DAILY_MQ)));
            } else {
                String processType = taskRunDto.getMeterProcessType();
                if (!processType.equalsIgnoreCase(MeterProcessType.PRELIM.name())) {
                    String processBefore = processType.equalsIgnoreCase(MeterProcessType.FINAL.name()) ?
                            MeterProcessType.PRELIM.name() : MeterProcessType.FINAL.name();
                    checkProcessTypeState(processBefore, taskRunDto.getStartDate(), taskRunDto.getEndDate(), RUN_WESM_JOB_NAME);
                }
                if (!MeterProcessType.ADJUSTED.name().equals(processType)) {
                    checkFinalizeProcessTypeState(processType, taskRunDto.getStartDate(), taskRunDto.getEndDate());
                }
                arguments.add(concatKeyValue(START_DATE, taskRunDto.getStartDate(), PARAMS_TYPE_DATE));
                arguments.add(concatKeyValue(END_DATE, taskRunDto.getEndDate(), PARAMS_TYPE_DATE));
                arguments.add(concatKeyValue(PROCESS_TYPE, processType));

                List<BatchJobAddtlParams> addtlParams = new ArrayList<>();

                BatchJobAddtlParams paramsBillingPeriodId = new BatchJobAddtlParams();
                paramsBillingPeriodId.setRunId(runId);
                paramsBillingPeriodId.setType(PARAMS_TYPE_LONG);
                paramsBillingPeriodId.setKey(PARAMS_BILLING_PERIOD_ID);
                paramsBillingPeriodId.setLongVal(taskRunDto.getBillingPeriodId());
                addtlParams.add(paramsBillingPeriodId);

                BatchJobAddtlParams paramsBillingPeriod = new BatchJobAddtlParams();
                paramsBillingPeriod.setRunId(runId);
                paramsBillingPeriod.setType(PARAMS_TYPE_LONG);
                paramsBillingPeriod.setKey(PARAMS_BILLING_PERIOD);
                paramsBillingPeriod.setLongVal(taskRunDto.getBillingPeriod());
                addtlParams.add(paramsBillingPeriod);

                BatchJobAddtlParams paramsSupplyMonth = new BatchJobAddtlParams();
                paramsSupplyMonth.setRunId(runId);
                paramsSupplyMonth.setType(PARAMS_TYPE_STRING);
                paramsSupplyMonth.setKey(PARAMS_SUPPLY_MONTH);
                paramsSupplyMonth.setStringVal(taskRunDto.getSupplyMonth());
                addtlParams.add(paramsSupplyMonth);

                BatchJobAddtlParams paramsBillingPeriodName = new BatchJobAddtlParams();
                paramsBillingPeriodName.setRunId(runId);
                paramsBillingPeriodName.setType(PARAMS_TYPE_STRING);
                paramsBillingPeriodName.setKey(PARAMS_BILLING_PERIOD_NAME);
                paramsBillingPeriodName.setStringVal(taskRunDto.getBillingPeriodName());
                addtlParams.add(paramsBillingPeriodName);

                batchJobAddtlParamsRepository.save(addtlParams);

                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(PROFILE_MONTHLY_MQ)));
            }

            // temporarily save in additional params to lessen task id length in chronos
            if (StringUtils.isNotEmpty(taskRunDto.getMtns())) {
                BatchJobAddtlParams paramsSelectedMtns = new BatchJobAddtlParams();
                paramsSelectedMtns.setRunId(runId);
                paramsSelectedMtns.setType(PARAMS_TYPE_STRING);
                paramsSelectedMtns.setKey(MTNS);
                paramsSelectedMtns.setStringVal(taskRunDto.getMtns());
                batchJobAddtlParamsRepository.save(paramsSelectedMtns);
            }

            arguments.add(concatKeyValue(RUN_ID, String.valueOf(runId), PARAMS_TYPE_LONG));
            arguments.add(concatKeyValue(METER_TYPE, METER_TYPE_WESM));
            arguments.add(concatKeyValue(WESM_USERNAME, taskRunDto.getCurrentUser()));
            jobName = "crss-meterprocess-task-mqcomputation";
        } else if (taskRunDto.getParentJob() != null) {
            JobInstance jobInstance = jobExplorer.getJobInstance(Long.valueOf(taskRunDto.getParentJob()));
            JobParameters jobParameters = getJobExecutions(jobInstance).get(0).getJobParameters();
            String processType = jobParameters.getString(PROCESS_TYPE);
            boolean isDaily = processType== null;
            if (isDaily) {
                if (!RUN_MQ_REPORT_JOB_NAME.equals(taskRunDto.getJobName())) {
                    checkFinalizeDailyState(dateFormat.format(jobParameters.getDate(DATE)));
                }
                arguments.add(concatKeyValue(DATE, dateFormat.format(jobParameters.getDate(DATE)), PARAMS_TYPE_DATE));
            } else {
                if (!MeterProcessType.ADJUSTED.name().equals(processType)
                        && !RUN_MQ_REPORT_JOB_NAME.equals(taskRunDto.getJobName())) {
                    checkFinalizeProcessTypeState(processType, dateFormat.format(jobParameters.getDate(START_DATE)),
                            dateFormat.format(jobParameters.getDate(END_DATE)));
                }
                arguments.add(concatKeyValue(START_DATE, dateFormat.format(jobParameters.getDate(START_DATE)), PARAMS_TYPE_DATE));
                arguments.add(concatKeyValue(END_DATE, dateFormat.format(jobParameters.getDate(END_DATE)), PARAMS_TYPE_DATE));
                arguments.add(concatKeyValue(PROCESS_TYPE, jobParameters.getString(PROCESS_TYPE)));
            }
            arguments.add(concatKeyValue(PARENT_JOB, taskRunDto.getParentJob(), PARAMS_TYPE_LONG));
            if (RUN_RCOA_JOB_NAME.equals(taskRunDto.getJobName())) {
                arguments.add(concatKeyValue(METER_TYPE, METER_TYPE_RCOA));
                if (isDaily) {
                    properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(PROFILE_DAILY_MQ)));
                } else {
                    properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(PROFILE_MONTHLY_MQ)));
                    if (!processType.equalsIgnoreCase(MeterProcessType.PRELIM.name())) {
                        String processBefore = processType.equalsIgnoreCase(MeterProcessType.FINAL.name()) ?
                                MeterProcessType.PRELIM.name() : MeterProcessType.FINAL.name();
                        checkProcessTypeState(processBefore, dateFormat.format(jobParameters.getDate(START_DATE)),
                                dateFormat.format(jobParameters.getDate(END_DATE)), RUN_RCOA_JOB_NAME);
                    }
                }
                arguments.add(concatKeyValue(RCOA_USERNAME, taskRunDto.getCurrentUser()));
                jobName = "crss-meterprocess-task-mqcomputation";
            } else if (RUN_STL_NOT_READY_JOB_NAME.equals(taskRunDto.getJobName())) {
                if (PROCESS_TYPE_DAILY.equals(taskRunDto.getMeterProcessType())) {
                    properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(PROFILE_DAILY_MQ)));
                } else if (MeterProcessType.PRELIM.name().equals(taskRunDto.getMeterProcessType())) {
                    properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(PROFILE_MONTHLY_PRELIM)));
                } else if (MeterProcessType.FINAL.name().equals(taskRunDto.getMeterProcessType())) {
                    checkProcessTypeState(MeterProcessType.PRELIM.name(), dateFormat.format(jobParameters.getDate(START_DATE)),
                            dateFormat.format(jobParameters.getDate(END_DATE)), RUN_STL_NOT_READY_JOB_NAME);
                    properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(PROFILE_MONTHLY_FINAL)));
                } else if (MeterProcessType.ADJUSTED.name().equals(taskRunDto.getMeterProcessType())) {
                    checkProcessTypeState(MeterProcessType.FINAL.name(), dateFormat.format(jobParameters.getDate(START_DATE)),
                            dateFormat.format(jobParameters.getDate(END_DATE)), RUN_STL_NOT_READY_JOB_NAME);
                    properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(PROFILE_MONTHLY_ADJUSTED)));
                }
                arguments.add(concatKeyValue(STL_NOT_READY_USERNAME, taskRunDto.getCurrentUser()));
                jobName = "crss-meterprocess-task-stlready";
            } else if (RUN_STL_READY_JOB_NAME.equals(taskRunDto.getJobName())) {
                if (PROCESS_TYPE_DAILY.equals(taskRunDto.getMeterProcessType())) {
                    properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(PROFILE_STL_READY_DAILY)));
                } else if (MeterProcessType.PRELIM.name().equals(taskRunDto.getMeterProcessType())) {
                    properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(PROFILE_STL_READY_MONTHLY_PRELIM)));
                } else if (MeterProcessType.FINAL.name().equals(taskRunDto.getMeterProcessType())) {
                    checkProcessTypeState(MeterProcessType.PRELIM.name(), dateFormat.format(jobParameters.getDate(START_DATE)),
                            dateFormat.format(jobParameters.getDate(END_DATE)), RUN_STL_READY_JOB_NAME);
                    properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(PROFILE_STL_READY_MONTHLY_FINAL)));
                } else if (MeterProcessType.ADJUSTED.name().equals(taskRunDto.getMeterProcessType())) {
                    checkProcessTypeState(MeterProcessType.FINAL.name(), dateFormat.format(jobParameters.getDate(START_DATE)),
                            dateFormat.format(jobParameters.getDate(END_DATE)), RUN_STL_READY_JOB_NAME);
                    checkFinalizedAdjustmentState(jobParameters.getLong(RUN_ID), MeterProcessType.ADJUSTED.name(),
                            dateFormat.format(jobParameters.getDate(START_DATE)), dateFormat.format(jobParameters.getDate(END_DATE)));
                    properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(PROFILE_STL_READY_MONTHLY_ADJUSTED)));
                }
                arguments.add(concatKeyValue(RUN_ID, String.valueOf(runId), PARAMS_TYPE_LONG));
                arguments.add(concatKeyValue(STL_READY_USERNAME, taskRunDto.getCurrentUser()));
                if (taskRunDto.getBillingPeriod() != null) {
                    arguments.add(concatKeyValue("bp", String.valueOf(taskRunDto.getBillingPeriod()), PARAMS_TYPE_LONG));
                }
                jobName = "crss-meterprocess-task-stlready";
            } else if (RUN_MQ_REPORT_JOB_NAME.equals(taskRunDto.getJobName())) {
                if (PROCESS_TYPE_DAILY.equals(taskRunDto.getMeterProcessType())) {
                    properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(PROFILE_DAILY_MQ_REPORT)));
                } else {
                    properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(PROFILE_MONTHLY_MQ_REPORT)));
                }
                if (taskRunDto.getTradingDate() != null
                        && taskRunDto.getTradingDate().equals(BatchStatus.COMPLETED.name())) {
                    arguments.add(concatKeyValue(MQ_REPORT_STAT_AFTER_FINALIZE, BatchStatus.COMPLETED.name()));
                }
                arguments.add(concatKeyValue(MQ_REPORT_USERNAME, taskRunDto.getCurrentUser()));
                jobName = "crss-meterprocess-task-mqcomputation";
            }
        }

        LOG.debug("Running job name={}, properties={}, arguments={}", taskRunDto.getJobName(), properties, arguments);

        if (jobName != null) {
            LOG.debug("Running job name={}, properties={}, arguments={}", taskRunDto.getJobName(), properties, arguments);
            launchJob(jobName, properties, arguments);
            lockJob(taskRunDto);
        }
    }

    @Override
    public void relaunchFailedJob(long jobId) throws URISyntaxException {

    }

    private void checkProcessTypeState(String process, String startDate, String endDate, String jobName) {
        String errMsq = "Must run " + process + " first!";
        Preconditions.checkState(executionParamRepository.countMonthlyRun(startDate, endDate, process, jobName) > 0, errMsq);
    }

    private void checkFinalizeDailyState(String date) {
        String errMsq = "You already have a process finalized on the same date: " + date + " !";
        Preconditions.checkState(executionParamRepository.countDailyRunAllMtn(date, RUN_STL_READY_JOB_NAME) < 1, errMsq);
    }

    private void checkFinalizeProcessTypeState(String process, String startDate, String endDate) {
        String errMsq = "You already have a " + process + " finalized on the same billing period!";
        Preconditions.checkState(executionParamRepository.countMonthlyRunAllMtn(startDate, endDate, process, RUN_STL_READY_JOB_NAME) < 1, errMsq);
    }

    private void checkFinalizedAdjustmentState(Long parentRunId, String process, String startDate, String endDate) {
        String errMsq = "A finalized run with a later date already exist!";
        Preconditions.checkState(executionParamRepository.findLatestWesmRunIdMonthly(startDate, endDate, process, RUN_STL_READY_JOB_NAME) < parentRunId, errMsq);
    }

    private List<TaskExecutionDto> getTaskExecutionDtos(int count, Pageable pageable, int multiplier) {
        List<TaskExecutionDto> taskExecutionDtos = Lists.newArrayList();

        if (count > 0) {
            taskExecutionDtos = jobExplorer.findJobInstancesByJobName(RUN_WESM_JOB_NAME.concat("*"),
                    pageable.getOffset(), pageable.getPageSize() * multiplier).stream()
                    .map((JobInstance jobInstance) -> {

                        if (getJobExecutions(jobInstance).iterator().hasNext()) {
                            JobExecution jobExecution = getJobExecutions(jobInstance).iterator().next();

                            Map<String, Object> jobParameters = jobExecution.getJobParameters().getParameters()
                                    .entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                            jobParameters.put(SEINS, jobExecution.getExecutionContext().getString(SEINS, EMPTY));
                            String mtns = jobExecution.getExecutionContext().getString(MTNS, EMPTY);
                            if (StringUtils.isEmpty(mtns)) {
                                String mtnFromAddtlParams = batchJobAddtlParamsService.getBatchJobAddtlParamsStringVal(jobExecution.getJobParameters().getLong(RUN_ID), MTNS);
                                jobParameters.put(MTNS, StringUtils.isNotEmpty(mtnFromAddtlParams) ? mtnFromAddtlParams : EMPTY);
                            } else {
                                jobParameters.put(MTNS, mtns);
                            }
                            String wesmUser = jobParameters.getOrDefault(WESM_USERNAME, "").toString();

                            TaskExecutionDto taskExecutionDto = new TaskExecutionDto();
                            taskExecutionDto.setId(jobInstance.getId());
                            taskExecutionDto.setRunDateTime(jobExecution.getStartTime());
                            taskExecutionDto.setParams(jobParameters);
                            taskExecutionDto.setWesmStatus(jobExecution.getStatus());
                            taskExecutionDto.setWesmUser(wesmUser);

                            if (taskExecutionDto.getWesmStatus().isRunning()) {
                                calculateProgress(jobExecution, taskExecutionDto);
                            } else if (taskExecutionDto.getWesmStatus().isUnsuccessful()) {
                                taskExecutionDto.setExitMessage(processFailedMessage(jobExecution));
                            } else if (taskExecutionDto.getWesmStatus() == BatchStatus.COMPLETED) {
                                taskExecutionDto.getSummary().put(RUN_WESM_JOB_NAME, showSummary(jobExecution, null));
                            }

                            taskExecutionDto.setStatus(convertStatus(taskExecutionDto.getWesmStatus(), "WESM"));

                            List<JobInstance> rcoaJobs = jobExplorer.findJobInstancesByJobName(
                                    RUN_RCOA_JOB_NAME.concat("*-")
                                            .concat(jobInstance.getId().toString()), 0, 1);

                            if (!rcoaJobs.isEmpty()) {
                                JobExecution rcoaJobExecution = getJobExecutions(rcoaJobs.get(0)).iterator().next();

                                jobParameters = rcoaJobExecution.getJobParameters().getParameters()
                                        .entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                                jobParameters.put("seins", rcoaJobExecution.getExecutionContext().getString(SEINS, EMPTY));
                                String rcoaUser = jobParameters.getOrDefault(RCOA_USERNAME, "").toString();

                                taskExecutionDto.setRcoaStatus(rcoaJobExecution.getStatus());
                                taskExecutionDto.setRcoaUser(rcoaUser);

                                if (taskExecutionDto.getRcoaStatus().isRunning()) {
                                    calculateProgress(rcoaJobExecution, taskExecutionDto);
                                } else if (taskExecutionDto.getRcoaStatus().isUnsuccessful()) {
                                    taskExecutionDto.setExitMessage(processFailedMessage(rcoaJobExecution));
                                } else if (taskExecutionDto.getRcoaStatus() == BatchStatus.COMPLETED) {
                                    taskExecutionDto.getSummary().put(RUN_RCOA_JOB_NAME, showSummary(rcoaJobExecution, null));
                                }

                                taskExecutionDto.setStatus(convertStatus(taskExecutionDto.getRcoaStatus(), "RCOA"));
                            }

                            List<JobInstance> mqReportJobs = jobExplorer.findJobInstancesByJobName(
                                    RUN_MQ_REPORT_JOB_NAME.concat("*-").concat(jobInstance.getId().toString()), 0, 1);
                            String mqReportStatusAfterFinalized = null;
                            if (!mqReportJobs.isEmpty()) {
                                JobExecution mqReportJobExecution = getJobExecutions(mqReportJobs.get(0)).iterator().next();
                                Map mqReportJobParameters = Maps.transformValues(mqReportJobExecution.getJobParameters().getParameters(), JobParameter::getValue);
                                taskExecutionDto.setMqReportStatus(mqReportJobExecution.getStatus());
                                mqReportStatusAfterFinalized = mqReportJobParameters.getOrDefault(MQ_REPORT_STAT_AFTER_FINALIZE, "").toString();

                                if (taskExecutionDto.getMqReportStatus().isRunning()) {
                                    calculateProgress(mqReportJobExecution, taskExecutionDto);
                                } else if (taskExecutionDto.getMqReportStatus().isUnsuccessful()) {
                                    taskExecutionDto.setExitMessage(processFailedMessage(mqReportJobExecution));
                                } else if (taskExecutionDto.getMqReportStatus() == BatchStatus.COMPLETED) {
                                    taskExecutionDto.getSummary().put(RUN_MQ_REPORT_JOB_NAME, showSummary(mqReportJobExecution, null));
                                }
                            }

                            List<JobInstance> settlementNotReadyJobs = jobExplorer.findJobInstancesByJobName(
                                    RUN_STL_NOT_READY_JOB_NAME.concat("*-").concat(jobInstance.getId().toString()), 0, 1);

                            if (!settlementNotReadyJobs.isEmpty()) {
                                JobExecution settlementJobExecution = getJobExecutions(settlementNotReadyJobs.get(0)).iterator().next();

                                jobParameters = Maps.transformValues(settlementJobExecution.getJobParameters().getParameters(), JobParameter::getValue);
                                String stlNotReadyUser = jobParameters.getOrDefault(STL_NOT_READY_USERNAME, "").toString();

                                taskExecutionDto.setSettlementStatus(settlementJobExecution.getStatus());
                                taskExecutionDto.setStlNotReadyUser(stlNotReadyUser);

                                if (taskExecutionDto.getSettlementStatus().isRunning()) {
                                    calculateProgress(settlementJobExecution, taskExecutionDto);
                                } else if (taskExecutionDto.getSettlementStatus().isUnsuccessful()) {
                                    taskExecutionDto.setExitMessage(processFailedMessage(settlementJobExecution));
                                } else if (taskExecutionDto.getSettlementStatus() == BatchStatus.COMPLETED) {
                                    taskExecutionDto.getSummary().put(RUN_STL_NOT_READY_JOB_NAME, showSummary(settlementJobExecution, null));
                                }
                                taskExecutionDto.setStatus(convertStatus(taskExecutionDto.getSettlementStatus(), "GESQ"));
                            }

                            List<JobInstance> settlementJobs = jobExplorer.findJobInstancesByJobName(
                                    RUN_STL_READY_JOB_NAME.concat("*-").concat(jobInstance.getId().toString()), 0, 1);

                            if (!settlementJobs.isEmpty()) {
                                JobExecution settlementJobExecution = getJobExecutions(settlementJobs.get(0)).iterator().next();

                                jobParameters = Maps.transformValues(settlementJobExecution.getJobParameters().getParameters(), JobParameter::getValue);
                                String stlReadyUser = jobParameters.getOrDefault(STL_READY_USERNAME, "").toString();

                                taskExecutionDto.setSettlementReadyStatus(settlementJobExecution.getStatus());
                                taskExecutionDto.setStlReadyUser(stlReadyUser);

                                if (taskExecutionDto.getSettlementReadyStatus().isRunning()) {
                                    calculateProgress(settlementJobExecution, taskExecutionDto);
                                } else if (taskExecutionDto.getSettlementReadyStatus().isUnsuccessful()) {
                                    taskExecutionDto.setExitMessage(processFailedMessage(settlementJobExecution));
                                } else if (taskExecutionDto.getSettlementReadyStatus() == BatchStatus.COMPLETED) {
                                    taskExecutionDto.getSummary().put(RUN_STL_READY_JOB_NAME, showSummary(settlementJobExecution, null));
                                }
                                taskExecutionDto.setStatus(convertStatus(taskExecutionDto.getSettlementReadyStatus(), "Settlement Ready"));
                            }

                            if (taskExecutionDto.getSettlementReadyStatus() == BatchStatus.COMPLETED
                                    && StringUtils.isNotEmpty(mqReportStatusAfterFinalized)
                                    && mqReportStatusAfterFinalized.equals(BatchStatus.COMPLETED.name())) {
                                taskExecutionDto.setMqReportStatusAfterFinalized(BatchStatus.COMPLETED);
                            }

                            return taskExecutionDto;
                        } else {
                            return null;
                        }
                    })
                    .filter(Objects::nonNull)
                    .collect(toList());
        }
        return taskExecutionDtos;
    }

    private Date setBillingPeriodStartDate(Date startDate) {
        LocalDateTime localDateTime = new LocalDateTime(startDate);
        // start >= 26 && start <= 31 = same month
        if (localDateTime.getDayOfMonth() > 26 && localDateTime.getDayOfMonth() <= 31) {
            localDateTime.withDayOfMonth(26);
        } else if (localDateTime.getDayOfMonth() < 26) {
            // start < 26 = month - 1
            localDateTime.withDayOfMonth(26).withMonthOfYear(localDateTime.getMonthOfYear() - 1);
        }
        return localDateTime.withTime(0,0,0,0).toDate();
    }

    private Date setBillingPeriodEndDate(Date endDate) {
        LocalDateTime localDateTime = new LocalDateTime(endDate);
        // end >= 25 && end <= 31 = month + 1
        if (localDateTime.getDayOfMonth() > 25 && localDateTime.getDayOfMonth() <= 31) {
            localDateTime.withDayOfMonth(25).withMonthOfYear(localDateTime.getMonthOfYear() + 1);
        } else if (localDateTime.getDayOfMonth() < 25) {
            // end <= 25 = same month
            localDateTime.withDayOfMonth(25);
        }
        return localDateTime.withTime(0,0,0,0).toDate();
    }
}
