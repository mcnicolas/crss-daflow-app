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
import com.pemc.crss.shared.commons.util.DateUtil;
import com.pemc.crss.shared.commons.util.TaskUtil;
import com.pemc.crss.shared.core.dataflow.dto.DistinctWesmBillingPeriod;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobAddtlParams;
import com.pemc.crss.shared.core.dataflow.repository.BatchJobAddtlParamsRepository;
import com.pemc.crss.shared.core.dataflow.repository.MapBillingPeriodAdjNoRepository;
import com.pemc.crss.shared.core.dataflow.repository.SettlementJobLockRepository;
import com.pemc.crss.shared.core.dataflow.service.BatchJobAddtlParamsService;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
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
    private static final String RUN_GESQ_REPORT_JOB_NAME = "genGesqReport";
    private static final String PARAMS_BILLING_PERIOD_ID = "billingPeriodId";
    private static final String PARAMS_BILLING_PERIOD = "billingPeriod";
    private static final String PARAMS_SUPPLY_MONTH = "supplyMonth";
    private static final String PARAMS_BILLING_PERIOD_NAME = "billingPeriodName";
    private static final String MQ_REPORT_STAT_AFTER_FINALIZE = "mqReportStatusAfterFinalized";
    private static final String COPY_STL_READY_JOB_NAME = "copyStlReady";

    @Autowired
    private BatchJobAddtlParamsRepository batchJobAddtlParamsRepository;

    @Autowired
    private BatchJobAddtlParamsService batchJobAddtlParamsService;

    @Autowired
    private SettlementJobLockRepository settlementJobLockRepository;

    @Autowired
    private MapBillingPeriodAdjNoRepository mapBillingPeriodAdjNoRepository;

    @Override
    public Page<TaskExecutionDto> findJobInstances(Pageable pageable) {
        int count = 0;

        try {
            count = jobExplorer.getJobInstanceCount(RUN_WESM_JOB_NAME.concat("Daily"));
            count += jobExplorer.getJobInstanceCount(RUN_WESM_JOB_NAME.concat("Monthly"));
        } catch (NoSuchJobException e) {
            LOG.error("Exception: " + e);
        }

        return new PageImpl<>(getTaskExecutionDtos(count, pageable), pageable, count);
    }

    @Override
    public Page<GroupTaskExecutionDto> findDistinctBillingPeriodAndProcessType(Pageable pageable) {
        int count = executionParamRepository.countDistinctBillingPeriodAndProcessType(RUN_WESM_JOB_NAME);
        List<GroupTaskExecutionDto> groupTaskExecutionDtos = Lists.newArrayList();
        DateTimeFormatter dtf = DateTimeFormat.forPattern("yyMMdd");
        List<DistinctWesmBillingPeriod> distinctBillingPeriodAndProcessType = executionParamRepository.getDistinctBillingPeriodAndProcessType(pageable.getOffset(), pageable.getPageSize(), RUN_WESM_JOB_NAME);
        for (DistinctWesmBillingPeriod o : distinctBillingPeriodAndProcessType) {
            GroupTaskExecutionDto groupTaskExecutionDto = new GroupTaskExecutionDto();
            groupTaskExecutionDto.setProcessType(o.getProcessType());
            groupTaskExecutionDto.setBillingPeriod(o.getBillingPeriod());
            if (o.getBillingPeriod().length() > 6) {
                // monthly
                DateTime startDate = dtf.parseDateTime(o.getBillingPeriod().substring(0, 6));
                DateTime endDate = dtf.parseDateTime(o.getBillingPeriod().substring(6));
                groupTaskExecutionDto.setStartDate(startDate.toDate());
                groupTaskExecutionDto.setEndDate(endDate.toDate());
                groupTaskExecutionDto.setAdjNo(o.getAdjNo());
            } else {
                // daily
                DateTime date = dtf.parseDateTime(o.getBillingPeriod());
                groupTaskExecutionDto.setDate(date.toDate());
            }
            groupTaskExecutionDtos.add(groupTaskExecutionDto);
        }
        return new PageImpl<>(groupTaskExecutionDtos, pageable, count);
    }

    @Override
    public Page<TaskExecutionDto> findJobInstancesByBillingPeriodAndProcessType(Pageable pageable, String billingPeriod, String processType, Long adjNo) {
        int count = executionParamRepository.countJobInstanceByBillingPeriodAndProcessType(RUN_WESM_JOB_NAME, billingPeriod, processType);
        return new PageImpl<>(getTaskExecutionDtosUnderBillingPeriod(count, pageable, billingPeriod, processType, adjNo));
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
        Preconditions.checkNotNull(taskRunDto.getRunId());
        Preconditions.checkNotNull(taskRunDto.getJobName());
        Preconditions.checkState(batchJobRunLockRepository.countByJobNameAndLockedDateAndLockedIsTrue(taskRunDto.getJobName(), new Date()) == 0,
                "There is an existing ".concat(taskRunDto.getJobName()).concat(" job running"));

        String jobName = null;
        List<String> properties = Lists.newArrayList();
        List<String> arguments = Lists.newArrayList();

        final Long runId = taskRunDto.getRunId();
        if (RUN_WESM_JOB_NAME.equals(taskRunDto.getJobName())) {

            String existingFinalRunAggregatedMtnWithinRange;
            List<String> mtnAlreadyFinalized = new ArrayList<>();
            String currentRunningMtns = taskRunDto.getMtns();

            if (taskRunDto.getMeterProcessType() == null || PROCESS_TYPE_DAILY.equals(taskRunDto.getMeterProcessType())) {
                checkFinalizedStlState(taskRunDto.getTradingDate(), null, PROCESS_TYPE_DAILY);
                // prevent running if selected mtn is already run within date range or the like
                existingFinalRunAggregatedMtnWithinRange = getAggregatedSelectedMtnFinalStlReadyRunWithinRange(PROCESS_TYPE_DAILY, taskRunDto.getTradingDate(), null, null);

                //Region group

                if (StringUtils.isNotEmpty(currentRunningMtns)) {
                    checkSelectedMtnsFinalizeStlReady(existingFinalRunAggregatedMtnWithinRange, currentRunningMtns, mtnAlreadyFinalized);
                    checkFinalizeDailyStateAllMtnsRegionGroup(taskRunDto.getRegionGroup(), taskRunDto.getTradingDate());
                } else if (isAllRegions(taskRunDto.getRegionGroup())) {
                    checkFinalizeDailyStateAnyRegion(taskRunDto.getTradingDate());
                } else {
                    checkFinalizeDailyStateRegionGroup(taskRunDto.getRegionGroup(), taskRunDto.getTradingDate());
                    checkFinalizeDailyStateAllRegions(taskRunDto.getTradingDate());
                }

                arguments.add(concatKeyValue(DATE, taskRunDto.getTradingDate(), PARAMS_TYPE_DATE));
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(PROFILE_DAILY_MQ)));
            } else {
                String processType = taskRunDto.getMeterProcessType();
                if (!processType.equalsIgnoreCase(MeterProcessType.PRELIM.name())) {
                   /* String processBefore = processType.equalsIgnoreCase(MeterProcessType.FINAL.name()) ?
                            MeterProcessType.PRELIM.name() : MeterProcessType.FINAL.name();*/
                    if (!processType.equalsIgnoreCase(MeterProcessType.FINAL.name())) {
                        checkProcessTypeState(MeterProcessType.FINAL.name(), taskRunDto.getStartDate(), taskRunDto.getEndDate(), RUN_WESM_JOB_NAME);
                    }
                }

                // prevent running if selected mtn is already run within date range or the like
                existingFinalRunAggregatedMtnWithinRange = getAggregatedSelectedMtnFinalStlReadyRunWithinRange(processType, null, taskRunDto.getStartDate(), taskRunDto.getEndDate());

                //Region group

                if (!MeterProcessType.ADJUSTED.name().equals(processType)) {
                    if (StringUtils.isEmpty(currentRunningMtns)) {
                        checkSelectedMtnsFinalizeStlReady(existingFinalRunAggregatedMtnWithinRange, currentRunningMtns, mtnAlreadyFinalized);
                        checkFinalizeMonthlyStateAllMtnsRegionGroup(taskRunDto.getRegionGroup(), processType, taskRunDto.getStartDate(), taskRunDto.getEndDate());
                    } else if (isAllRegions(taskRunDto.getRegionGroup())) {
                        checkFinalizedStlState(taskRunDto.getStartDate(), taskRunDto.getEndDate(), processType);
                    } else {
                        checkFinalizeMonthlyStateRegionGroup(taskRunDto.getRegionGroup(), processType, taskRunDto.getStartDate(), taskRunDto.getEndDate());
                        checkFinalizeMonthlyStateAllRegions(processType, taskRunDto.getStartDate(), taskRunDto.getEndDate());
                    }
                } else {
                    BatchJobAddtlParams paramsAdjNo = new BatchJobAddtlParams();
                    paramsAdjNo.setRunId(runId);
                    paramsAdjNo.setType(PARAMS_TYPE_LONG);
                    paramsAdjNo.setKey(ADJUSTMENT_NO);
                    paramsAdjNo.setLongVal(taskRunDto.getAdjNo());
                    saveBatchJobAddtlParamsJdbc(paramsAdjNo);
                }

                arguments.add(concatKeyValue(START_DATE, taskRunDto.getStartDate(), PARAMS_TYPE_DATE));
                arguments.add(concatKeyValue(END_DATE, taskRunDto.getEndDate(), PARAMS_TYPE_DATE));
                arguments.add(concatKeyValue(PROCESS_TYPE, processType));

                BatchJobAddtlParams paramsBillingPeriodId = new BatchJobAddtlParams();
                paramsBillingPeriodId.setRunId(runId);
                paramsBillingPeriodId.setType(PARAMS_TYPE_LONG);
                paramsBillingPeriodId.setKey(PARAMS_BILLING_PERIOD_ID);
                paramsBillingPeriodId.setLongVal(taskRunDto.getBillingPeriodId());
                saveBatchJobAddtlParamsJdbc(paramsBillingPeriodId);

                BatchJobAddtlParams paramsBillingPeriod = new BatchJobAddtlParams();
                paramsBillingPeriod.setRunId(runId);
                paramsBillingPeriod.setType(PARAMS_TYPE_LONG);
                paramsBillingPeriod.setKey(PARAMS_BILLING_PERIOD);
                paramsBillingPeriod.setLongVal(taskRunDto.getBillingPeriod());
                saveBatchJobAddtlParamsJdbc(paramsBillingPeriod);

                BatchJobAddtlParams paramsSupplyMonth = new BatchJobAddtlParams();
                paramsSupplyMonth.setRunId(runId);
                paramsSupplyMonth.setType(PARAMS_TYPE_STRING);
                paramsSupplyMonth.setKey(PARAMS_SUPPLY_MONTH);
                paramsSupplyMonth.setStringVal(taskRunDto.getSupplyMonth());
                saveBatchJobAddtlParamsJdbc(paramsSupplyMonth);

                BatchJobAddtlParams paramsBillingPeriodName = new BatchJobAddtlParams();
                paramsBillingPeriodName.setRunId(runId);
                paramsBillingPeriodName.setType(PARAMS_TYPE_STRING);
                paramsBillingPeriodName.setKey(PARAMS_BILLING_PERIOD_NAME);
                paramsBillingPeriodName.setStringVal(taskRunDto.getBillingPeriodName());
                saveBatchJobAddtlParamsJdbc(paramsBillingPeriodName);

                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(PROFILE_MONTHLY_MQ)));
            }

            LOG.debug("Selected MTNs = {}", taskRunDto.getMtns());
            // temporarily save in additional params to lessen task id length in chronos
            if (StringUtils.isNotEmpty(taskRunDto.getMtns())) {
                LOG.debug("Adding Selected MTNs = {}", taskRunDto.getMtns());
                BatchJobAddtlParams paramsSelectedMtns = new BatchJobAddtlParams();
                paramsSelectedMtns.setRunId(runId);
                paramsSelectedMtns.setType(PARAMS_TYPE_STRING);
                paramsSelectedMtns.setKey(MTNS);
                paramsSelectedMtns.setStringVal(taskRunDto.getMtns());
                saveBatchJobAddtlParamsJdbc(paramsSelectedMtns);
            }

            arguments.add(concatKeyValue(METER_TYPE, METER_TYPE_WESM));
            arguments.add(concatKeyValue(WESM_USERNAME, taskRunDto.getCurrentUser()));
            // for list by billing period
            arguments.add(concatKeyValue(TaskUtil.BP, taskRunDto.getFormattedBillingPeriod()));

            arguments.add(concatKeyValue(RG, taskRunDto.getRegionGroup()));
            jobName = "crss-meterprocess-task-mqcomputation";
        } else if (taskRunDto.getParentJob() != null) {
            JobInstance jobInstance = jobExplorer.getJobInstance(Long.valueOf(taskRunDto.getParentJob()));
            JobParameters jobParameters = getJobExecutions(jobInstance).get(0).getJobParameters();
            String processType = jobParameters.getString(PROCESS_TYPE);
            String regionGroup = jobParameters.getString(RG);
            boolean isDaily = processType == null || Objects.equals(processType, MeterProcessType.DAILY.name());
            Long parentJobRunId = jobParameters.getLong(RUN_ID);

            String existingFinalRunAggregatedMtnWithinRange = EMPTY;
            String currentRunningMtns = batchJobAddtlParamsService.getBatchJobAddtlParamsStringVal(parentJobRunId, MTNS);
            List<String> mtnAlreadyFinalized = new ArrayList<>();

            if (isDaily) {
                if (!RUN_MQ_REPORT_JOB_NAME.equals(taskRunDto.getJobName())) {
                    if (StringUtils.isNotEmpty(currentRunningMtns)) {
                        checkSelectedMtnsFinalizeStlReady(existingFinalRunAggregatedMtnWithinRange, currentRunningMtns, mtnAlreadyFinalized);
                        checkFinalizeDailyStateAllMtnsRegionGroup(jobParameters.getString(RG), dateFormat.format(jobParameters.getDate(DATE)));
                    } else if (isAllRegions(regionGroup)) {
                        //checkFinalizeDailyState(dateFormat.format(jobParameters.getDate(DATE)));
                        checkFinalizeDailyStateAnyRegion(dateFormat.format(jobParameters.getDate(DATE)));
                    } else {
                        checkFinalizeDailyStateRegionGroup(jobParameters.getString(RG), dateFormat.format(jobParameters.getDate(DATE)));
                        checkFinalizeDailyStateAllRegions(dateFormat.format(jobParameters.getDate(DATE)));
                    }
                }
                arguments.add(concatKeyValue(DATE, dateFormat.format(jobParameters.getDate(DATE)), PARAMS_TYPE_DATE));
            } else {
                if (!MeterProcessType.ADJUSTED.name().equals(processType)
                        && !RUN_MQ_REPORT_JOB_NAME.equals(taskRunDto.getJobName())) {
                    if (StringUtils.isNotEmpty(currentRunningMtns)) {
                        checkSelectedMtnsFinalizeStlReady(existingFinalRunAggregatedMtnWithinRange, currentRunningMtns, mtnAlreadyFinalized);
                        checkFinalizeMonthlyStateAllMtnsRegionGroup(regionGroup, processType, dateFormat.format(jobParameters.getDate(START_DATE)), dateFormat.format(jobParameters.getDate(END_DATE)));
                    } else if (isAllRegions(regionGroup)) {
                        //checkFinalizeDailyState(dateFormat.format(jobParameters.getDate(DATE)));
                        checkFinalizeMonthlyStateAnyRegion(taskRunDto.getMeterProcessType(), dateFormat.format(jobParameters.getDate(START_DATE)), dateFormat.format(jobParameters.getDate(END_DATE)));
                    } else {
                        checkFinalizeMonthlyStateRegionGroup(regionGroup, processType, dateFormat.format(jobParameters.getDate(START_DATE)), dateFormat.format(jobParameters.getDate(END_DATE)));
                        checkFinalizeMonthlyStateAllRegions(processType, dateFormat.format(jobParameters.getDate(START_DATE)), dateFormat.format(jobParameters.getDate(END_DATE)));
                    }
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
//                        String processBefore = processType.equalsIgnoreCase(MeterProcessType.FINAL.name()) ?
//                                MeterProcessType.PRELIM.name() : MeterProcessType.FINAL.name();
                        if (!processType.equalsIgnoreCase(MeterProcessType.FINAL.name())) {
                            checkProcessTypeState(MeterProcessType.FINAL.name(), taskRunDto.getStartDate(), taskRunDto.getEndDate(), RUN_WESM_JOB_NAME);
                        }
                    }
                }
                arguments.add(concatKeyValue(RCOA_USERNAME, taskRunDto.getCurrentUser()));
                jobName = "crss-meterprocess-task-mqcomputation";
            } else if (RUN_STL_NOT_READY_JOB_NAME.equals(taskRunDto.getJobName())) {
                if (isDaily) {
                    properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(PROFILE_DAILY_MQ)));
                } else if (MeterProcessType.PRELIM.name().equals(taskRunDto.getMeterProcessType())) {
                    properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(PROFILE_MONTHLY_PRELIM)));
                } else if (MeterProcessType.FINAL.name().equals(taskRunDto.getMeterProcessType())) {
//                    checkProcessTypeState(MeterProcessType.PRELIM.name(), dateFormat.format(jobParameters.getDate(START_DATE)),
//                            dateFormat.format(jobParameters.getDate(END_DATE)), RUN_STL_NOT_READY_JOB_NAME);
                    properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(PROFILE_MONTHLY_FINAL)));
                } else if (MeterProcessType.ADJUSTED.name().equals(taskRunDto.getMeterProcessType())) {
//                    checkProcessTypeState(MeterProcessType.FINAL.name(), dateFormat.format(jobParameters.getDate(START_DATE)),
//                            dateFormat.format(jobParameters.getDate(END_DATE)), RUN_STL_NOT_READY_JOB_NAME);
                    properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(PROFILE_MONTHLY_ADJUSTED)));
                }
                arguments.add(concatKeyValue(STL_NOT_READY_USERNAME, taskRunDto.getCurrentUser()));
                jobName = "crss-meterprocess-task-stlready";
            } else if (RUN_STL_READY_JOB_NAME.equals(taskRunDto.getJobName())) {
                // compare two string fields and check if the current running is already included in the existing, if true, prevent from running, else, run
                if (isDaily) {
                    // prevent running if selected mtn is already run within date range or the like
                    existingFinalRunAggregatedMtnWithinRange = getAggregatedSelectedMtnFinalStlReadyRunWithinRange(PROCESS_TYPE_DAILY, dateFormat.format(jobParameters.getDate(DATE)), null, null);
                    if (StringUtils.isNotEmpty(currentRunningMtns)) {
                        checkSelectedMtnsFinalizeStlReady(existingFinalRunAggregatedMtnWithinRange, currentRunningMtns, mtnAlreadyFinalized);
                        checkFinalizeDailyStateAllMtnsRegionGroup(regionGroup, dateFormat.format(jobParameters.getDate(DATE)));
                    } else if (isAllRegions(regionGroup)) {
                        //prevent running if any region is finalized.
                        //checkFinalizeDailyState(dateFormat.format(jobParameters.getDate(DATE)));
                        checkFinalizeDailyStateAnyRegion(dateFormat.format(jobParameters.getDate(DATE)));
                    } else {
                        checkFinalizeDailyStateRegionGroup(regionGroup, dateFormat.format(jobParameters.getDate(DATE)));
                        checkFinalizeDailyStateAllRegions(dateFormat.format(jobParameters.getDate(DATE)));
                    }

                    properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(PROFILE_STL_READY_DAILY)));
                } else if (MeterProcessType.PRELIM.name().equals(taskRunDto.getMeterProcessType())) {
                    // prevent running if selected mtn is already run within date range or the like
                    existingFinalRunAggregatedMtnWithinRange = getAggregatedSelectedMtnFinalStlReadyRunWithinRange(MeterProcessType.PRELIM.name(), null, dateFormat.format(jobParameters.getDate(START_DATE)), dateFormat.format(jobParameters.getDate(END_DATE)));

                    if (StringUtils.isNotEmpty(currentRunningMtns)) {
                        checkSelectedMtnsFinalizeStlReady(existingFinalRunAggregatedMtnWithinRange, currentRunningMtns, mtnAlreadyFinalized);
                        checkFinalizeMonthlyStateAllMtnsRegionGroup(regionGroup, taskRunDto.getMeterProcessType(), taskRunDto.getStartDate(), taskRunDto.getEndDate());
                    } else if (isAllRegions(regionGroup)) {
                        //checkFinalizeProcessTypeState(MeterProcessType.PRELIM.name(), dateFormat.format(jobParameters.getDate(START_DATE)), dateFormat.format(jobParameters.getDate(END_DATE)));
                        checkFinalizeMonthlyStateAnyRegion(taskRunDto.getMeterProcessType(), dateFormat.format(jobParameters.getDate(START_DATE)), dateFormat.format(jobParameters.getDate(END_DATE)));
                    } else {
                        checkFinalizeMonthlyStateRegionGroup(regionGroup, taskRunDto.getMeterProcessType(), taskRunDto.getStartDate(), taskRunDto.getEndDate());
                        checkFinalizeMonthlyStateAllRegions(taskRunDto.getMeterProcessType(), dateFormat.format(jobParameters.getDate(START_DATE)), dateFormat.format(jobParameters.getDate(END_DATE)));
                    }

                    properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(PROFILE_STL_READY_MONTHLY_PRELIM)));
                } else if (MeterProcessType.FINAL.name().equals(taskRunDto.getMeterProcessType())) {
                    // prevent running if selected mtn is already run within date range or the like
                    existingFinalRunAggregatedMtnWithinRange = getAggregatedSelectedMtnFinalStlReadyRunWithinRange(MeterProcessType.FINAL.name(), null, dateFormat.format(jobParameters.getDate(START_DATE)), dateFormat.format(jobParameters.getDate(END_DATE)));
                    if (StringUtils.isEmpty(currentRunningMtns)) {
                        checkSelectedMtnsFinalizeStlReady(existingFinalRunAggregatedMtnWithinRange, currentRunningMtns, mtnAlreadyFinalized);
                        checkFinalizeMonthlyStateAllMtnsRegionGroup(regionGroup, taskRunDto.getMeterProcessType(), taskRunDto.getStartDate(), taskRunDto.getEndDate());
                    } else if (isAllRegions(regionGroup)) {
                        //checkFinalizeProcessTypeState(MeterProcessType.FINAL.name(), dateFormat.format(jobParameters.getDate(START_DATE)), dateFormat.format(jobParameters.getDate(END_DATE)));
                        checkFinalizeMonthlyStateAnyRegion(taskRunDto.getMeterProcessType(), dateFormat.format(jobParameters.getDate(START_DATE)), dateFormat.format(jobParameters.getDate(END_DATE)));
                    } else {
                        checkFinalizeMonthlyStateRegionGroup(regionGroup, taskRunDto.getMeterProcessType(), taskRunDto.getStartDate(), taskRunDto.getEndDate());
                        checkFinalizeMonthlyStateAllRegions(taskRunDto.getMeterProcessType(), dateFormat.format(jobParameters.getDate(START_DATE)), dateFormat.format(jobParameters.getDate(END_DATE)));
                    }

//                    checkProcessTypeState(MeterProcessType.PRELIM.name(), dateFormat.format(jobParameters.getDate(START_DATE)),
//                            dateFormat.format(jobParameters.getDate(END_DATE)), RUN_STL_READY_JOB_NAME);
                    properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(PROFILE_STL_READY_MONTHLY_FINAL)));
                } else if (MeterProcessType.ADJUSTED.name().equals(taskRunDto.getMeterProcessType())) {
//                    checkProcessTypeState(MeterProcessType.FINAL.name(), dateFormat.format(jobParameters.getDate(START_DATE)),
//                            dateFormat.format(jobParameters.getDate(END_DATE)), RUN_STL_READY_JOB_NAME);
//                    checkFinalizedAdjustmentStateRegionGroup(jobParameters.getLong(RUN_ID), MeterProcessType.ADJUSTED.name(),
//                            dateFormat.format(jobParameters.getDate(START_DATE)), dateFormat.format(jobParameters.getDate(END_DATE)), regionGroup);
                    properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(PROFILE_STL_READY_MONTHLY_ADJUSTED)));
                }
                arguments.add(concatKeyValue(STL_READY_USERNAME, taskRunDto.getCurrentUser()));
                arguments.add(concatKeyValue(TaskUtil.BP, taskRunDto.getFormattedBillingPeriod()));
                jobName = "crss-meterprocess-task-stlready";
            } else if (RUN_MQ_REPORT_JOB_NAME.equals(taskRunDto.getJobName())) {
                if (isDaily) {
                    properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(PROFILE_DAILY_MQ_REPORT)));
                } else {
                    properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(PROFILE_MONTHLY_MQ_REPORT)));
                }
                if (taskRunDto.getTradingDate() != null
                        && taskRunDto.getTradingDate().equals(BatchStatus.COMPLETED.name())) {
                    arguments.add(concatKeyValue(MQ_REPORT_STAT_AFTER_FINALIZE, BatchStatus.COMPLETED.name()));
                }
                arguments.add(concatKeyValue(MQ_REPORT_USERNAME, taskRunDto.getCurrentUser()));
                arguments.add(concatKeyValue(TaskUtil.BP, jobParameters.getString(TaskUtil.BP)));
                jobName = "crss-meterprocess-task-mqcomputation";
            } else if (RUN_GESQ_REPORT_JOB_NAME.equals(taskRunDto.getJobName())) {
                if (isDaily) {
                    properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(PROFILE_DAILY_MQ_REPORT)));
                } else {
                    properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(PROFILE_MONTHLY_MQ_REPORT)));
                }
                if (taskRunDto.getTradingDate() != null
                        && taskRunDto.getTradingDate().equals(BatchStatus.COMPLETED.name())) {
                    arguments.add(concatKeyValue(MQ_REPORT_STAT_AFTER_FINALIZE, BatchStatus.COMPLETED.name()));
                }
                arguments.add(concatKeyValue(MQ_REPORT_USERNAME, taskRunDto.getCurrentUser()));
                arguments.add(concatKeyValue(TaskUtil.BP, jobParameters.getString(TaskUtil.BP)));
                jobName = "crss-meterprocess-task-gesqreport";
            }
        } else if (COPY_STL_READY_JOB_NAME.equals(taskRunDto.getJobName())) {

            checkFinalizedStlStateRegionGroup(taskRunDto.getStartDate(), taskRunDto.getEndDate(), taskRunDto.getRegionGroup());

            checkFinalizedStlStateRegionGroupAdjNo(taskRunDto.getFormattedBillingPeriod(), taskRunDto.getAdjNo(), taskRunDto.getRegionGroup());

            arguments.add(concatKeyValue(START_DATE, taskRunDto.getStartDate(), PARAMS_TYPE_DATE));
            arguments.add(concatKeyValue(END_DATE, taskRunDto.getEndDate(), PARAMS_TYPE_DATE));
            arguments.add(concatKeyValue(PROCESS_TYPE,  taskRunDto.getMeterProcessType()));
            arguments.add(concatKeyValue(METER_TYPE, METER_TYPE_WESM));
            arguments.add(concatKeyValue(STL_READY_USERNAME, taskRunDto.getCurrentUser()));
            arguments.add(concatKeyValue(TaskUtil.BP, taskRunDto.getFormattedBillingPeriod()));
            arguments.add(concatKeyValue(RG, taskRunDto.getRegionGroup()));

            BatchJobAddtlParams paramsBillingPeriodId = new BatchJobAddtlParams();
            paramsBillingPeriodId.setRunId(runId);
            paramsBillingPeriodId.setType(PARAMS_TYPE_LONG);
            paramsBillingPeriodId.setKey(PARAMS_BILLING_PERIOD_ID);
            paramsBillingPeriodId.setLongVal(taskRunDto.getBillingPeriodId());
            saveBatchJobAddtlParamsJdbc(paramsBillingPeriodId);

            BatchJobAddtlParams paramsBillingPeriod = new BatchJobAddtlParams();
            paramsBillingPeriod.setRunId(runId);
            paramsBillingPeriod.setType(PARAMS_TYPE_LONG);
            paramsBillingPeriod.setKey(PARAMS_BILLING_PERIOD);
            paramsBillingPeriod.setLongVal(taskRunDto.getBillingPeriod());
            saveBatchJobAddtlParamsJdbc(paramsBillingPeriod);

            BatchJobAddtlParams paramsSupplyMonth = new BatchJobAddtlParams();
            paramsSupplyMonth.setRunId(runId);
            paramsSupplyMonth.setType(PARAMS_TYPE_STRING);
            paramsSupplyMonth.setKey(PARAMS_SUPPLY_MONTH);
            paramsSupplyMonth.setStringVal(taskRunDto.getSupplyMonth());
            saveBatchJobAddtlParamsJdbc(paramsSupplyMonth);

            BatchJobAddtlParams paramsBillingPeriodName = new BatchJobAddtlParams();
            paramsBillingPeriodName.setRunId(runId);
            paramsBillingPeriodName.setType(PARAMS_TYPE_STRING);
            paramsBillingPeriodName.setKey(PARAMS_BILLING_PERIOD_NAME);
            paramsBillingPeriodName.setStringVal(taskRunDto.getBillingPeriodName());
            saveBatchJobAddtlParamsJdbc(paramsBillingPeriodName);

            BatchJobAddtlParams paramsAdjNo = new BatchJobAddtlParams();
            paramsAdjNo.setRunId(runId);
            paramsAdjNo.setType(PARAMS_TYPE_LONG);
            paramsAdjNo.setKey(ADJUSTMENT_NO);
            paramsAdjNo.setLongVal(taskRunDto.getAdjNo());
            saveBatchJobAddtlParamsJdbc(paramsAdjNo);

            properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(PROFILE_COPY_STL_READY_MONTHLY)));

            jobName = "crss-meterprocess-task-stlready";
        }

        LOG.debug("Running job name={}, properties={}, arguments={}", taskRunDto.getJobName(), properties, arguments);

        if (jobName != null) {
            arguments.add(concatKeyValue(RUN_ID, String.valueOf(runId), PARAMS_TYPE_LONG));

            LOG.debug("Running job name={}, properties={}, arguments={}", taskRunDto.getJobName(), properties, arguments);
            launchJob(jobName, properties, arguments);

            //Removed job locking since there's already job queueing.
            //lockJobJdbc(taskRunDto);
        }
    }

    @Override
    public void relaunchFailedJob(long jobId) throws URISyntaxException {

    }

    //modify countMonthlyRun to use range instead of specific date
    private void checkProcessTypeState(String process, String startDate, String endDate, String jobName) {
        String errMsq = "Must run " + process + " first!";
        Preconditions.checkState(executionParamRepository.countMonthlyRun(startDate, endDate, process, jobName) > 0, errMsq);
    }

    private void checkFinalizeDailyState(String date) {
        String errMsq = "You already have a process finalized on the same date: " + date + " !";
        Preconditions.checkState(executionParamRepository.countDailyRunAllMtn(date, RUN_STL_READY_JOB_NAME) < 1, errMsq);
    }

    private void checkFinalizeDailyStateAnyRegion(String date) {
        String errMsq = "You already have another process finalized on any or all regions on the same date: " + date + " !";
        Preconditions.checkState(executionParamRepository.countDailyRunAllMtn(date, RUN_STL_READY_JOB_NAME) < 1 &&
                executionParamRepository.countDailyRunAllMtnAnyRegion(date, RUN_WESM_JOB_NAME) < 1, errMsq);
    }

    private void checkFinalizeDailyStateAllRegions(String date) {
        String errMsq = "You already have another process finalized on all regions on the same date: " + date + " !";
        Preconditions.checkState(executionParamRepository.countDailyRunAllMtnAllRegion(date, RUN_STL_READY_JOB_NAME) < 1, errMsq);
    }

    private void checkFinalizeDailyStateRegionGroup(String regionGroup, String date) {
        String errMsq = "You already have a settlement-ready process finalized on the same date (" + date + ") and region group (" + regionGroup + ")!";
        Preconditions.checkState(executionParamRepository.countDailyRunRegionGroup(date, regionGroup, RUN_STL_READY_JOB_NAME) < 1, errMsq);
    }

    private void checkFinalizeDailyStateAllMtnsRegionGroup(String regionGroup, String date) {
        String errMsq = "You already have a settlement-ready process finalized on the same date (" + date + ") and region group (" + regionGroup + ") on all MTNs!";
        Preconditions.checkState(executionParamRepository.countDailyRunAllMtnRegionGroup(date, RUN_STL_READY_JOB_NAME, regionGroup) < 1, errMsq);
    }

    private void checkFinalizeMonthlyStateRegionGroup(String regionGroup, String process, String startDate, String endDate) {
        String errMsq = "You already have a settlement-ready process finalized on the same billing period (" + startDate + " - " + endDate + ") and region group (" + regionGroup + ") of processType: " + process + "!";
        Preconditions.checkState(executionParamRepository.countMonthlyRunRegionGroup(startDate, endDate, regionGroup, process, RUN_STL_READY_JOB_NAME) < 1, errMsq);
    }

    private void checkFinalizeMonthlyStateAllMtnsRegionGroup(String regionGroup, String process, String startDate, String endDate) {
        String errMsq = "You already have a settlement-ready process finalized on the same billing period (" + startDate + " - " + endDate + ") and region group (" + regionGroup + ") of processType: " + process + " on all MTNs!";
        Preconditions.checkState(executionParamRepository.countMonthlyRunAllMtnRegionGroup(startDate, endDate, process, RUN_STL_READY_JOB_NAME, regionGroup) < 1, errMsq);
    }

    private void checkFinalizeProcessTypeState(String process, String startDate, String endDate) {
        String errMsq = "You already have a " + process + " finalized on the same billing period!";
        Preconditions.checkState(executionParamRepository.countMonthlyRunAllMtn(startDate, endDate, process, RUN_STL_READY_JOB_NAME) < 1, errMsq);
    }

    private void checkFinalizeMonthlyStateAnyRegion(String process, String startDate, String endDate) {
        String errMsq = "You already have a " + process + " on any or all regions finalized on the same billing period!";
        Preconditions.checkState(executionParamRepository.countMonthlyRunAllMtn(startDate, endDate, process, RUN_STL_READY_JOB_NAME) < 1 &&
                executionParamRepository.countMonthlyRunAllMtnAnyRegion(startDate, endDate, process, RUN_WESM_JOB_NAME) < 1, errMsq);
    }

    private void checkFinalizeMonthlyStateAllRegions(String process, String startDate, String endDate) {
        String errMsq = "You already have a " + process + " on all regions finalized on the same billing period!";
        Preconditions.checkState(executionParamRepository.countMonthlyRunAllMtnAllRegion(startDate, endDate, process, RUN_STL_READY_JOB_NAME) < 1, errMsq);
    }

    private void checkFinalizedAdjustmentState(Long parentRunId, String process, String startDate, String endDate) {
        String errMsq = "A finalized run with a later date already exist!";
        Preconditions.checkState(executionParamRepository.findLatestWesmRunIdMonthly(startDate, endDate, process, RUN_STL_READY_JOB_NAME) < parentRunId, errMsq);
    }

    private void checkFinalizedAdjustmentStateRegionGroup(Long parentRunId, String process, String startDate, String endDate, String regionGroup) {
        String errMsq = "A finalized run with a later date already exist!";
        Preconditions.checkState(executionParamRepository.findLatestWesmRunIdMonthlyRegionGroup(startDate, endDate, process, RUN_STL_READY_JOB_NAME, regionGroup) < parentRunId, errMsq);
    }

    private List<TaskExecutionDto> getTaskExecutionDtos(int count, Pageable pageable) {
        List<TaskExecutionDto> taskExecutionDtos = Lists.newArrayList();

        if (count > 0) {
            taskExecutionDtos = jobExplorer.findJobInstancesByJobName(RUN_WESM_JOB_NAME.concat("*"),
                    pageable.getOffset(), pageable.getPageSize()).stream()
                    .map(this::getTaskExecutionDto)
                    .filter(Objects::nonNull)
                    .collect(toList());
        }
        return taskExecutionDtos;
    }

    private List<TaskExecutionDto> getTaskExecutionDtosUnderBillingPeriod(int count, Pageable pageable, String billingPeriod, String processType, Long adjNo) {
        List<TaskExecutionDto> taskExecutionDtos = executionParamRepository.getJobInstanceByBillingPeriodAndProcessType(pageable.getOffset(), pageable.getPageSize(), RUN_WESM_JOB_NAME, billingPeriod, processType, adjNo).stream()
                .map(this::getTaskExecutionDto)
                .filter(Objects::nonNull)
                .collect(toList());
        return taskExecutionDtos;
    }

    private void checkSelectedMtnsFinalizeStlReady(String existingFinalRunAggregatedMtn, String currentRunningMtns, List<String> mtnAlreadyFinalized) {
        String errorMessage = "Cannot run Finalize Settlement Ready if MTNs are already finalized. %s";
        if (StringUtils.isNotEmpty(existingFinalRunAggregatedMtn) && StringUtils.isNotEmpty(currentRunningMtns)) {
            for (String existingMtn : existingFinalRunAggregatedMtn.split(",")) {
                for (String currentMtn : currentRunningMtns.split(",")) {
                    if (existingMtn.equalsIgnoreCase(currentMtn)) {
                        mtnAlreadyFinalized.add(currentMtn);
                    }
                }
            }
        }
        Preconditions.checkState(mtnAlreadyFinalized.size() == 0, String.format(errorMessage, mtnAlreadyFinalized.stream().collect(Collectors.joining(","))));
    }

    private String getAggregatedSelectedMtnFinalStlReadyRunWithinRange(String processType, String date, String startDate, String endDate) {
        if (StringUtils.isNotEmpty(processType)) {
            if (PROCESS_TYPE_DAILY.equals(processType)) {
                return executionParamRepository.getAggregatedSelectedMtnsDailyWithinRange(date);
            } else {
                return executionParamRepository.getAggregatedSelectedMtnsMonthlyWithinRange(startDate, endDate, processType);
            }
        } else {
            return EMPTY;
        }
    }

    private void checkFinalizedStlState(String startDate, String endDate, String processType) {

        if (!MeterProcessType.ADJUSTED.name().equalsIgnoreCase(processType)) {
            String errorMessage = "You already have a settlement process finalized on the same billing date ( %s ) with meter process type: %s";
            Preconditions.checkState(!settlementJobLockRepository.tdAmtOrEMFBillingPeriodIsFinalized(startDate, endDate, processType), String.format(errorMessage, startDate + (endDate != null ? " / " + endDate : ""), processType));
        } else {
            String errorMessage = "Cannot run WESM on billing date ( %s ) with ADJUSTED meter process type. Must have a Settlement Process of FINAL meter type finalized on the same billing period";
            Preconditions.checkState(settlementJobLockRepository.tdAmtOrEMFBillingPeriodIsFinalized(startDate, endDate, MeterProcessType.FINAL.name()), String.format(errorMessage, startDate + (endDate != null ? " / " + endDate : "")));
        }
    }


    private void checkFinalizedStlStateRegionGroup(String startDate, String endDate, String regionGroup) {
        String errorMessage = "Cannot Copy GESQ on billing date ( %s ) with ADJUSTED meter process type. Must have a Settlement Process of FINAL meter type finalized on the same billing period";
        Preconditions.checkState(settlementJobLockRepository.tdAmtOrEMFBillingPeriodIsFinalized(startDate, endDate, MeterProcessType.FINAL.name(), regionGroup), String.format(errorMessage, startDate + (endDate != null ? " / " + endDate : "")));
    }

    private void checkFinalizedStlStateRegionGroupAdjNo(String billingPeriod, Long adjNo, String regionGroup) {
        String groupId = billingPeriod + adjNo;
        String errorMessage = "Cannot Copy GESQ on Formatted Billing Period ( %s ) with Adjustment Number ( %s ) and Region Group ( %s ). An Adjustment Settlement Process with the same parameters is already finalized!";
        Preconditions.checkState(!settlementJobLockRepository.tdAmtOrEMFBillingPeriodIsFinalizedAdj(groupId, MeterProcessType.ADJUSTED.name(), regionGroup), String.format(errorMessage, billingPeriod, adjNo + "", regionGroup));
    }

    private TaskExecutionDto getTaskExecutionDto(JobInstance jobInstance) {
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

            JobParameters jParams = jobExecution.getJobParameters();
            String processType = jParams.getString(PROCESS_TYPE);
            boolean isDaily = processType == null || Objects.equals(MeterProcessType.DAILY.name(), processType);
            String sDate;
            String eDate = null;
            if (isDaily) {
                processType = PROCESS_TYPE_DAILY;
                sDate = DateUtil.convertToString(jParams.getDate(DATE), DateUtil.DEFAULT_DATETIME_FORMAT);
            } else {
                sDate = DateUtil.convertToString(jParams.getDate(START_DATE), DateUtil.DEFAULT_DATETIME_FORMAT);
                eDate = DateUtil.convertToString(jParams.getDate(END_DATE), DateUtil.DEFAULT_DATETIME_FORMAT);
            }

            LOG.info("sDate: " + sDate + " | eDate: " + eDate);
            taskExecutionDto.setStlProcessFinalizedStatus(settlementJobLockRepository.tdAmtOrEMFBillingPeriodIsFinalized(sDate, eDate, processType) ? BatchStatus.COMPLETED : null);

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
    }

    private boolean isAllRegions(String regionGroupParam) {
        return StringUtils.isBlank(regionGroupParam) || ALL.equalsIgnoreCase(regionGroupParam);
    }
}
