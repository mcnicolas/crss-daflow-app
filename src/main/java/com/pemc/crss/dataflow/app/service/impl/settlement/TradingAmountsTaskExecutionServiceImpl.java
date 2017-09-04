package com.pemc.crss.dataflow.app.service.impl.settlement;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.pemc.crss.dataflow.app.dto.JobCalculationDto;
import com.pemc.crss.dataflow.app.dto.SettlementTaskExecutionDto;
import com.pemc.crss.dataflow.app.dto.StlJobGroupDto;
import com.pemc.crss.dataflow.app.dto.TaskRunDto;
import com.pemc.crss.dataflow.app.dto.parent.GroupTaskExecutionDto;
import com.pemc.crss.dataflow.app.dto.parent.StubTaskExecutionDto;
import com.pemc.crss.dataflow.app.service.StlReadyJobQueryService;
import com.pemc.crss.dataflow.app.support.PageableRequest;
import com.pemc.crss.dataflow.app.support.StlJobStage;
import com.pemc.crss.shared.commons.reference.MeterProcessType;
import com.pemc.crss.shared.commons.reference.SettlementStepUtil;
import com.pemc.crss.shared.core.dataflow.dto.DistinctStlReadyJob;
import com.pemc.crss.shared.core.dataflow.entity.ViewSettlementJob;
import com.pemc.crss.shared.core.dataflow.reference.SettlementJobProfile;
import com.pemc.crss.shared.core.dataflow.reference.StlCalculationType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameters;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import javax.transaction.Transactional;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.pemc.crss.shared.commons.reference.MeterProcessType.*;
import static com.pemc.crss.shared.commons.reference.SettlementStepUtil.CALC_SCALING_FACTOR;
import static com.pemc.crss.shared.commons.reference.SettlementStepUtil.DISAGGREGATE_BCQ;
import static com.pemc.crss.shared.commons.reference.SettlementStepUtil.RETRIEVE_BCQ_STEP;
import static com.pemc.crss.shared.commons.reference.SettlementStepUtil.RETRIEVE_DATA_STEP;
import static com.pemc.crss.shared.core.dataflow.reference.SettlementJobName.CALC_GMR;
import static com.pemc.crss.shared.core.dataflow.reference.SettlementJobName.CALC_STL;
import static com.pemc.crss.shared.core.dataflow.reference.SettlementJobName.FILE_TA;
import static com.pemc.crss.shared.core.dataflow.reference.SettlementJobName.GEN_EBRSV_INPUT_WS;
import static com.pemc.crss.shared.core.dataflow.reference.SettlementJobName.TAG_TA;

@Slf4j
@Service("tradingAmountsTaskExecutionService")
@Transactional
public class TradingAmountsTaskExecutionServiceImpl extends StlTaskExecutionServiceImpl {

    private static final List<String> STL_GMR_CALC_STEP_WITH_SKIP_LOGS = Collections.singletonList(SettlementStepUtil.CALC_GMR_VAT);

    @Autowired
    private StlReadyJobQueryService stlReadyJobQueryService;

    @Override
    public Page<? extends StubTaskExecutionDto> findJobInstances(PageableRequest pageableRequest) {

        Page<DistinctStlReadyJob> stlReadyJobs = stlReadyJobQueryService.findDistinctStlReadyJobsForTradingAmounts(pageableRequest);
        List<DistinctStlReadyJob> distinctStlReadyJobs = stlReadyJobs.getContent();

        List<SettlementTaskExecutionDto> taskExecutionDtos = new ArrayList<>();

        for (DistinctStlReadyJob stlReadyJob : distinctStlReadyJobs) {

            String parentIdStr = stlReadyJob.getParentId();

            final Long parentId = Long.valueOf(parentIdStr);

            SettlementTaskExecutionDto taskExecutionDto = initializeTaskExecutionDto(stlReadyJob, parentIdStr);
            String stlReadyGroupId = taskExecutionDto.getStlReadyGroupId();

            StlJobGroupDto initialJobGroupDto = new StlJobGroupDto();
            initialJobGroupDto.setGroupId(stlReadyGroupId);

            taskExecutionDto.setParentStlJobGroupDto(initialJobGroupDto);

            Map<String, StlJobGroupDto> stlJobGroupDtoMap = new HashMap<>();
            stlJobGroupDtoMap.put(stlReadyGroupId, initialJobGroupDto);

            final MeterProcessType processType = taskExecutionDto.getProcessType();

            /* GENERATE INPUT WORKSPACE START */
            List<JobInstance> generateInputWsJobInstances = findJobInstancesByNameAndProcessTypeAndParentId(
                    GEN_EBRSV_INPUT_WS, processType, parentId);

            initializeGenInputWorkSpace(generateInputWsJobInstances, stlJobGroupDtoMap, taskExecutionDto, stlReadyGroupId);

            /* SETTLEMENT CALCULATION START */
            List<JobInstance> calculationJobInstances = findJobInstancesByNameAndProcessTypeAndParentId(
                    CALC_STL, processType, parentId);

            initializeStlCalculation(calculationJobInstances, stlJobGroupDtoMap, taskExecutionDto, stlReadyGroupId);

            /* CALCULATE GMR START */
            List<JobInstance> calculateGmrJobInstances = findJobInstancesByNameAndProcessTypeAndParentId(
                    CALC_GMR, processType, parentId);

            initializeCalculateGmr(calculateGmrJobInstances, stlJobGroupDtoMap, taskExecutionDto, stlReadyGroupId);

            /* FINALIZE START */
            List<JobInstance> taggingJobInstances = findJobInstancesByNameAndProcessTypeAndParentId(
                    TAG_TA, processType, parentId);

            initializeTagging(taggingJobInstances, stlJobGroupDtoMap, taskExecutionDto, stlReadyGroupId);

            /* GEN FILES START */
            List<JobInstance> genFileJobInstances = findJobInstancesByNameAndProcessTypeAndParentId(
                    FILE_TA, processType, parentId);

            initializeFileGen(genFileJobInstances, stlJobGroupDtoMap, taskExecutionDto, stlReadyGroupId);

            taskExecutionDto.setStlJobGroupDtoMap(stlJobGroupDtoMap);

            if (Arrays.asList(FINAL, ADJUSTED, PRELIM).contains(taskExecutionDto.getProcessType())) {
                determineIfJobsAreLocked(taskExecutionDto);
            }

            taskExecutionDto.getStlJobGroupDtoMap().values().forEach(stlJobGroupDto -> {

                boolean isDaily = taskExecutionDto.getProcessType().equals(DAILY);

                List<JobCalculationDto> jobDtos = stlJobGroupDto.getJobCalculationDtos();
                Date billPeriodStart = taskExecutionDto.getBillPeriodStartDate();
                Date billPeriodEnd = taskExecutionDto.getBillPeriodEndDate();

                stlJobGroupDto.setRemainingDatesCalc(getRemainingDatesForCalculation(jobDtos,billPeriodStart, billPeriodEnd));

                stlJobGroupDto.setRemainingDatesGenInputWs(getRemainingDatesForGenInputWs(jobDtos, billPeriodStart, billPeriodEnd));

                determineStlJobGroupDtoStatus(stlJobGroupDto, isDaily);

                if (!isDaily && stlJobGroupDto.isHeader()) {

                    List<ViewSettlementJob> viewSettlementJobs = stlReadyJobQueryService
                            .getStlReadyJobsByParentIdAndProcessType(processType, parentIdStr);

                    stlJobGroupDto.setOutdatedTradingDates(getOutdatedTradingDates(jobDtos,
                            viewSettlementJobs, billPeriodStart, billPeriodEnd));
                }
            });

            taskExecutionDtos.add(taskExecutionDto);
        }

        return new PageImpl<>(taskExecutionDtos, pageableRequest.getPageable(), stlReadyJobs.getTotalElements());
    }

    @Override
    public Page<GroupTaskExecutionDto> findDistinctBillingPeriodAndProcessType(Pageable pageable) {
        return null;
    }

    @Override
    public Page<? extends StubTaskExecutionDto> findJobInstancesByBillingPeriodAndProcessType(Pageable pageable, String billingPeriod, String processType) {
        return null;
    }

    @Override
    public void launchJob(TaskRunDto taskRunDto) throws URISyntaxException {
        validateJobName(taskRunDto.getJobName());

        log.info("Running JobName=[{}], type=[{}], baseType=[{}]", taskRunDto.getJobName(), taskRunDto.getMeterProcessType(),
                taskRunDto.getBaseType());

        switch (taskRunDto.getJobName()) {
            case GEN_EBRSV_INPUT_WS:
                validateJobName(CALC_STL);
                validateJobName(CALC_GMR);
                validateJobName(TAG_TA);
                launchGenerateInputWorkspaceJob(taskRunDto);
                break;
            case CALC_STL:
                validateJobName(GEN_EBRSV_INPUT_WS);
                validateJobName(CALC_GMR);
                validateJobName(TAG_TA);
                launchCalculateJob(taskRunDto);
                break;
            case CALC_GMR:
                validateJobName(GEN_EBRSV_INPUT_WS);
                validateJobName(CALC_STL);
                validateJobName(TAG_TA);
                launchCalculateGmrJob(taskRunDto);
                break;
            case TAG_TA:
                validateJobName(GEN_EBRSV_INPUT_WS);
                validateJobName(CALC_STL);
                validateJobName(CALC_GMR);
                launchFinalizeJob(taskRunDto);
                break;
            case FILE_TA:
                launchGenerateFileJob(taskRunDto);
                break;
            default:
                throw new RuntimeException("Job launch failed. Unhandled Job Name: " + taskRunDto.getJobName());
        }
    }


    // Calculate GMR is exclusive for Trading Amounts
    private void initializeCalculateGmr(final List<JobInstance> calculateGmrJobInstances,
                                        final Map<String, StlJobGroupDto> stlJobGroupDtoMap,
                                        final SettlementTaskExecutionDto taskExecutionDto,
                                        final String stlReadyGroupId) {

        Set<String> calcGmrNames = Sets.newHashSet();
        Map<String, List<JobCalculationDto>> gmrJobCalculationDtoMap = getCalcGmrJobCalculationMap(calculateGmrJobInstances);

        for (JobInstance calcGmrStlJobInstance : calculateGmrJobInstances) {
            String calcGmrStlJobName = calcGmrStlJobInstance.getJobName();
            if (calcGmrNames.contains(calcGmrStlJobName)) {
                continue;
            }

            JobExecution calcGmrJobExecution = getJobExecutionFromJobInstance(calcGmrStlJobInstance);

            JobParameters calcGmrJobParameters = calcGmrJobExecution.getJobParameters();
            String groupId = calcGmrJobParameters.getString(GROUP_ID);
            StlJobGroupDto stlJobGroupDto = stlJobGroupDtoMap.getOrDefault(groupId, new StlJobGroupDto());
            BatchStatus currentStatus = calcGmrJobExecution.getStatus();

            // add gmr calculations for view calculations
            Optional.ofNullable(gmrJobCalculationDtoMap.get(calcGmrStlJobName)).ifPresent(
                    jobCalcDtoList -> stlJobGroupDto.getJobCalculationDtos().addAll(jobCalcDtoList)
            );

            stlJobGroupDto.setGmrVatMFeeCalculationStatus(currentStatus);
            stlJobGroupDto.setGroupId(groupId);
            stlJobGroupDto.setGmrCalcRunDate(calcGmrJobExecution.getStartTime());

            if (!stlJobGroupDto.getLatestJobExecStartDate().after(calcGmrJobExecution.getStartTime())) {
                updateProgress(calcGmrJobExecution, stlJobGroupDto);
            }

            stlJobGroupDtoMap.put(groupId, stlJobGroupDto);

            if (stlReadyGroupId.equals(groupId)) {
                stlJobGroupDto.setHeader(true);
                taskExecutionDto.setParentStlJobGroupDto(stlJobGroupDto);
            }

            calcGmrNames.add(calcGmrStlJobName);
        }

    }

    private Map<String, List<JobCalculationDto>> getCalcGmrJobCalculationMap(List<JobInstance> calcGmrStlJobInstances) {
        Map<String, List<JobCalculationDto>> jobCalculationDtoMap = new HashMap<>();

        // add distinct jobNames for multiple same parentId-groupId job instances
        calcGmrStlJobInstances.stream().map(JobInstance::getJobName).distinct().forEach(calcJobInstanceName ->
                jobCalculationDtoMap.put(calcJobInstanceName, new ArrayList<>()));

        calcGmrStlJobInstances.forEach(calcGmrInstance -> getJobExecutions(calcGmrInstance).forEach(jobExecution -> {
            JobParameters calcGmrJobParameters = jobExecution.getJobParameters();
            Date calcGmrStartDate = calcGmrJobParameters.getDate(START_DATE);
            Date calcGmrEndDate = calcGmrJobParameters.getDate(END_DATE);
            JobCalculationDto gmrCalcDto = new JobCalculationDto(jobExecution.getStartTime(), jobExecution.getEndTime(),
                    calcGmrStartDate, calcGmrEndDate, convertStatus(jobExecution.getStatus(), StlJobStage.CALCULATE_GMR.getLabel()),
                    StlJobStage.CALCULATE_GMR, jobExecution.getStatus());
            gmrCalcDto.setTaskSummaryList(showSummary(jobExecution, STL_GMR_CALC_STEP_WITH_SKIP_LOGS));
            jobCalculationDtoMap.get(calcGmrInstance.getJobName()).add(gmrCalcDto);
        }));

        return jobCalculationDtoMap;
    }

    // Calculate GMR is exclusive for TTA
    private void launchCalculateGmrJob(final TaskRunDto taskRunDto) throws URISyntaxException {
        final Long runId = System.currentTimeMillis();
        final String groupId = taskRunDto.getGroupId();
        final String type = taskRunDto.getMeterProcessType();

        List<String> arguments = initializeJobArguments(taskRunDto, runId, groupId, type);
        arguments.add(concatKeyValue(START_DATE, taskRunDto.getBaseStartDate(), "date"));
        arguments.add(concatKeyValue(END_DATE, taskRunDto.getBaseEndDate(), "date"));

        List<String> properties = Lists.newArrayList();

        switch (MeterProcessType.valueOf(type)) {
            case PRELIM:
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(
                        SettlementJobProfile.CALC_MONTHLY_PRELIM_GMR_VAT)));
                break;
            case FINAL:
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(
                        SettlementJobProfile.CALC_MONTHLY_FINAL_GMR_VAT)));
                break;
            case ADJUSTED:
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(
                        SettlementJobProfile.CALC_MONTHLY_ADJ_GMR_VAT)));
                break;
            default:
                throw new RuntimeException("Failed to launch job. Unhandled processType: " + type);
        }

        log.info("Running calculate gmr job name={}, properties={}, arguments={}", taskRunDto.getJobName(), properties, arguments);

        launchJob(SPRING_BATCH_MODULE_STL_CALC, properties, arguments);
        lockJob(taskRunDto);
    }


    @Override
    StlCalculationType getStlCalculationType() {
        return StlCalculationType.TRADING_AMOUNTS;
    }

    @Override
    String getDailyGenInputWorkspaceProfile() {
        return SettlementJobProfile.GEN_DAILY_EBRSV_INPUT_WS;
    }

    @Override
    String getPrelimGenInputWorkspaceProfile() {
        return SettlementJobProfile.GEN_MONTHLY_PRELIM_EBRSV_INPUT_WS;
    }

    @Override
    String getFinalGenInputWorkspaceProfile() {
        return SettlementJobProfile.GEN_MONTHLY_FINAL_EBRSV_INPUT_WS;
    }

    @Override
    String getAdjustedMtrAdjGenInputWorkSpaceProfile() {
        return SettlementJobProfile.GEN_MONTHLY_ADJ_MTR_ADJ_EBRSV_INPUT_WS;
    }

    @Override
    String getAdjustedMtrFinGenInputWorkSpaceProfile() {
        return SettlementJobProfile.GEN_MONTHLY_ADJ_MTR_FIN_EBRSV_INPUT_WS;
    }

    @Override
    List<String> getInputWorkSpaceStepsForSkipLogs() {
        return Arrays.asList(RETRIEVE_DATA_STEP, RETRIEVE_BCQ_STEP);
    }

    @Override
    String getDailyCalculateProfile() {
        return SettlementJobProfile.CALC_DAILY_STL_AMTS;
    }

    @Override
    String getPrelimCalculateProfile() {
        return SettlementJobProfile.CALC_MONTHLY_PRELIM_STL_AMTS;
    }

    @Override
    String getFinalCalculateProfile() {
        return SettlementJobProfile.CALC_MONTHLY_FINAL_STL_AMTS;
    }

    @Override
    String getAdjustedMtrAdjCalculateProfile() {
        return SettlementJobProfile.CALC_MONTHLY_ADJ_STL_AMTS_MTR_ADJ;
    }

    @Override
    String getAdjustedMtrFinCalculateProfile() {
        return SettlementJobProfile.CALC_MONTHLY_ADJ_STL_AMTS_MTR_FIN;
    }

    @Override
    List<String> getCalculateStepsForSkipLogs() {
        return Arrays.asList(DISAGGREGATE_BCQ, CALC_SCALING_FACTOR);
    }

    @Override
    String getPrelimTaggingProfile() {
        return SettlementJobProfile.TAG_MONTHLY_PRELIM;
    }

    @Override
    String getFinalTaggingProfile() {
        return SettlementJobProfile.TAG_MONTHLY_FINAL;
    }

    @Override
    String getAdjustedTaggingProfile() {
        return SettlementJobProfile.TAG_MONTHLY_ADJ;
    }

    @Override
    String getPrelimGenFileProfile() {
        return SettlementJobProfile.GEN_FILE_PRELIM;
    }

    @Override
    String getFinalGenFileProfile() {
        return SettlementJobProfile.GEN_FILE_FINAL;
    }

    @Override
    String getAdjustedGenFileProfile() {
        return SettlementJobProfile.GEN_FILE_ADJ;
    }
}
