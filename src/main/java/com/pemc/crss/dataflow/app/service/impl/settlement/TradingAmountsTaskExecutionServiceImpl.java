package com.pemc.crss.dataflow.app.service.impl.settlement;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.pemc.crss.dataflow.app.dto.JobCalculationDto;
import com.pemc.crss.dataflow.app.dto.SettlementTaskExecutionDto;
import com.pemc.crss.dataflow.app.dto.StlJobGroupDto;
import com.pemc.crss.dataflow.app.dto.TaskRunDto;
import com.pemc.crss.dataflow.app.dto.parent.GroupTaskExecutionDto;
import com.pemc.crss.dataflow.app.dto.parent.StubTaskExecutionDto;
import com.pemc.crss.dataflow.app.support.PageableRequest;
import com.pemc.crss.shared.commons.reference.SettlementStepUtil;
import com.pemc.crss.shared.core.dataflow.reference.SettlementJobName;
import com.pemc.crss.shared.core.dataflow.reference.SettlementJobProfile;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameters;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import javax.transaction.Transactional;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.pemc.crss.shared.commons.reference.SettlementStepUtil.CALC_SCALING_FACTOR;
import static com.pemc.crss.shared.commons.reference.SettlementStepUtil.DISAGGREGATE_BCQ;
import static com.pemc.crss.shared.commons.reference.SettlementStepUtil.RETRIEVE_BCQ_STEP;
import static com.pemc.crss.shared.commons.reference.SettlementStepUtil.RETRIEVE_DATA_STEP;
import static com.pemc.crss.shared.core.dataflow.reference.SettlementJobName.CALC_GMR;
import static com.pemc.crss.shared.core.dataflow.reference.SettlementJobName.CALC_STL;
import static com.pemc.crss.shared.core.dataflow.reference.SettlementJobName.GEN_EBRSV_INPUT_WS;

@Slf4j
@Service("tradingAmountsTaskExecutionService")
@Transactional
public class TradingAmountsTaskExecutionServiceImpl extends StlTaskExecutionServiceImpl {

    private static final String STAGE_GMR_CALC = "CALCULATION-GMR";
    private static final List<String> STL_GMR_CALC_STEP_WITH_SKIP_LOGS = Arrays.asList(SettlementStepUtil.CALC_MARKET_FEE,
            SettlementStepUtil.CALC_RESERVE_MARKET_FEE, SettlementStepUtil.CALC_GMR_VAT);

    @Override
    public Page<? extends StubTaskExecutionDto> findJobInstances(PageableRequest pageableRequest) {
        final Long totalSize = dataFlowJdbcJobExecutionDao.countStlJobInstances(pageableRequest);

        List<JobInstance> stlReadyJobInstances = findStlReadyJobInstances(pageableRequest);
        List<SettlementTaskExecutionDto> taskExecutionDtos = new ArrayList<>();

        for (JobInstance jobInstance : stlReadyJobInstances ) {
            List<JobExecution> stlJobExecutions = getCompletedJobExecutions(jobInstance);

            for (JobExecution stlJobExecution : stlJobExecutions) {
                String parentId = jobInstance.getJobName().split("-")[1];

                if (StringUtils.isEmpty(parentId)) {
                    log.warn("Parent id not appended for job instance id {}. Skipping...", jobInstance.getId());
                    continue;
                }

                SettlementTaskExecutionDto taskExecutionDto = initializeTaskExecutionDto(stlJobExecution, parentId);
                Long stlReadyJobId = taskExecutionDto.getStlReadyJobId();

                Map<Long, StlJobGroupDto> stlJobGroupDtoMap = new HashMap<>();

                /* GENERATE INPUT WORKSPACE START */
                List<JobInstance> generateInputWsJobInstances = findJobInstancesByJobNameAndParentId(
                        SettlementJobName.GEN_EBRSV_INPUT_WS, parentId);

                initializeGenInputWorkSpace(generateInputWsJobInstances, stlJobGroupDtoMap, taskExecutionDto, stlReadyJobId);

                /* SETTLEMENT CALCULATION START */
                List<JobInstance> calculationJobInstances = findJobInstancesByJobNameAndParentId(CALC_STL, parentId);

                initializeStlCalculation(calculationJobInstances, stlJobGroupDtoMap, taskExecutionDto, stlReadyJobId);

                /* CALCULATE GMR START */
                List<JobInstance> calculateGmrJobInstances = findJobInstancesByJobNameAndParentId(CALC_GMR, parentId);

                initializeCalculateGmr(calculateGmrJobInstances, stlJobGroupDtoMap, taskExecutionDto, stlReadyJobId);

                /* FINALIZE START */

                /* GEN FILES START */

                taskExecutionDto.setStlJobGroupDtoMap(stlJobGroupDtoMap);
                taskExecutionDtos.add(taskExecutionDto);
            }
        }

        return new PageImpl<>(taskExecutionDtos, pageableRequest.getPageable(), totalSize);
    }

    @Override
    public Page<GroupTaskExecutionDto> findJobInstancesGroupByBillingPeriod(Pageable pageable) {
        return null;
    }

    @Override
    public void launchJob(TaskRunDto taskRunDto) throws URISyntaxException {
        validateJobName(taskRunDto.getJobName());

        log.info("Running JobName=[{}], type=[{}], baseType=[{}]", taskRunDto.getJobName(), taskRunDto.getMeterProcessType(),
                taskRunDto.getBaseType());

        switch (taskRunDto.getJobName()) {
            case GEN_EBRSV_INPUT_WS:
                launchGenerateInputWorkspaceJob(taskRunDto);
                break;
            case CALC_STL:
                launchCalculateJob(taskRunDto);
            case CALC_GMR:
                launchCalculateGmrJob(taskRunDto);
                break;
            default:
                throw new RuntimeException("Job launch failed. Unhandled Job Name: " + taskRunDto.getJobName());
        }
    }


    // Calculate GMR is exclusive for TTA
    private void initializeCalculateGmr(final List<JobInstance> calculateGmrJobInstances,
                                        final Map<Long, StlJobGroupDto> stlJobGroupDtoMap,
                                        final SettlementTaskExecutionDto taskExecutionDto,
                                        final Long stlReadyJobId) {

        Set<String> calcGmrNames = Sets.newHashSet();
        Map<String, List<JobCalculationDto>> gmrJobCalculationDtoMap = getCalcGmrJobCalculationMap(calculateGmrJobInstances);

        for (JobInstance calcGmrStlJobInstance : calculateGmrJobInstances) {
            String calcGmrStlJobName = calcGmrStlJobInstance.getJobName();
            if (calcGmrNames.contains(calcGmrStlJobName)) {
                continue;
            }

            JobExecution calcGmrJobExecution = getJobExecutionFromJobInstance(calcGmrStlJobInstance);

            JobParameters calcGmrJobParameters = calcGmrJobExecution.getJobParameters();
            Long groupId = calcGmrJobParameters.getLong(GROUP_ID);
            StlJobGroupDto stlJobGroupDto = stlJobGroupDtoMap.getOrDefault(groupId, new StlJobGroupDto());
            BatchStatus currentStatus = calcGmrJobExecution.getStatus();

            // add gmr calculations for view calculations
            Optional.ofNullable(gmrJobCalculationDtoMap.get(calcGmrStlJobName)).ifPresent(
                    jobCalcDtoList -> stlJobGroupDto.getJobCalculationDtos().addAll(jobCalcDtoList)
            );

            stlJobGroupDto.setStatus(convertStatus(currentStatus, STAGE_GMR_CALC));
            stlJobGroupDto.setGmrVatMFeeCalculationStatus(currentStatus);
            stlJobGroupDto.setGroupId(groupId);
            stlJobGroupDto.setGmrCalcRunDate(calcGmrJobExecution.getStartTime());

            // change status to <latest calc job execution status> - FULL-CALCULATION if for GMR Recalculation
            if (stlJobGroupDto.isForGmrRecalculation()) {
                String latestJobCalcStatus = getLatestJobCalcStatusByStage(stlJobGroupDto, STAGE_PARTIAL_CALC);
                BatchStatus latestJobExecCalcStatus = BatchStatus.valueOf(latestJobCalcStatus.split("-")[0]);

                stlJobGroupDto.setStatus(convertStatus(latestJobExecCalcStatus, STATUS_FULL_STL_CALC));
            }

            if (!stlJobGroupDto.getLatestJobExecStartDate().after(calcGmrJobExecution.getStartTime())) {
                updateProgress(calcGmrJobExecution, stlJobGroupDto);
            }

            stlJobGroupDtoMap.put(groupId, stlJobGroupDto);

            if (stlReadyJobId.equals(groupId)) {
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
            JobCalculationDto gmrCalcDto = new JobCalculationDto(jobExecution.getStartTime(), jobExecution.getEndTime(),  calcGmrStartDate,
                    calcGmrEndDate, convertStatus(jobExecution.getStatus(), STAGE_GMR_CALC), STAGE_GMR_CALC);
            gmrCalcDto.setTaskSummaryList(showSummary(jobExecution, STL_GMR_CALC_STEP_WITH_SKIP_LOGS));
            jobCalculationDtoMap.get(calcGmrInstance.getJobName()).add(gmrCalcDto);
        }));

        return jobCalculationDtoMap;
    }

    // Calculate GMR is exclusive for TTA
    private void launchCalculateGmrJob(final TaskRunDto taskRunDto) throws URISyntaxException {
        final Long runId = System.currentTimeMillis();
        final Long groupId = Long.parseLong(taskRunDto.getGroupId());
        final String type = taskRunDto.getMeterProcessType();

        List<String> arguments = initializeJobArguments(taskRunDto, runId, groupId);
        arguments.add(concatKeyValue(PROCESS_TYPE, type));

        List<String> properties = Lists.newArrayList();

        switch (type) {
            case "PRELIM":
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(
                        SettlementJobProfile.CALC_MONTHLY_PRELIM_GMR_VAT)));
                arguments.add(concatKeyValue(START_DATE, taskRunDto.getStartDate(), "date"));
                arguments.add(concatKeyValue(END_DATE, taskRunDto.getEndDate(), "date"));
                break;
            case "FINAL":
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(
                        SettlementJobProfile.CALC_MONTHLY_FINAL_GMR_VAT)));
                arguments.add(concatKeyValue(START_DATE, taskRunDto.getStartDate(), "date"));
                arguments.add(concatKeyValue(END_DATE, taskRunDto.getEndDate(), "date"));
                break;
            case "ADJUSTED":
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(
                        SettlementJobProfile.CALC_MONTHLY_ADJ_GMR_VAT)));
                arguments.add(concatKeyValue(START_DATE, taskRunDto.getStartDate(), "date"));
                arguments.add(concatKeyValue(END_DATE, taskRunDto.getEndDate(), "date"));
                break;
            default:
                throw new RuntimeException("Failed to launch job. Unhandled processType: " + type);
        }

        log.info("Running calculate gmr job name={}, properties={}, arguments={}", taskRunDto.getJobName(), properties, arguments);

        launchJob("crss-settlement-task-calculation", properties, arguments);
        lockJob(taskRunDto);
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
}
