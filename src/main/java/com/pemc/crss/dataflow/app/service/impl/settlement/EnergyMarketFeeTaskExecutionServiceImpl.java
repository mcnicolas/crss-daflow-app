package com.pemc.crss.dataflow.app.service.impl.settlement;

import com.pemc.crss.dataflow.app.dto.JobCalculationDto;
import com.pemc.crss.dataflow.app.dto.SettlementTaskExecutionDto;
import com.pemc.crss.dataflow.app.dto.StlJobGroupDto;
import com.pemc.crss.dataflow.app.dto.TaskRunDto;
import com.pemc.crss.dataflow.app.dto.parent.GroupTaskExecutionDto;
import com.pemc.crss.dataflow.app.dto.parent.StubTaskExecutionDto;
import com.pemc.crss.dataflow.app.service.StlReadyJobQueryService;
import com.pemc.crss.dataflow.app.support.PageableRequest;
import com.pemc.crss.shared.commons.reference.MeterProcessType;
import com.pemc.crss.shared.core.dataflow.dto.DistinctStlReadyJob;
import com.pemc.crss.shared.core.dataflow.entity.ViewSettlementJob;
import com.pemc.crss.shared.core.dataflow.reference.SettlementJobProfile;
import com.pemc.crss.shared.core.dataflow.reference.StlCalculationType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobInstance;
import org.springframework.beans.factory.annotation.Autowired;
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

import static com.pemc.crss.shared.commons.reference.SettlementStepUtil.CALC_MARKET_FEE;
import static com.pemc.crss.shared.commons.reference.SettlementStepUtil.RETRIEVE_DATA_STEP;
import static com.pemc.crss.shared.core.dataflow.reference.SettlementJobName.*;

@Slf4j
@Service("energyMarketFeeTaskExecutionService")
@Transactional
public class EnergyMarketFeeTaskExecutionServiceImpl extends StlTaskExecutionServiceImpl {

    @Autowired
    private StlReadyJobQueryService stlReadyJobQueryService;

    @Override
    public Page<? extends StubTaskExecutionDto> findJobInstances(PageableRequest pageableRequest) {
        Page<DistinctStlReadyJob> stlReadyJobs = stlReadyJobQueryService.findDistinctStlReadyJobsForMarketFee(pageableRequest);
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

            MeterProcessType processType = taskExecutionDto.getProcessType();

            /* GENERATE INPUT WORKSPACE START */
            List<JobInstance> generateInputWsJobInstances = findJobInstancesByNameAndProcessTypeAndParentId(
                    GEN_EMF_INPUT_WS, processType, parentId);

            initializeGenInputWorkSpace(generateInputWsJobInstances, stlJobGroupDtoMap, taskExecutionDto, stlReadyGroupId);

            /* SETTLEMENT CALCULATION START */
            List<JobInstance> calculationJobInstances = findJobInstancesByNameAndProcessTypeAndParentId(
                    CALC_EMF, processType, parentId);

            initializeStlCalculation(calculationJobInstances, stlJobGroupDtoMap, taskExecutionDto, stlReadyGroupId);

            /* FINALIZE START */
            List<JobInstance> taggingJobInstances = findJobInstancesByNameAndProcessTypeAndParentId(
                    TAG_EMF, processType, parentId);

            initializeTagging(taggingJobInstances, stlJobGroupDtoMap, taskExecutionDto, stlReadyGroupId);

            /* GEN FILES START */
            List<JobInstance> genFileJobInstances = findJobInstancesByNameAndProcessTypeAndParentId(
                    FILE_EMF, processType, parentId);

            initializeFileGen(genFileJobInstances, stlJobGroupDtoMap, taskExecutionDto, stlReadyGroupId);

            taskExecutionDto.setStlJobGroupDtoMap(stlJobGroupDtoMap);

            determineIfJobsAreLocked(taskExecutionDto);

            taskExecutionDto.getStlJobGroupDtoMap().values().forEach(stlJobGroupDto -> {
                List<JobCalculationDto> jobDtos = stlJobGroupDto.getJobCalculationDtos();
                Date billPeriodStart = taskExecutionDto.getBillPeriodStartDate();
                Date billPeriodEnd = taskExecutionDto.getBillPeriodEndDate();

                stlJobGroupDto.setRemainingDatesCalc(getRemainingDatesForCalculation(jobDtos,billPeriodStart, billPeriodEnd));

                stlJobGroupDto.setRemainingDatesGenInputWs(getRemainingDatesForGenInputWs(jobDtos, billPeriodStart, billPeriodEnd));

                determineStlJobGroupDtoStatus(stlJobGroupDto, false);

                if (stlJobGroupDto.isHeader()) {

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
            case GEN_EMF_INPUT_WS:
                validateJobName(CALC_EMF);
                validateJobName(TAG_EMF);
                launchGenerateInputWorkspaceJob(taskRunDto);
                break;
            case CALC_EMF:
                validateJobName(GEN_EMF_INPUT_WS);
                validateJobName(TAG_EMF);
                launchCalculateJob(taskRunDto);
                break;
            case TAG_EMF:
                validateJobName(GEN_EMF_INPUT_WS);
                validateJobName(CALC_EMF);
                launchFinalizeJob(taskRunDto);
                break;
            case FILE_EMF:
                launchGenerateFileJob(taskRunDto);
                break;
            default:
                throw new RuntimeException("Job launch failed. Unhandled Job Name: " + taskRunDto.getJobName());
        }
    }

    @Override
    StlCalculationType getStlCalculationType() {
        return StlCalculationType.ENERGY_MARKET_FEE;
    }

    // No Daily calculation needed for Market Fee
    @Override
    String getDailyGenInputWorkspaceProfile() {
        return null;
    }

    @Override
    String getPrelimGenInputWorkspaceProfile() {
        return SettlementJobProfile.GEN_MONTHLY_PRELIM_EMF_INPUT_WS;
    }

    @Override
    String getFinalGenInputWorkspaceProfile() {
        return SettlementJobProfile.GEN_MONTHLY_FINAL_EMF_INPUT_WS;
    }

    // determine if profile for adj is split
    @Override
    String getAdjustedMtrAdjGenInputWorkSpaceProfile() {
        return SettlementJobProfile.GEN_MONTHLY_ADJ_MTR_ADJ_EMF_INPUT_WS;
    }

    @Override
    String getAdjustedMtrFinGenInputWorkSpaceProfile() {
        return SettlementJobProfile.GEN_MONTHLY_ADJ_MTR_FIN_EMF_INPUT_WS ;
    }

    @Override
    List<String> getInputWorkSpaceStepsForSkipLogs() {
        return Arrays.asList(RETRIEVE_DATA_STEP);
    }

    // no DAILY calculation for EMF
    @Override
    String getDailyCalculateProfile() {
        return null;
    }

    @Override
    String getPrelimCalculateProfile() {
        return SettlementJobProfile.CALC_EMF_MONTHLY_PRELIM;
    }

    @Override
    String getFinalCalculateProfile() {
        return SettlementJobProfile.CALC_EMF_MONTHLY_FINAL;
    }

    // Energy Market Fee uses only one profile for Adjusted Calculate jobs
    @Override
    String getAdjustedMtrAdjCalculateProfile() {
        return SettlementJobProfile.CALC_EMF_MONTHLY_ADJUSTED;
    }

    @Override
    String getAdjustedMtrFinCalculateProfile() {
        return SettlementJobProfile.CALC_EMF_MONTHLY_ADJUSTED;
    }

    @Override
    List<String> getCalculateStepsForSkipLogs() {
        return Arrays.asList(CALC_MARKET_FEE);
    }

    @Override
    String getPrelimTaggingProfile() {
        return SettlementJobProfile.TAG_MONTHLY_PRELIM_EMF;
    }

    @Override
    String getFinalTaggingProfile() {
        return SettlementJobProfile.TAG_MONTHLY_FINAL_EMF;
    }

    @Override
    String getAdjustedTaggingProfile() {
        return SettlementJobProfile.TAG_MONTHLY_ADJ_EMF;
    }

    @Override
    String getPrelimGenFileProfile() {
        return SettlementJobProfile.GEN_FILE_PRELIM_EMF;
    }

    @Override
    String getFinalGenFileProfile() {
        return SettlementJobProfile.GEN_FILE_FINAL_EMF;
    }

    @Override
    String getAdjustedGenFileProfile() {
        return SettlementJobProfile.GEN_FILE_ADJ_EMF;
    }
}
