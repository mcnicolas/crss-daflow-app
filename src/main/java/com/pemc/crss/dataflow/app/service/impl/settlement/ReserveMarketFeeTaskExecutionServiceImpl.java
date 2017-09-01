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
import java.util.*;

import static com.pemc.crss.shared.commons.reference.SettlementStepUtil.GEN_RESERVE_IW_STEP;
import static com.pemc.crss.shared.commons.reference.SettlementStepUtil.RETRIEVE_DATA_STEP;
import static com.pemc.crss.shared.core.dataflow.reference.SettlementJobName.CALC_RMF;
import static com.pemc.crss.shared.core.dataflow.reference.SettlementJobName.FILE_RMF;
import static com.pemc.crss.shared.core.dataflow.reference.SettlementJobName.GEN_RMF_INPUT_WS;
import static com.pemc.crss.shared.core.dataflow.reference.SettlementJobName.TAG_RMF;

@Slf4j
@Service("reserveMarketFeeTaskExecutionService")
@Transactional
public class ReserveMarketFeeTaskExecutionServiceImpl extends StlTaskExecutionServiceImpl {

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

            MeterProcessType processType = taskExecutionDto.getProcessType();

            Map<String, StlJobGroupDto> stlJobGroupDtoMap = new HashMap<>();
            stlJobGroupDtoMap.put(stlReadyGroupId, initialJobGroupDto);

            /* GENERATE INPUT WORKSPACE START */
            List<JobInstance> generateInputWsJobInstances = findJobInstancesByNameAndProcessTypeAndParentId(
                    GEN_RMF_INPUT_WS, processType, parentId);

            initializeGenInputWorkSpace(generateInputWsJobInstances, stlJobGroupDtoMap, taskExecutionDto, stlReadyGroupId);

            /* SETTLEMENT CALCULATION START */
            List<JobInstance> calculationJobInstances = findJobInstancesByNameAndProcessTypeAndParentId(
                    CALC_RMF, processType, parentId);

            initializeStlCalculation(calculationJobInstances, stlJobGroupDtoMap, taskExecutionDto, stlReadyGroupId);

            /* FINALIZE START */
            List<JobInstance> taggingJobInstances = findJobInstancesByNameAndProcessTypeAndParentId(
                    TAG_RMF, processType, parentId);

            initializeTagging(taggingJobInstances, stlJobGroupDtoMap, taskExecutionDto, stlReadyGroupId);

            /* GEN FILES START */
            List<JobInstance> genFileJobInstances = findJobInstancesByNameAndProcessTypeAndParentId(
                    FILE_RMF, processType, parentId);

            initializeFileGen(genFileJobInstances, stlJobGroupDtoMap, taskExecutionDto, stlReadyGroupId);

            taskExecutionDto.setStlJobGroupDtoMap(stlJobGroupDtoMap);

            determineIfJobsAreLocked(taskExecutionDto, StlCalculationType.RESERVE_MARKET_FEE);

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
            case GEN_RMF_INPUT_WS:
                validateJobName(CALC_RMF);
                validateJobName(TAG_RMF);
                launchGenerateInputWorkspaceJob(taskRunDto, StlCalculationType.RESERVE_MARKET_FEE);
                break;
            case CALC_RMF:
                validateJobName(GEN_RMF_INPUT_WS);
                validateJobName(TAG_RMF);
                launchCalculateJob(taskRunDto);
                break;
            case TAG_RMF:
                validateJobName(GEN_RMF_INPUT_WS);
                validateJobName(CALC_RMF);
                launchFinalizeJob(taskRunDto);
                break;
            case FILE_RMF:
                launchGenerateFileJob(taskRunDto, StlCalculationType.RESERVE_MARKET_FEE);
                break;
            default:
                throw new RuntimeException("Job launch failed. Unhandled Job Name: " + taskRunDto.getJobName());
        }
    }

    // No Daily calculation needed for Market Fee
    @Override
    String getDailyGenInputWorkspaceProfile() {
        return null;
    }

    @Override
    String getPrelimGenInputWorkspaceProfile() {
        return SettlementJobProfile.GEN_MONTHLY_PRELIM_RMF_INPUT_WS;
    }

    @Override
    String getFinalGenInputWorkspaceProfile() {
        return SettlementJobProfile.GEN_MONTHLY_FINAL_RMF_INPUT_WS;
    }

    // determine if profile for adj is split
    @Override
    String getAdjustedMtrAdjGenInputWorkSpaceProfile() {
        return SettlementJobProfile.GEN_MONTHLY_ADJ_MTR_ADJ_RMF_INPUT_WS;
    }

    @Override
    String getAdjustedMtrFinGenInputWorkSpaceProfile() {
        return SettlementJobProfile.GEN_MONTHLY_ADJ_MTR_FIN_RMF_INPUT_WS;
    }

    @Override
    List<String> getInputWorkSpaceStepsForSkipLogs() {
        return Arrays.asList(RETRIEVE_DATA_STEP, GEN_RESERVE_IW_STEP);
    }

    @Override
    String getDailyCalculateProfile() {
        return null;
    }

    @Override
    String getPrelimCalculateProfile() {
        return SettlementJobProfile.CALC_RMF_MONTHLY_PRELIM;
    }

    @Override
    String getFinalCalculateProfile() {
        return SettlementJobProfile.CALC_RMF_MONTHLY_FINAL;
    }

    @Override
    String getAdjustedMtrAdjCalculateProfile() {
        return SettlementJobProfile.CALC_RMF_MONTHLY_ADJUSTED;
    }

    @Override
    String getAdjustedMtrFinCalculateProfile() {
        return SettlementJobProfile.CALC_RMF_MONTHLY_ADJUSTED;
    }

    // TODO: add steps with skip logs
    @Override
    List<String> getCalculateStepsForSkipLogs() {
        return new ArrayList<>();
    }

    @Override
    String getPrelimTaggingProfile() {
        return SettlementJobProfile.TAG_MONTHLY_PRELIM_RMF;
    }

    @Override
    String getFinalTaggingProfile() {
        return SettlementJobProfile.TAG_MONTHLY_FINAL_RMF;
    }

    @Override
    String getAdjustedTaggingProfile() {
        return SettlementJobProfile.TAG_MONTHLY_ADJ_RMF;
    }

    @Override
    String getPrelimGenFileProfile() {
        return SettlementJobProfile.GEN_FILE_PRELIM_RMF;
    }

    @Override
    String getFinalGenFileProfile() {
        return SettlementJobProfile.GEN_FILE_FINAL_RMF;
    }

    @Override
    String getAdjustedGenFileProfile() {
        return SettlementJobProfile.GEN_FILE_ADJ_RMF;
    }
}
