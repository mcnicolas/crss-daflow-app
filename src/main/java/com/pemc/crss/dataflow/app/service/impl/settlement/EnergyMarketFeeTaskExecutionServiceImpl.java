package com.pemc.crss.dataflow.app.service.impl.settlement;

import com.pemc.crss.dataflow.app.dto.SettlementTaskExecutionDto;
import com.pemc.crss.dataflow.app.dto.StlJobGroupDto;
import com.pemc.crss.dataflow.app.dto.TaskRunDto;
import com.pemc.crss.dataflow.app.dto.parent.GroupTaskExecutionDto;
import com.pemc.crss.dataflow.app.dto.parent.StubTaskExecutionDto;
import com.pemc.crss.dataflow.app.service.StlReadyJobQueryService;
import com.pemc.crss.dataflow.app.support.PageableRequest;
import com.pemc.crss.shared.core.dataflow.dto.DistinctStlReadyJob;
import com.pemc.crss.shared.core.dataflow.reference.SettlementJobProfile;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
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
            String parentId = stlReadyJob.getJobName().split("-")[1];

            if (StringUtils.isEmpty(parentId)) {
                log.warn("Parent id not appended for stlReadyJob with name {}. Skipping...", stlReadyJob.getJobName());
                continue;
            }

            SettlementTaskExecutionDto taskExecutionDto = initializeTaskExecutionDto(stlReadyJob, parentId);
            Long stlReadyJobId = taskExecutionDto.getStlReadyJobId();

            Map<Long, StlJobGroupDto> stlJobGroupDtoMap = new HashMap<>();

            /* GENERATE INPUT WORKSPACE START */
            List<JobInstance> generateInputWsJobInstances = findJobInstancesByJobNameAndParentId(
                    GEN_EMF_INPUT_WS, parentId);

            initializeGenInputWorkSpace(generateInputWsJobInstances, stlJobGroupDtoMap, taskExecutionDto, stlReadyJobId);

            /* SETTLEMENT CALCULATION START */
            List<JobInstance> calculationJobInstances = findJobInstancesByJobNameAndParentId(CALC_EMF, parentId);

            initializeStlCalculation(calculationJobInstances, stlJobGroupDtoMap, taskExecutionDto, stlReadyJobId);

            /* FINALIZE START */

            /* GEN FILES START */

            taskExecutionDto.setStlJobGroupDtoMap(stlJobGroupDtoMap);
            taskExecutionDtos.add(taskExecutionDto);
        }

        return new PageImpl<>(taskExecutionDtos, pageableRequest.getPageable(), stlReadyJobs.getTotalElements());
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
            case GEN_EMF_INPUT_WS:
                launchGenerateInputWorkspaceJob(taskRunDto);
                break;
            case CALC_EMF:
                launchCalculateJob(taskRunDto);
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
        return null;
    }

    @Override
    String getFinalTaggingProfile() {
        return null;
    }

    @Override
    String getAdjustedTaggingProfile() {
        return null;
    }

    @Override
    String getPrelimGenFileProfile() {
        return null;
    }

    @Override
    String getFinalGenFileProfile() {
        return null;
    }

    @Override
    String getAdjustedGenFileProfile() {
        return null;
    }
}
