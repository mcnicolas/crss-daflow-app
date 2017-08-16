package com.pemc.crss.dataflow.app.service.impl.settlement;

import com.pemc.crss.dataflow.app.dto.SettlementTaskExecutionDto;
import com.pemc.crss.dataflow.app.dto.StlJobGroupDto;
import com.pemc.crss.dataflow.app.dto.TaskRunDto;
import com.pemc.crss.dataflow.app.dto.parent.GroupTaskExecutionDto;
import com.pemc.crss.dataflow.app.dto.parent.StubTaskExecutionDto;
import com.pemc.crss.dataflow.app.service.StlReadyJobQueryService;
import com.pemc.crss.dataflow.app.support.PageableRequest;
import com.pemc.crss.shared.commons.reference.MeterProcessType;
import com.pemc.crss.shared.commons.util.DateUtil;
import com.pemc.crss.shared.core.dataflow.dto.DistinctStlReadyJob;
import com.pemc.crss.shared.core.dataflow.entity.SettlementJobLock;
import com.pemc.crss.shared.core.dataflow.reference.SettlementJobProfile;
import com.pemc.crss.shared.core.dataflow.repository.SettlementJobLockRepository;
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

import static com.pemc.crss.shared.commons.reference.MeterProcessType.ADJUSTED;
import static com.pemc.crss.shared.commons.reference.MeterProcessType.FINAL;
import static com.pemc.crss.shared.commons.reference.SettlementStepUtil.GEN_RESERVE_IW_STEP;
import static com.pemc.crss.shared.commons.reference.SettlementStepUtil.RETRIEVE_DATA_STEP;
import static com.pemc.crss.shared.core.dataflow.reference.SettlementJobName.*;

@Slf4j
@Service("reserveMarketFeeTaskExecutionService")
@Transactional
public class ReserveMarketFeeTaskExecutionServiceImpl extends StlTaskExecutionServiceImpl {

    @Autowired
    private StlReadyJobQueryService stlReadyJobQueryService;

    @Autowired
    private SettlementJobLockRepository settlementJobLockRepository;

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
            String stlReadyGroupId = taskExecutionDto.getStlReadyGroupId();

            StlJobGroupDto initialJobGroupDto = new StlJobGroupDto();
            initialJobGroupDto.setGroupId(stlReadyGroupId);

            taskExecutionDto.setParentStlJobGroupDto(initialJobGroupDto);

            Map<String, StlJobGroupDto> stlJobGroupDtoMap = new HashMap<>();
            stlJobGroupDtoMap.put(stlReadyGroupId, initialJobGroupDto);

            /* GENERATE INPUT WORKSPACE START */
            List<JobInstance> generateInputWsJobInstances = findJobInstancesByJobNameAndParentId(
                    GEN_RMF_INPUT_WS, parentId);

            initializeGenInputWorkSpace(generateInputWsJobInstances, stlJobGroupDtoMap, taskExecutionDto, stlReadyGroupId);

            /* SETTLEMENT CALCULATION START */

            /* FINALIZE START */

            /* GEN FILES START */

            taskExecutionDto.setStlJobGroupDtoMap(stlJobGroupDtoMap);

            if (Arrays.asList(FINAL, ADJUSTED).contains(MeterProcessType.valueOf(taskExecutionDto.getProcessType()))) {
                determineIfJobsAreLocked(taskExecutionDto);
            }

            taskExecutionDtos.add(taskExecutionDto);
        }

        return new PageImpl<>(taskExecutionDtos, pageableRequest.getPageable(), stlReadyJobs.getTotalElements());
    }

    private void determineIfJobsAreLocked(final SettlementTaskExecutionDto taskExecutionDto) {

        List<SettlementJobLock> stlJobLocks = settlementJobLockRepository.findByStartDateAndEndDate(
                DateUtil.convertToLocalDateTime(taskExecutionDto.getBillPeriodStartDate()),
                DateUtil.convertToLocalDateTime(taskExecutionDto.getBillPeriodEndDate()));

        for (StlJobGroupDto stlJobGroupDto : taskExecutionDto.getStlJobGroupDtoMap().values()) {

            stlJobLocks.stream().filter(stlJobLock -> stlJobLock.getGroupId().equals(stlJobGroupDto.getGroupId())
                    && stlJobLock.getParentJobId().equals(taskExecutionDto.getParentId()))
                    .findFirst().ifPresent(stlLock -> stlJobGroupDto.setLocked(stlLock.isLockedRmf()));

            // additional lock checking for adjusted type
            if (ADJUSTED.equals(MeterProcessType.valueOf(taskExecutionDto.getProcessType()))) {
                stlJobLocks.stream().filter(s -> s.getProcessType() == FINAL).findFirst().ifPresent(stlLock -> {
                    // if FINAL run is not yet tagged, ADJUSTED cannot be run yet.
                    if (!stlLock.isLockedRmf()) {
                        stlJobGroupDto.setLocked(true);
                    }
                });
            }
        }

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
            case GEN_RMF_INPUT_WS:
                launchGenerateInputWorkspaceJob(taskRunDto);
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
        return null;
    }

    @Override
    String getFinalCalculateProfile() {
        return null;
    }

    @Override
    String getAdjustedMtrAdjCalculateProfile() {
        return null;
    }

    @Override
    String getAdjustedMtrFinCalculateProfile() {
        return null;
    }

    // TODO: add steps with skip logs
    @Override
    List<String> getCalculateStepsForSkipLogs() {
        return new ArrayList<>();
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
