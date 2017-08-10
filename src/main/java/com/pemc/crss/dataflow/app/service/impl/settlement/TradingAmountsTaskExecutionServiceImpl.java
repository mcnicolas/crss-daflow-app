package com.pemc.crss.dataflow.app.service.impl.settlement;

import com.pemc.crss.dataflow.app.dto.SettlementTaskExecutionDto;
import com.pemc.crss.dataflow.app.dto.StlJobGroupDto;
import com.pemc.crss.dataflow.app.dto.TaskRunDto;
import com.pemc.crss.dataflow.app.dto.parent.GroupTaskExecutionDto;
import com.pemc.crss.dataflow.app.dto.parent.StubTaskExecutionDto;
import com.pemc.crss.dataflow.app.support.PageableRequest;
import com.pemc.crss.shared.core.dataflow.reference.SettlementJobName;
import com.pemc.crss.shared.core.dataflow.reference.SettlementJobProfile;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
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

import static com.pemc.crss.shared.commons.reference.SettlementStepUtil.CALC_SCALING_FACTOR;
import static com.pemc.crss.shared.commons.reference.SettlementStepUtil.DISAGGREGATE_BCQ;
import static com.pemc.crss.shared.commons.reference.SettlementStepUtil.RETRIEVE_BCQ_STEP;
import static com.pemc.crss.shared.commons.reference.SettlementStepUtil.RETRIEVE_DATA_STEP;
import static com.pemc.crss.shared.core.dataflow.reference.SettlementJobName.CALC_STL;
import static com.pemc.crss.shared.core.dataflow.reference.SettlementJobName.GEN_EBRSV_INPUT_WS;

@Slf4j
@Service("tradingAmountsTaskExecutionService")
@Transactional
public class TradingAmountsTaskExecutionServiceImpl extends StlTaskExecutionServiceImpl {

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
                break;
            default:
                throw new RuntimeException("Job launch failed. Unhandled Job Name: " + taskRunDto.getJobName());
        }
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
