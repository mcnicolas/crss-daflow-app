package com.pemc.crss.dataflow.app.service.impl.settlement;

import com.pemc.crss.dataflow.app.dto.SettlementTaskExecutionDto;
import com.pemc.crss.dataflow.app.dto.StlJobGroupDto;
import com.pemc.crss.dataflow.app.dto.TaskRunDto;
import com.pemc.crss.dataflow.app.dto.parent.StubTaskExecutionDto;
import com.pemc.crss.dataflow.app.support.PageableRequest;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.stereotype.Service;

import javax.transaction.Transactional;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service("reserveMarketFeeTaskExecutionService")
@Transactional
public class ReserveMarketFeeTaskExecutionServiceImpl extends StlTaskExecutionServiceImpl {

    private static final String GEN_WS_RMF_JOB_NAME = "genRmfIw";

    // TODO: for removal. used for mocking purposes only using old data.
    private static final String COMPUTE_STL_JOB_NAME = "calcSTL_AMT";

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
                        COMPUTE_STL_JOB_NAME, parentId);

                initializeGenInputWorkSpace(generateInputWsJobInstances, stlJobGroupDtoMap, taskExecutionDto, stlReadyJobId);

                /* SETTLEMENT CALCULATION START */

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
    public void launchJob(TaskRunDto taskRunDto) throws URISyntaxException {

    }
}
