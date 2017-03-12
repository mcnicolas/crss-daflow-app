package com.pemc.crss.dataflow.app.service;

import com.pemc.crss.dataflow.app.dto.BaseTaskExecutionDto;
import com.pemc.crss.dataflow.app.dto.DataInterfaceExecutionRequestDTO;
import com.pemc.crss.dataflow.app.dto.TaskExecutionDto;
import com.pemc.crss.dataflow.app.dto.TaskRunDto;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobSkipLog;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import java.net.URISyntaxException;
import java.util.List;

/**
 * Created on 1/22/17.
 */
public interface DataFlowTaskExecutionService {

    Page<? extends BaseTaskExecutionDto> findJobInstances(Pageable pageable, String type,
                                                          String status, String mode,
                                                          String runStartDate, String runEndDate,
                                                          String tradingStartDate, String tradingEndDate);

    void launchJob(TaskRunDto taskRunDto) throws URISyntaxException;

    /**
     * This method should be revisited once its finalized where to get dispatch interval.
     * Currently being used in sow7. But should be used in all execution services
     * @return
     */
    int getDispatchInterval();

    List<BatchJobSkipLog> getBatchJobSkipLogs(int stepId);

    void deleteJob(long jobId);
}
