package com.pemc.crss.dataflow.app.service;

import com.pemc.crss.dataflow.app.dto.BaseTaskExecutionDto;
import com.pemc.crss.dataflow.app.dto.TaskRunDto;
import com.pemc.crss.dataflow.app.support.PageableRequest;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobSkipLog;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import java.net.URISyntaxException;

/**
 * Created on 1/22/17.
 */
public interface TaskExecutionService {

    Page<? extends BaseTaskExecutionDto> findJobInstances(Pageable pageable);

    Page<? extends BaseTaskExecutionDto> findJobInstances(Pageable pageable, String type,
                                                          String status, String mode,
                                                          String runStartDate, String tradingStartDate,
                                                          String tradingEndDate, String username);

    Page<? extends BaseTaskExecutionDto> findJobInstances(PageableRequest pageableRequest);

    void launchJob(TaskRunDto taskRunDto) throws URISyntaxException;

    /**
     * This method should be revisited once its finalized where to get dispatch interval.
     * Currently being used in sow7. But should be used in all execution services
     * @return
     */
    int getDispatchInterval();

    Page<BatchJobSkipLog> getBatchJobSkipLogs(Pageable pageable, int stepId);

    Page<BatchJobSkipLog> getBatchJobSkipLogs(PageableRequest pageableRequest);

    void deleteJob(long jobId);

    void relaunchFailedJob(long jobId) throws URISyntaxException;

    String getFailedExitMsg(int
                                    stepId);
}
