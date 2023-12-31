package com.pemc.crss.dataflow.app.service;

import com.pemc.crss.dataflow.app.dto.parent.GroupTaskExecutionDto;
import com.pemc.crss.dataflow.app.dto.parent.StubTaskExecutionDto;
import com.pemc.crss.dataflow.app.dto.TaskRunDto;
import com.pemc.crss.dataflow.app.support.PageableRequest;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobSkipLog;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Created on 1/22/17.
 */
public interface TaskExecutionService {

    Page<? extends StubTaskExecutionDto> findJobInstances(Pageable pageable);

    Page<? extends StubTaskExecutionDto> findJobInstances(Pageable pageable, String type,
                                                          String status, String mode,
                                                          String runStartDate, String tradingStartDate,
                                                          String tradingEndDate, String username);

    Page<? extends StubTaskExecutionDto> findJobInstances(PageableRequest pageableRequest);

    Page<GroupTaskExecutionDto> findDistinctBillingPeriodAndProcessType(Pageable pageable);

    Page<? extends StubTaskExecutionDto> findJobInstancesByBillingPeriodAndProcessType(Pageable pageable, String billingPeriod, String processType, Long adjNo);

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
