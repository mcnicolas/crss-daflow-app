package com.pemc.crss.dataflow.app.service;

import com.pemc.crss.dataflow.app.dto.DataInterfaceExecutionDTO;
import com.pemc.crss.dataflow.app.dto.TaskExecutionDto;
import com.pemc.crss.dataflow.app.dto.TaskRunDto;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import java.net.URISyntaxException;

/**
 * Created on 1/22/17.
 */
public interface TaskExecutionService {

    Page<TaskExecutionDto> findJobInstances(Pageable pageable);

    Page<DataInterfaceExecutionDTO> findDataInterfaceInstances(Pageable pageable);

    void launchJob(TaskRunDto taskRunDto) throws URISyntaxException;

    int getDispatchInterval();

    void deleteJob(long jobId);
}
