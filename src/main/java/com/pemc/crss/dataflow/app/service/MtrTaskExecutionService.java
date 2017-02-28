package com.pemc.crss.dataflow.app.service;

import com.pemc.crss.dataflow.app.dto.TaskExecutionDto;
import com.pemc.crss.dataflow.app.dto.TaskRunDto;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import java.net.URISyntaxException;

public interface MtrTaskExecutionService {

    Page<TaskExecutionDto> findJobInstances(Pageable pageable);

    void launchJob(TaskRunDto taskRunDto) throws URISyntaxException;

}
