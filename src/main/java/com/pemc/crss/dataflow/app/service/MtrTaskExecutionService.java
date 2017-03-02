package com.pemc.crss.dataflow.app.service;

import com.pemc.crss.dataflow.app.dto.MtrTaskExecutionDto;
import com.pemc.crss.dataflow.app.dto.TaskRunDto;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import java.net.URISyntaxException;

public interface MtrTaskExecutionService {

    Page<MtrTaskExecutionDto> findJobInstances(Pageable pageable);

    void launchJob(TaskRunDto taskRunDto) throws URISyntaxException;

}
