package com.pemc.crss.restresource.app.service;

import com.pemc.crss.meterprocess.core.main.entity.BillingPeriod;
import com.pemc.crss.restresource.app.dto.DataInterfaceExecutionDTO;
import com.pemc.crss.restresource.app.dto.TaskExecutionDto;
import com.pemc.crss.restresource.app.dto.TaskRunDto;
import org.springframework.data.domain.Pageable;

import java.net.URISyntaxException;
import java.util.List;

/**
 * Created on 1/22/17.
 */
public interface TaskExecutionService {

    List<TaskExecutionDto> findJobInstances(Pageable pageable);

    List<TaskExecutionDto> findSettlementJobInstances(Pageable pageable);

    List<DataInterfaceExecutionDTO> findDataInterfaceInstances(Pageable pageable);

    List<BillingPeriod> findBillingPeriods();

    void launchJob(TaskRunDto taskRunDto) throws URISyntaxException;

}
