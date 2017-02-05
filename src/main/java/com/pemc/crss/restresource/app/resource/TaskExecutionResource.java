package com.pemc.crss.restresource.app.resource;

import com.pemc.crss.meterprocess.core.main.entity.BillingPeriod;
import com.pemc.crss.restresource.app.dto.DataInterfaceExecutionDTO;
import com.pemc.crss.restresource.app.dto.TaskExecutionDto;
import com.pemc.crss.restresource.app.dto.TaskRunDto;
import com.pemc.crss.restresource.app.service.TaskExecutionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.net.URISyntaxException;
import java.util.List;

@RestController
@RequestMapping("/task-executions")
public class TaskExecutionResource {

    @Autowired
    private TaskExecutionService taskExecutionService;

    @RequestMapping(method = RequestMethod.GET)
    public ResponseEntity<Page<TaskExecutionDto>> findAllJobInstances(Pageable pageable) {
        List<TaskExecutionDto> taskExecutionDtos = taskExecutionService.findJobInstances(pageable);
        return new ResponseEntity<>(new PageImpl<>(taskExecutionDtos, pageable, taskExecutionDtos.size()), HttpStatus.OK);
    }

    @RequestMapping(value = "/stl", method = RequestMethod.GET)
    public ResponseEntity<Page<TaskExecutionDto>> findAllStlJobInstances(Pageable pageable) {
        List<TaskExecutionDto> taskExecutionDtos = taskExecutionService.findSettlementJobInstances(pageable);
        return new ResponseEntity<>(new PageImpl<>(taskExecutionDtos, pageable, taskExecutionDtos.size()), HttpStatus.OK);
    }

    @RequestMapping(method = RequestMethod.POST)
    public ResponseEntity runJob(@RequestBody TaskRunDto taskRunDto) throws URISyntaxException {
        taskExecutionService.launchJob(taskRunDto);
        return new ResponseEntity(HttpStatus.OK);
    }


    @RequestMapping(value = "/billing-period", method = RequestMethod.GET)
    public ResponseEntity<List<BillingPeriod>> findBillingPeriods() {
        List<BillingPeriod> billingPeriods = taskExecutionService.findBillingPeriods();
        return new ResponseEntity<>(billingPeriods, HttpStatus.OK);
    }

    @RequestMapping(value = "/data-interface-task-executions", method = RequestMethod.GET)
    public ResponseEntity<Page<DataInterfaceExecutionDTO>> findDataInterfaceJobInstances(Pageable pageable) {
        List<DataInterfaceExecutionDTO> taskExecutionDtos = taskExecutionService.findDataInterfaceInstances(pageable);
        return new ResponseEntity<>(new PageImpl<>(taskExecutionDtos, pageable, taskExecutionDtos.size()), HttpStatus.OK);
    }
}
