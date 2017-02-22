package com.pemc.crss.dataflow.app.resource;

import com.pemc.crss.dataflow.app.dto.DataInterfaceExecutionDTO;
import com.pemc.crss.dataflow.app.dto.TaskExecutionDto;
import com.pemc.crss.dataflow.app.dto.TaskRunDto;
import com.pemc.crss.dataflow.app.service.TaskExecutionService;
import com.pemc.crss.meterprocess.core.main.entity.BillingPeriod;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.Page;
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
@RequestMapping("/task-executions/stl")
public class StlTaskExecutionResource {

    @Autowired
    @Qualifier("stlTaskExecutionServiceImpl")
    private TaskExecutionService taskExecutionService;

    @RequestMapping(method = RequestMethod.GET)
    public ResponseEntity<Page<TaskExecutionDto>> findAllJobInstances(Pageable pageable) {
        return new ResponseEntity<>(taskExecutionService.findJobInstances(pageable), HttpStatus.OK);
    }

    @RequestMapping(value = "/data-interface-task-executions", method = RequestMethod.GET)
    public ResponseEntity<Page<DataInterfaceExecutionDTO>> findDataInterfaceJobInstances(Pageable pageable) {
        return new ResponseEntity<>(taskExecutionService.findDataInterfaceInstances(pageable), HttpStatus.OK);
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
}