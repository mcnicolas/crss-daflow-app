package com.pemc.crss.dataflow.app.resource;

import com.pemc.crss.dataflow.app.dto.BaseTaskExecutionDto;
import com.pemc.crss.dataflow.app.support.PageableRequest;
import com.pemc.crss.dataflow.app.dto.TaskRunDto;
import com.pemc.crss.dataflow.app.service.TaskExecutionService;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobSkipLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.net.URISyntaxException;

@RestController
@RequestMapping("/task-executions/settlement")
public class SettlementTaskExecutionResource {

    private static final Logger LOG = LoggerFactory.getLogger(SettlementTaskExecutionResource.class);

    @Autowired
    @Qualifier("settlementTaskExecutionService")
    private TaskExecutionService taskExecutionService;

    @PostMapping("/job-instances")
    public ResponseEntity<Page<? extends BaseTaskExecutionDto>> findAllJobInstances(@RequestBody PageableRequest pageableRequest) {
        LOG.debug("Finding job instances request. pageable={}", pageableRequest.getPageable());
        return new ResponseEntity<>(taskExecutionService.findJobInstances(pageableRequest), HttpStatus.OK);
    }

    @RequestMapping(method = RequestMethod.POST)
    public ResponseEntity runJob(@RequestBody TaskRunDto taskRunDto) throws URISyntaxException {
        LOG.debug("Running job request. taskRunDto={}", taskRunDto);
        taskExecutionService.launchJob(taskRunDto);
        return new ResponseEntity(HttpStatus.OK);
    }

    @RequestMapping(value = "/get-batch-job-skip-logs", method = RequestMethod.GET)
    public ResponseEntity<Page<BatchJobSkipLog>> getBatchJobSkipLogs(Pageable pageable, int stepId) {
        LOG.debug("Finding skip logs request. pageable={}", pageable);
        return new ResponseEntity<>(taskExecutionService.getBatchJobSkipLogs(pageable, stepId), HttpStatus.OK);
    }

}
