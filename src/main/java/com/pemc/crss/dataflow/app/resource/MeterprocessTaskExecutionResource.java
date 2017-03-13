package com.pemc.crss.dataflow.app.resource;

import com.pemc.crss.dataflow.app.dto.BaseTaskExecutionDto;
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
import org.springframework.web.bind.annotation.*;

import java.net.URISyntaxException;
import java.util.List;

@RestController
@RequestMapping("/task-executions/meterprocess")
public class MeterprocessTaskExecutionResource {

    private static final Logger LOG = LoggerFactory.getLogger(MeterprocessTaskExecutionResource.class);

    @Autowired
    @Qualifier("meterprocessTaskExecutionService")
    private TaskExecutionService taskExecutionService;

    @RequestMapping(method = RequestMethod.GET)
    public ResponseEntity<Page<? extends BaseTaskExecutionDto>> findAllJobInstances(Pageable pageable) {
        LOG.debug("Finding job instances request. pageable={}", pageable);
        return new ResponseEntity<>(taskExecutionService.findJobInstances(pageable), HttpStatus.OK);
    }

    @RequestMapping(method = RequestMethod.POST)
    public ResponseEntity runJob(@RequestBody TaskRunDto taskRunDto) throws URISyntaxException {
        LOG.debug("Running job request. taskRunDto={}", taskRunDto);
        taskExecutionService.launchJob(taskRunDto);
        return new ResponseEntity(HttpStatus.OK);
    }

    @RequestMapping(value = "/get-dispatch-interval", method = RequestMethod.GET)
    public int getDispatchInterval() {
        return taskExecutionService.getDispatchInterval();
    }

    @RequestMapping(method = RequestMethod.DELETE)
    public ResponseEntity deleteJob(@RequestParam(value = "jobId") long jobId) throws URISyntaxException {
        taskExecutionService.deleteJob(jobId);
        return new ResponseEntity(HttpStatus.OK);
    }

    @RequestMapping(value = "/get-batch-job-skip-logs", method = RequestMethod.GET)
    public String getBatchJobSkipLogs(@RequestParam(value = "stepId") int stepId) {
        String retVal="";
        for (BatchJobSkipLog log : taskExecutionService.getBatchJobSkipLogs(stepId)) {
            retVal = retVal.concat(log.getError()).concat("\n");
        }
        return retVal;
    }
}