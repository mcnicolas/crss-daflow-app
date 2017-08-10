package com.pemc.crss.dataflow.app.resource.settlement;

import com.pemc.crss.dataflow.app.dto.TaskRunDto;
import com.pemc.crss.dataflow.app.dto.parent.StubTaskExecutionDto;
import com.pemc.crss.dataflow.app.service.TaskExecutionService;
import com.pemc.crss.dataflow.app.support.PageableRequest;
import com.pemc.crss.dataflow.app.util.SecurityUtil;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobSkipLog;
import com.pemc.crss.shared.core.dataflow.reference.SettlementJobName;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.Page;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URISyntaxException;
import java.security.Principal;

@Slf4j
@RestController
@RequestMapping("/task-executions/settlement/energy-market-fee")
public class EnergyMarketFeeTaskExecutionResource {

    private static final Logger LOG = LoggerFactory.getLogger(EnergyMarketFeeTaskExecutionResource.class);

    @Autowired
    @Qualifier("energyMarketFeeTaskExecutionService")
    private TaskExecutionService taskExecutionService;

    @PostMapping("/job-instances")
    public ResponseEntity<Page<? extends StubTaskExecutionDto>> findAllJobInstances(@RequestBody PageableRequest pageableRequest) {

        return new ResponseEntity<>(taskExecutionService.findJobInstances(pageableRequest), HttpStatus.OK);
    }

    @PostMapping("/generate-input-workspace")
    public ResponseEntity runGenInputWorkSpaceJob(@RequestBody TaskRunDto taskRunDto, Principal principal) throws URISyntaxException {
        log.info("Running runGenInputWorkSpaceJob. taskRunDto={}", taskRunDto);

        taskRunDto.setJobName(SettlementJobName.GEN_EMF_INPUT_WS);
        taskRunDto.setCurrentUser(SecurityUtil.getCurrentUser(principal));
        taskExecutionService.launchJob(taskRunDto);

        return new ResponseEntity(HttpStatus.OK);
    }

    @PostMapping("/calculate-stl-amount")
    public ResponseEntity runCalculateJob(@RequestBody TaskRunDto taskRunDto, Principal principal) throws URISyntaxException {
        log.info("Running calculateJob. taskRunDto={}", taskRunDto);

//        TODO: add jobName
//        taskRunDto.setJobName(SettlementJobName.GEN_EMF_INPUT_WS);
        taskRunDto.setCurrentUser(SecurityUtil.getCurrentUser(principal));
        taskExecutionService.launchJob(taskRunDto);

        return new ResponseEntity(HttpStatus.OK);
    }

    @PostMapping(value = "/get-batch-job-skip-logs")
    public ResponseEntity<Page<BatchJobSkipLog>> getBatchJobSkipLogs(@RequestBody PageableRequest pageableRequest) {
        LOG.debug("Finding skip logs request. pageable={}", pageableRequest.getPageable());
        return new ResponseEntity<>(taskExecutionService.getBatchJobSkipLogs(pageableRequest), HttpStatus.OK);
    }

}
