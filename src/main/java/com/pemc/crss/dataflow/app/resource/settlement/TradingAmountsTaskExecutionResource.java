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
@RequestMapping("/task-executions/settlement/trading-amounts")
public class TradingAmountsTaskExecutionResource {

    private static final Logger LOG = LoggerFactory.getLogger(TradingAmountsTaskExecutionResource.class);

    @Autowired
    @Qualifier("tradingAmountsTaskExecutionService")
    private TaskExecutionService taskExecutionService;

    @PostMapping("/job-instances")
    public ResponseEntity<Page<? extends StubTaskExecutionDto>> findAllJobInstances(@RequestBody PageableRequest pageableRequest) {

        return new ResponseEntity<>(taskExecutionService.findJobInstances(pageableRequest), HttpStatus.OK);
    }

    @PostMapping("/generate-input-workspace")
    public ResponseEntity runGenInputWorkSpaceJob(@RequestBody TaskRunDto taskRunDto, Principal principal) throws URISyntaxException {
        log.info("Running runGenInputWorkSpaceJob for tta. taskRunDto={}", taskRunDto);
        taskRunDto.setJobName(SettlementJobName.GEN_EBRSV_INPUT_WS);
        taskRunDto.setCurrentUser(SecurityUtil.getCurrentUser(principal));
        taskExecutionService.launchJob(taskRunDto);

        return new ResponseEntity(HttpStatus.OK);
    }

    @PostMapping("/calculate")
    public ResponseEntity runCalculateJob(@RequestBody TaskRunDto taskRunDto, Principal principal) throws URISyntaxException {
        log.info("Running calculateJob for tta. taskRunDto={}", taskRunDto);

        taskRunDto.setJobName(SettlementJobName.CALC_STL);
        taskRunDto.setCurrentUser(SecurityUtil.getCurrentUser(principal));
        taskExecutionService.launchJob(taskRunDto);

        return new ResponseEntity(HttpStatus.OK);
    }

    @PostMapping("/calculate-gmr")
    public ResponseEntity runCalculateGmrJob(@RequestBody TaskRunDto taskRunDto, Principal principal) throws URISyntaxException {
        log.info("Running calculateGmrJob for tta. taskRunDto={}", taskRunDto);

        taskRunDto.setJobName(SettlementJobName.CALC_GMR);
        taskRunDto.setCurrentUser(SecurityUtil.getCurrentUser(principal));
        taskExecutionService.launchJob(taskRunDto);

        return new ResponseEntity(HttpStatus.OK);
    }

    @PostMapping("/finalize")
    public ResponseEntity runFinalizeJob(@RequestBody TaskRunDto taskRunDto, Principal principal) throws URISyntaxException {
        log.info("Running finalize job for tta. taskRunDto={}", taskRunDto);

        taskRunDto.setJobName(SettlementJobName.TAG_TA);
        taskRunDto.setCurrentUser(SecurityUtil.getCurrentUser(principal));
        taskExecutionService.launchJob(taskRunDto);

        return new ResponseEntity(HttpStatus.OK);
    }

    @PostMapping("/generate-file")
    public ResponseEntity runGenerateFileJob(@RequestBody TaskRunDto taskRunDto, Principal principal) throws URISyntaxException {
        log.info("Running generate file job for tta. taskRunDto={}", taskRunDto);

        taskRunDto.setJobName(SettlementJobName.FILE_TA);
        taskRunDto.setCurrentUser(SecurityUtil.getCurrentUser(principal));
        taskExecutionService.launchJob(taskRunDto);

        return new ResponseEntity(HttpStatus.OK);
    }

    @PostMapping(value = "/get-batch-job-skip-logs")
    public ResponseEntity<Page<BatchJobSkipLog>> getBatchJobSkipLogs(@RequestBody PageableRequest pageableRequest) {
        LOG.debug("Finding skip logs request. pageable={}", pageableRequest.getPageable());
        return new ResponseEntity<>(taskExecutionService.getBatchJobSkipLogs(pageableRequest), HttpStatus.OK);
    }

    @PostMapping("/stl-validation")
    public ResponseEntity runStlValidationJob(@RequestBody TaskRunDto taskRunDto, Principal principal) throws URISyntaxException {
        log.info("Running stl validation job for trading amounts. taskRunDto={}", taskRunDto);

        taskRunDto.setJobName(SettlementJobName.STL_VALIDATION);
        taskRunDto.setCurrentUser(SecurityUtil.getCurrentUser(principal));
        taskExecutionService.launchJob(taskRunDto);

        return new ResponseEntity(HttpStatus.OK);
    }

}
