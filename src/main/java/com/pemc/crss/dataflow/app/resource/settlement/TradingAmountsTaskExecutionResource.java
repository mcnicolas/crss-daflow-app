package com.pemc.crss.dataflow.app.resource.settlement;

import com.pemc.crss.dataflow.app.dto.TaskRunDto;
import com.pemc.crss.dataflow.app.dto.parent.StubTaskExecutionDto;
import com.pemc.crss.dataflow.app.jobqueue.BatchJobQueueService;
import com.pemc.crss.dataflow.app.service.TaskExecutionService;
import com.pemc.crss.dataflow.app.support.PageableRequest;
import com.pemc.crss.dataflow.app.util.SecurityUtil;
import com.pemc.crss.shared.commons.reference.MeterProcessType;
import com.pemc.crss.shared.commons.util.ModelMapper;
import com.pemc.crss.shared.commons.util.reference.Module;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobQueue;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobSkipLog;
import com.pemc.crss.shared.core.dataflow.reference.JobProcess;
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
import java.util.List;
import java.util.Objects;

@Slf4j
@RestController
@RequestMapping("/task-executions/settlement/trading-amounts")
public class TradingAmountsTaskExecutionResource {

    private static final Logger LOG = LoggerFactory.getLogger(TradingAmountsTaskExecutionResource.class);

    @Autowired
    @Qualifier("tradingAmountsTaskExecutionService")
    private TaskExecutionService taskExecutionService;

    @Autowired
    private BatchJobQueueService queueService;

    @PostMapping("/job-instances")
    public ResponseEntity<Page<? extends StubTaskExecutionDto>> findAllJobInstances(@RequestBody PageableRequest pageableRequest) {

        return new ResponseEntity<>(taskExecutionService.findJobInstances(pageableRequest), HttpStatus.OK);
    }

    @PostMapping("/generate-input-workspace")
    public ResponseEntity runGenInputWorkSpaceJob(@RequestBody TaskRunDto taskRunDto, Principal principal) throws URISyntaxException {

        if (taskRunDto.isNewGroup()) {
            for (BatchJobQueue runAdjJob : queueService.findQueuedAndInProgressJobs(JobProcess.GEN_INPUT_WS_TA)) {
                TaskRunDto runAdjTaskDto = ModelMapper.toModel(runAdjJob.getTaskObj(), TaskRunDto.class);
                if (runAdjTaskDto.isNewGroup() && Objects.equals(taskRunDto.getParentJob(), runAdjTaskDto.getParentJob())) {
                    throw new RuntimeException("Cannot queue run adjustment job. Another run adjustment job"
                            + " with the same billing period is already queued");
                }
            }
        }

        taskRunDto.setRunId(System.currentTimeMillis());
        taskRunDto.setJobName(SettlementJobName.GEN_EBRSV_INPUT_WS);
        taskRunDto.setCurrentUser(SecurityUtil.getCurrentUser(principal));

        validateAdjustedRun(taskRunDto);

        log.info("Queueing runGenInputWorkSpaceJob for trading amounts. taskRunDto={}", taskRunDto);

        BatchJobQueue jobQueue = BatchJobQueueService.newInst(Module.SETTLEMENT, JobProcess.GEN_INPUT_WS_TA, taskRunDto);
        queueService.save(jobQueue);

        return new ResponseEntity(HttpStatus.OK);
    }

    @PostMapping("/calculate")
    public ResponseEntity runCalculateJob(@RequestBody TaskRunDto taskRunDto, Principal principal) throws URISyntaxException {

        taskRunDto.setRunId(System.currentTimeMillis());
        taskRunDto.setJobName(SettlementJobName.CALC_STL);
        taskRunDto.setCurrentUser(SecurityUtil.getCurrentUser(principal));

        validateAdjustedRun(taskRunDto);

        log.info("Queueing calculateJob for trading amounts. taskRunDto={}", taskRunDto);

        BatchJobQueue jobQueue = BatchJobQueueService.newInst(Module.SETTLEMENT, JobProcess.CALC_TA, taskRunDto);
        queueService.save(jobQueue);

        return new ResponseEntity(HttpStatus.OK);
    }

    @PostMapping("/calculate-lr")
    public ResponseEntity runCalculateLineRentalJob(@RequestBody TaskRunDto taskRunDto, Principal principal) throws URISyntaxException {

        taskRunDto.setRunId(System.currentTimeMillis());
        taskRunDto.setJobName(SettlementJobName.CALC_LR);
        taskRunDto.setCurrentUser(SecurityUtil.getCurrentUser(principal));

        validateAdjustedRun(taskRunDto);

        log.info("Queueing calculateJob for line rental. taskRunDto={}", taskRunDto);

        BatchJobQueue jobQueue = BatchJobQueueService.newInst(Module.SETTLEMENT, JobProcess.CALC_LR, taskRunDto);
        queueService.save(jobQueue);

        return new ResponseEntity(HttpStatus.OK);
    }

    @PostMapping("/calculate-gmr")
    public ResponseEntity runCalculateGmrJob(@RequestBody TaskRunDto taskRunDto, Principal principal) throws URISyntaxException {

        taskRunDto.setRunId(System.currentTimeMillis());
        taskRunDto.setJobName(SettlementJobName.CALC_GMR);
        taskRunDto.setCurrentUser(SecurityUtil.getCurrentUser(principal));
        log.info("Queueing calculateGmrJob for trading amounts. taskRunDto={}", taskRunDto);

        validateAdjustedRun(taskRunDto);

        BatchJobQueue jobQueue = BatchJobQueueService.newInst(Module.SETTLEMENT, JobProcess.CALC_GMR_VAT, taskRunDto);
        queueService.save(jobQueue);

        return new ResponseEntity(HttpStatus.OK);
    }

    @PostMapping("/finalize")
    public ResponseEntity runFinalizeJob(@RequestBody TaskRunDto taskRunDto, Principal principal) throws URISyntaxException {

        taskRunDto.setRunId(System.currentTimeMillis());
        taskRunDto.setJobName(SettlementJobName.TAG_TA);
        taskRunDto.setCurrentUser(SecurityUtil.getCurrentUser(principal));

        validateAdjustedRun(taskRunDto);

        log.info("Queueing finalize job for trading amounts. taskRunDto={}", taskRunDto);

        BatchJobQueue jobQueue = BatchJobQueueService.newInst(Module.SETTLEMENT, JobProcess.FINALIZE_TA, taskRunDto);
        queueService.save(jobQueue);

        return new ResponseEntity(HttpStatus.OK);
    }

    @PostMapping("/finalize-lr")
    public ResponseEntity runFinalizeLineRentalJob(@RequestBody TaskRunDto taskRunDto, Principal principal) throws URISyntaxException {

        taskRunDto.setRunId(System.currentTimeMillis());
        taskRunDto.setJobName(SettlementJobName.TAG_LR);
        taskRunDto.setCurrentUser(SecurityUtil.getCurrentUser(principal));

        validateAdjustedRun(taskRunDto);

        log.info("Queueing finalize job for line rental. taskRunDto={}", taskRunDto);

        BatchJobQueue jobQueue = BatchJobQueueService.newInst(Module.SETTLEMENT, JobProcess.FINALIZE_LR, taskRunDto);
        queueService.save(jobQueue);

        return new ResponseEntity(HttpStatus.OK);
    }

    private void validateAdjustedRun(final TaskRunDto taskRunDto) {
        if (Objects.equals(taskRunDto.getMeterProcessType(), MeterProcessType.ADJUSTED.name()) || taskRunDto.isNewGroup()) {
            queueService.validateAdjustedProcess(taskRunDto, JobProcess.FINALIZE_TA);
        }
    }

    @PostMapping("/generate-file-energy")
    public ResponseEntity runGenerateFileJobEnergy(@RequestBody TaskRunDto taskRunDto, Principal principal) throws URISyntaxException {

        taskRunDto.setRunId(System.currentTimeMillis());
        taskRunDto.setJobName(SettlementJobName.FILE_TA);
        taskRunDto.setCurrentUser(SecurityUtil.getCurrentUser(principal));
        log.info("Queueing generate file job for energy trading amounts. taskRunDto={}", taskRunDto);

        BatchJobQueue jobQueue = BatchJobQueueService.newInst(Module.SETTLEMENT, JobProcess.GEN_ENERGY_FILES, taskRunDto);
        queueService.save(jobQueue);

        return new ResponseEntity(HttpStatus.OK);
    }

    @PostMapping("/generate-file-reserve")
    public ResponseEntity runGenerateFileJobReserve(@RequestBody TaskRunDto taskRunDto, Principal principal) throws URISyntaxException {

        taskRunDto.setRunId(System.currentTimeMillis());
        taskRunDto.setJobName(SettlementJobName.FILE_RSV_TA);
        taskRunDto.setCurrentUser(SecurityUtil.getCurrentUser(principal));
        log.info("Queueing generate file job for reserve trading amounts. taskRunDto={}", taskRunDto);

        BatchJobQueue jobQueue = BatchJobQueueService.newInst(Module.SETTLEMENT, JobProcess.GEN_RESERVE_FILES, taskRunDto);
        queueService.save(jobQueue);

        return new ResponseEntity(HttpStatus.OK);
    }

    @PostMapping("/generate-file-line-rental")
    public ResponseEntity runGenerateFileJobLineRental(@RequestBody TaskRunDto taskRunDto, Principal principal) throws URISyntaxException {

        taskRunDto.setRunId(System.currentTimeMillis());
        taskRunDto.setJobName(SettlementJobName.FILE_LR);
        taskRunDto.setCurrentUser(SecurityUtil.getCurrentUser(principal));

        log.info("Queueing generate file job for line rental. taskRunDto={}", taskRunDto);

        BatchJobQueue jobQueue = BatchJobQueueService.newInst(Module.SETTLEMENT, JobProcess.GEN_LR_FILES, taskRunDto);
        queueService.save(jobQueue);

        return new ResponseEntity(HttpStatus.OK);
    }

    @PostMapping("/stl-validation")
    public ResponseEntity runStlValidationJob(@RequestBody TaskRunDto taskRunDto, Principal principal) throws URISyntaxException {
        log.info("Queueing stl validation job for trading amounts. taskRunDto={}", taskRunDto);

        taskRunDto.setRunId(System.currentTimeMillis());
        taskRunDto.setJobName(SettlementJobName.STL_VALIDATION);
        taskRunDto.setCurrentUser(SecurityUtil.getCurrentUser(principal));

        log.info("Queueing stl validation job for trading amounts. taskRunDto={}", taskRunDto);

        BatchJobQueue jobQueue = BatchJobQueueService.newInst(Module.SETTLEMENT, JobProcess.STL_VALIDATION, taskRunDto);
        queueService.save(jobQueue);

        return new ResponseEntity(HttpStatus.OK);
    }

    @PostMapping(value = "/get-batch-job-skip-logs")
    public ResponseEntity<Page<BatchJobSkipLog>> getBatchJobSkipLogs(@RequestBody PageableRequest pageableRequest) {
        LOG.debug("Finding skip logs request. pageable={}", pageableRequest.getPageable());
        return new ResponseEntity<>(taskExecutionService.getBatchJobSkipLogs(pageableRequest), HttpStatus.OK);
    }

}
