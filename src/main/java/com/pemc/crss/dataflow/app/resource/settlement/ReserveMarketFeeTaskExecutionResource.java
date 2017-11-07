package com.pemc.crss.dataflow.app.resource.settlement;

import com.pemc.crss.dataflow.app.dto.TaskRunDto;
import com.pemc.crss.dataflow.app.dto.parent.StubTaskExecutionDto;
import com.pemc.crss.dataflow.app.jobqueue.BatchJobQueueService;
import com.pemc.crss.dataflow.app.service.TaskExecutionService;
import com.pemc.crss.dataflow.app.support.PageableRequest;
import com.pemc.crss.dataflow.app.util.SecurityUtil;
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
import java.util.Objects;

@Slf4j
@RestController
@RequestMapping("/task-executions/settlement/reserve-market-fee")
public class ReserveMarketFeeTaskExecutionResource {

    private static final Logger LOG = LoggerFactory.getLogger(ReserveMarketFeeTaskExecutionResource.class);

    @Autowired
    @Qualifier("reserveMarketFeeTaskExecutionService")
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
            for (BatchJobQueue runAdjJob : queueService.findQueuedAndInProgressJobs(JobProcess.GEN_INPUT_WS_RMF)) {
                TaskRunDto runAdjTaskDto = ModelMapper.toModel(runAdjJob.getTaskObj(), TaskRunDto.class);
                if (runAdjTaskDto.isNewGroup() && Objects.equals(taskRunDto.getParentJob(), runAdjTaskDto.getParentJob())) {
                    throw new RuntimeException("Cannot queue run adjustment job. Another run adjustment job"
                            + " with the same billing period is already queued");
                }
            }
        }

        taskRunDto.setRunId(System.currentTimeMillis());
        taskRunDto.setJobName(SettlementJobName.GEN_RMF_INPUT_WS);
        taskRunDto.setCurrentUser(SecurityUtil.getCurrentUser(principal));
        log.info("Queueing runGenInputWorkSpaceJob for rmf. taskRunDto={}", taskRunDto);

        BatchJobQueue jobQueue = BatchJobQueueService.newInst(Module.SETTLEMENT, JobProcess.GEN_INPUT_WS_RMF, taskRunDto);
        queueService.save(jobQueue);

        return new ResponseEntity(HttpStatus.OK);
    }

    @PostMapping("/calculate")
    public ResponseEntity runCalculateJob(@RequestBody TaskRunDto taskRunDto, Principal principal) throws URISyntaxException {

        taskRunDto.setRunId(System.currentTimeMillis());
        taskRunDto.setJobName(SettlementJobName.CALC_RMF);
        taskRunDto.setCurrentUser(SecurityUtil.getCurrentUser(principal));
        log.info("Queueing calculateJob for rmf. taskRunDto={}", taskRunDto);

        BatchJobQueue jobQueue = BatchJobQueueService.newInst(Module.SETTLEMENT, JobProcess.CALC_RMF, taskRunDto);
        queueService.save(jobQueue);

        return new ResponseEntity(HttpStatus.OK);
    }

    @PostMapping("/finalize")
    public ResponseEntity runFinalizeJob(@RequestBody TaskRunDto taskRunDto, Principal principal) throws URISyntaxException {

        taskRunDto.setRunId(System.currentTimeMillis());
        taskRunDto.setJobName(SettlementJobName.TAG_RMF);
        taskRunDto.setCurrentUser(SecurityUtil.getCurrentUser(principal));
        log.info("Queueing finalize job for rmf. taskRunDto={}", taskRunDto);

        BatchJobQueue jobQueue = BatchJobQueueService.newInst(Module.SETTLEMENT, JobProcess.FINALIZE_RMF, taskRunDto);
        queueService.save(jobQueue);

        return new ResponseEntity(HttpStatus.OK);
    }

    @PostMapping("/generate-file")
    public ResponseEntity runGenerateFileJob(@RequestBody TaskRunDto taskRunDto, Principal principal) throws URISyntaxException {

        taskRunDto.setRunId(System.currentTimeMillis());
        taskRunDto.setJobName(SettlementJobName.FILE_RMF);
        taskRunDto.setCurrentUser(SecurityUtil.getCurrentUser(principal));
        log.info("Queueing generate file job for rmf. taskRunDto={}", taskRunDto);

        BatchJobQueue jobQueue = BatchJobQueueService.newInst(Module.SETTLEMENT, JobProcess.GEN_FILES_RMF, taskRunDto);
        queueService.save(jobQueue);

        return new ResponseEntity(HttpStatus.OK);
    }


    @PostMapping(value = "/get-batch-job-skip-logs")
    public ResponseEntity<Page<BatchJobSkipLog>> getBatchJobSkipLogs(@RequestBody PageableRequest pageableRequest) {
        LOG.debug("Finding skip logs request. pageable={}", pageableRequest.getPageable());
        return new ResponseEntity<>(taskExecutionService.getBatchJobSkipLogs(pageableRequest), HttpStatus.OK);
    }

}
