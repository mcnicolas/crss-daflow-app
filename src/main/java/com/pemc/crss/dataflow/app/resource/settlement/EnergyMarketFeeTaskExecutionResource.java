package com.pemc.crss.dataflow.app.resource.settlement;

import com.pemc.crss.dataflow.app.dto.TaskRunDto;
import com.pemc.crss.dataflow.app.dto.parent.StubTaskExecutionDto;
import com.pemc.crss.dataflow.app.jobqueue.BatchJobQueueService;
import com.pemc.crss.dataflow.app.service.TaskExecutionService;
import com.pemc.crss.dataflow.app.support.PageableRequest;
import com.pemc.crss.dataflow.app.util.SecurityUtil;
import com.pemc.crss.shared.commons.reference.MeterProcessType;
import com.pemc.crss.shared.commons.util.DateUtil;
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
@RequestMapping("/task-executions/settlement/energy-market-fee")
public class EnergyMarketFeeTaskExecutionResource {

    private static final Logger LOG = LoggerFactory.getLogger(EnergyMarketFeeTaskExecutionResource.class);

    @Autowired
    @Qualifier("energyMarketFeeTaskExecutionService")
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
            for (BatchJobQueue runAdjJob : queueService.findQueuedAndInProgressJobs(JobProcess.GEN_INPUT_WS_EMF)) {
                TaskRunDto runAdjTaskDto = ModelMapper.toModel(runAdjJob.getTaskObj(), TaskRunDto.class);
                if (runAdjTaskDto.isNewGroup() && Objects.equals(taskRunDto.getParentJob(), runAdjTaskDto.getParentJob())) {
                    throw new RuntimeException("Cannot queue run adjustment job. Another run adjustment job"
                            + " with the same billing period is already queued");
                }
            }
        }

        validateAdjustedRun(taskRunDto);

        List<String> dateRangeStr = DateUtil.createRangeString(taskRunDto.getStartDate(), taskRunDto.getEndDate(), null);

        dateRangeStr.forEach(dateStr -> {
            TaskRunDto runDto = TaskRunDto.clone(taskRunDto);
            runDto.setStartDate(dateStr);
            runDto.setEndDate(dateStr);
            runDto.setRunId(System.currentTimeMillis());
            runDto.setJobName(SettlementJobName.GEN_EMF_INPUT_WS);
            runDto.setCurrentUser(SecurityUtil.getCurrentUser(principal));

            log.info("Queueing runGenInputWorkSpaceJob for emf. taskRunDto={}", taskRunDto);

            BatchJobQueue jobQueue = BatchJobQueueService.newInst(Module.SETTLEMENT, JobProcess.GEN_INPUT_WS_EMF, taskRunDto);
            queueService.save(jobQueue);
        });

        return new ResponseEntity(HttpStatus.OK);
    }

    @PostMapping("/calculate")
    public ResponseEntity runCalculateJob(@RequestBody TaskRunDto taskRunDto, Principal principal) throws URISyntaxException {

        validateAdjustedRun(taskRunDto);

        List<String> dateRangeStr = DateUtil.createRangeString(taskRunDto.getStartDate(), taskRunDto.getEndDate(), null);

        dateRangeStr.forEach(dateStr -> {
            TaskRunDto runDto = TaskRunDto.clone(taskRunDto);
            runDto.setStartDate(dateStr);
            runDto.setEndDate(dateStr);
            runDto.setRunId(System.currentTimeMillis());
            runDto.setJobName(SettlementJobName.CALC_EMF);
            runDto.setCurrentUser(SecurityUtil.getCurrentUser(principal));

            log.info("Queueing calculateJob for emf. taskRunDto={}", taskRunDto);

            BatchJobQueue jobQueue = BatchJobQueueService.newInst(Module.SETTLEMENT, JobProcess.CALC_EMF, taskRunDto);
            queueService.save(jobQueue);
        });

        return new ResponseEntity(HttpStatus.OK);
    }

    @PostMapping("/finalize")
    public ResponseEntity runFinalizeJob(@RequestBody TaskRunDto taskRunDto, Principal principal) throws URISyntaxException {

        taskRunDto.setRunId(System.currentTimeMillis());
        taskRunDto.setJobName(SettlementJobName.TAG_EMF);
        taskRunDto.setCurrentUser(SecurityUtil.getCurrentUser(principal));

        validateAdjustedRun(taskRunDto);

        log.info("Queueing finalize job for emf. taskRunDto={}", taskRunDto);

        BatchJobQueue jobQueue = BatchJobQueueService.newInst(Module.SETTLEMENT, JobProcess.FINALIZE_EMF, taskRunDto);
        queueService.save(jobQueue);

        return new ResponseEntity(HttpStatus.OK);
    }

    private void validateAdjustedRun(final TaskRunDto taskRunDto) {
        if (Objects.equals(taskRunDto.getMeterProcessType(), MeterProcessType.ADJUSTED.name()) || taskRunDto.isNewGroup()) {
            queueService.validateAdjustedProcess(taskRunDto, JobProcess.FINALIZE_EMF);
        }
    }

    @PostMapping("/generate-file")
    public ResponseEntity runGenerateFileJob(@RequestBody TaskRunDto taskRunDto, Principal principal) throws URISyntaxException {

        taskRunDto.setRunId(System.currentTimeMillis());
        taskRunDto.setJobName(SettlementJobName.FILE_EMF);
        taskRunDto.setCurrentUser(SecurityUtil.getCurrentUser(principal));
        log.info("Queueing generate file job for emf. taskRunDto={}", taskRunDto);

        BatchJobQueue jobQueue = BatchJobQueueService.newInst(Module.SETTLEMENT, JobProcess.GEN_FILES_EMF, taskRunDto);
        queueService.save(jobQueue);

        return new ResponseEntity(HttpStatus.OK);
    }

    @PostMapping(value = "/get-batch-job-skip-logs")
    public ResponseEntity<Page<BatchJobSkipLog>> getBatchJobSkipLogs(@RequestBody PageableRequest pageableRequest) {
        LOG.debug("Finding skip logs request. pageable={}", pageableRequest.getPageable());
        return new ResponseEntity<>(taskExecutionService.getBatchJobSkipLogs(pageableRequest), HttpStatus.OK);
    }

}
