package com.pemc.crss.dataflow.app.resource.settlement;

import com.pemc.crss.dataflow.app.dto.TaskRunDto;
import com.pemc.crss.dataflow.app.dto.parent.StubTaskExecutionDto;
import com.pemc.crss.dataflow.app.jobqueue.BatchJobQueueService;
import com.pemc.crss.dataflow.app.service.DispatchIntervalService;
import com.pemc.crss.dataflow.app.service.TaskExecutionService;
import com.pemc.crss.dataflow.app.support.PageableRequest;
import com.pemc.crss.dataflow.app.util.SecurityUtil;
import com.pemc.crss.shared.commons.reference.MeterProcessType;
import com.pemc.crss.shared.commons.util.DateUtil;
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

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.Principal;
import java.util.List;
import java.util.Objects;

@Slf4j
@RestController
@RequestMapping("/task-executions/settlement/reserve-trading-amounts")
public class ReserveTradingAmountsTaskExecutionResource {

    private static final Logger LOG = LoggerFactory.getLogger(ReserveTradingAmountsTaskExecutionResource.class);

    @Autowired
    @Qualifier("reserveTradingAmountsTaskExecutionService")
    private TaskExecutionService taskExecutionService;

    @Autowired
    @Qualifier("tradingAmountsDispatchIntervalService")
    private DispatchIntervalService dispatchIntervalService;

    @Autowired
    private BatchJobQueueService queueService;

    @PostMapping("/job-instances")
    public ResponseEntity<Page<? extends StubTaskExecutionDto>> findAllJobInstances(@RequestBody PageableRequest pageableRequest) {

        return new ResponseEntity<>(taskExecutionService.findJobInstances(pageableRequest), HttpStatus.OK);
    }

    @PostMapping("/calculate-reserve")
    public ResponseEntity runCalculateJob(@RequestBody TaskRunDto taskRunDto, Principal principal) throws URISyntaxException {

        validateAdjustedRun(taskRunDto);

        List<String> dateRangeStr;

        switch (MeterProcessType.valueOf(taskRunDto.getMeterProcessType())) {
            case DAILY:
                dateRangeStr = DateUtil.createRangeString(taskRunDto.getTradingDate(), taskRunDto.getTradingDate(),
                        null);
                break;
            default:
                dateRangeStr = DateUtil.createRangeString(taskRunDto.getStartDate(), taskRunDto.getEndDate(), null);
        }

        dateRangeStr.forEach(dateStr -> {
            TaskRunDto runDto = TaskRunDto.clone(taskRunDto);
            runDto.setStartDate(dateStr);
            runDto.setEndDate(dateStr);
            runDto.setRunId(System.currentTimeMillis());
            runDto.setJobName(SettlementJobName.CALC_RSV_STL);
            runDto.setCurrentUser(SecurityUtil.getCurrentUser(principal));

            log.info("Queueing calculateJob for reserve trading amounts. taskRunDto={}", runDto);

            BatchJobQueue jobQueue = BatchJobQueueService.newInst(Module.SETTLEMENT, JobProcess.CALC_RTA, runDto);
            jobQueue.setTradingDate(DateUtil.parseLocalDate(dateStr).atStartOfDay());
            jobQueue.setGroupId(taskRunDto.getGroupId());
            jobQueue.setRegionGroup(taskRunDto.getRegionGroup());

            queueService.validateGenIwsAndCalcQueuedJobs(runDto);
            queueService.save(jobQueue);
        });

        return new ResponseEntity(HttpStatus.OK);
    }

    @PostMapping("/gen-monthly-summary")
    public ResponseEntity runGenMonthlySummaryJob(@RequestBody TaskRunDto taskRunDto, Principal principal) throws URISyntaxException {

        taskRunDto.setRunId(System.currentTimeMillis());
        taskRunDto.setJobName(SettlementJobName.GEN_MONTHLY_SUMMARY_RTA);
        taskRunDto.setCurrentUser(SecurityUtil.getCurrentUser(principal));
        log.info("Queueing genMonthlySummary for trading amounts. taskRunDto={}", taskRunDto);

        validateAdjustedRun(taskRunDto);

        BatchJobQueue jobQueue = BatchJobQueueService.newInst(Module.SETTLEMENT, JobProcess.GEN_MONTHLY_SUMMARY_RTA, taskRunDto);
        queueService.save(jobQueue);

        return new ResponseEntity(HttpStatus.OK);
    }

    @PostMapping("/calculate-rgmr")
    public ResponseEntity runCalculateGmrJob(@RequestBody TaskRunDto taskRunDto, Principal principal) throws URISyntaxException {

        taskRunDto.setRunId(System.currentTimeMillis());
        taskRunDto.setJobName(SettlementJobName.CALC_RGMR);
        taskRunDto.setCurrentUser(SecurityUtil.getCurrentUser(principal));
        log.info("Queueing calculateRgmrJob for trading amounts. taskRunDto={}", taskRunDto);

        validateAdjustedRun(taskRunDto);

        BatchJobQueue jobQueue = BatchJobQueueService.newInst(Module.SETTLEMENT, JobProcess.CALC_RGMR_VAT, taskRunDto);
        queueService.save(jobQueue);

        return new ResponseEntity(HttpStatus.OK);
    }

    @PostMapping("/finalize")
    public ResponseEntity runFinalizeJob(@RequestBody TaskRunDto taskRunDto, Principal principal) throws URISyntaxException {

        taskRunDto.setRunId(System.currentTimeMillis());
        taskRunDto.setJobName(SettlementJobName.TAG_RTA);
        taskRunDto.setCurrentUser(SecurityUtil.getCurrentUser(principal));

        validateAdjustedRun(taskRunDto);

        log.info("Queueing finalize job for reserve trading amounts. taskRunDto={}", taskRunDto);

        BatchJobQueue jobQueue = BatchJobQueueService.newInst(Module.SETTLEMENT, JobProcess.FINALIZE_RTA, taskRunDto);
        queueService.save(jobQueue);

        return new ResponseEntity(HttpStatus.OK);
    }

    private void validateAdjustedRun(final TaskRunDto taskRunDto) {
        if (Objects.equals(taskRunDto.getMeterProcessType(), MeterProcessType.ADJUSTED.name()) || taskRunDto.isNewGroup()) {
            queueService.validateAdjustedProcess(taskRunDto, JobProcess.FINALIZE_TA);
        }
    }

    @PostMapping("/generate-bill-statement-file-reserve")
    public ResponseEntity runGenerateStatementJobReserve(@RequestBody TaskRunDto taskRunDto, Principal principal) throws URISyntaxException {

        taskRunDto.setRunId(System.currentTimeMillis());
        taskRunDto.setJobName(SettlementJobName.FILE_RSV_BILL_STATEMENT_TA);
        taskRunDto.setCurrentUser(SecurityUtil.getCurrentUser(principal));
        log.info("Queueing generate bill statement job for reserve trading amounts. taskRunDto={}", taskRunDto);

        BatchJobQueue jobQueue = BatchJobQueueService.newInst(Module.SETTLEMENT, JobProcess.GEN_RESERVE_BILLING_STATEMENTS, taskRunDto);
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

    @PostMapping("/calculate-alloc")
    public ResponseEntity runCalculateAllocJob(@RequestBody TaskRunDto taskRunDto, Principal principal) throws URISyntaxException {

        taskRunDto.setRunId(System.currentTimeMillis());
        taskRunDto.setJobName(SettlementJobName.CALC_ALLOC_RESERVE);
        taskRunDto.setCurrentUser(SecurityUtil.getCurrentUser(principal));

        log.info("Queueing calculate allocation job for reserve trading amounts. taskRunDto={}", taskRunDto);

        BatchJobQueue jobQueue = BatchJobQueueService.newInst(Module.SETTLEMENT, JobProcess.CALC_ALLOC_RESERVE, taskRunDto);
        queueService.save(jobQueue);

        return new ResponseEntity(HttpStatus.OK);
    }

    @PostMapping("/generate-alloc-report")
    public ResponseEntity runGenerateAllocReportJob(@RequestBody TaskRunDto taskRunDto, Principal principal) throws URISyntaxException {

        taskRunDto.setRunId(System.currentTimeMillis());
        taskRunDto.setJobName(SettlementJobName.FILE_ALLOC_RESERVE);
        taskRunDto.setCurrentUser(SecurityUtil.getCurrentUser(principal));

        log.info("Queueing generate allocation report job for reserve trading amounts. taskRunDto={}", taskRunDto);

        BatchJobQueue jobQueue = BatchJobQueueService.newInst(Module.SETTLEMENT, JobProcess.GEN_ALLOC_REPORT_RESERVE, taskRunDto);
        queueService.save(jobQueue);

        return new ResponseEntity(HttpStatus.OK);
    }

    @PostMapping(value = "/get-batch-job-skip-logs")
    public ResponseEntity<Page<BatchJobSkipLog>> getBatchJobSkipLogs(@RequestBody PageableRequest pageableRequest) {
        LOG.debug("Finding skip logs request. pageable={}", pageableRequest.getPageable());
        return new ResponseEntity<>(taskExecutionService.getBatchJobSkipLogs(pageableRequest), HttpStatus.OK);
    }

    @PostMapping(value = "/download/get-batch-job-skip-logs")
    public void exportBatchJobSkipLogs(@RequestBody PageableRequest pageableRequest, HttpServletResponse response) throws IOException {
        LOG.debug("Exporting skip logs request. pageable={}", pageableRequest.getPageable());
        dispatchIntervalService.exportBatchJobSkipLogs(pageableRequest, response);
    }

    @PostMapping(value = "/download/get-processed-logs")
    public void exportProcessedLogs(@RequestBody PageableRequest pageableRequest, HttpServletResponse response) throws IOException {
        LOG.debug("Exporting processed logs request. pageable={}", pageableRequest.getPageable());
        dispatchIntervalService.exportProcessedLogs(pageableRequest, response);
    }
}
