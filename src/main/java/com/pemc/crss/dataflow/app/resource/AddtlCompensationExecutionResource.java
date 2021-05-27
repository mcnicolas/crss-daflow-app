package com.pemc.crss.dataflow.app.resource;

import com.pemc.crss.dataflow.app.dto.AddtlCompensationRunDto;
import com.pemc.crss.dataflow.app.dto.AddtlCompensationRunListDto;
import com.pemc.crss.dataflow.app.dto.TaskRunDto;
import com.pemc.crss.dataflow.app.dto.parent.StubTaskExecutionDto;
import com.pemc.crss.dataflow.app.jobqueue.BatchJobQueueService;
import com.pemc.crss.dataflow.app.service.AddtlCompJobService;
import com.pemc.crss.dataflow.app.service.FinalizedAddtlCompensationExecutionService;
import com.pemc.crss.dataflow.app.service.TaskExecutionService;
import com.pemc.crss.dataflow.app.service.impl.AddtlCompensationExecutionServiceImpl;
import com.pemc.crss.dataflow.app.support.PageableRequest;
import com.pemc.crss.dataflow.app.util.SecurityUtil;
import com.pemc.crss.shared.commons.util.reference.Module;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobQueue;
import com.pemc.crss.shared.core.dataflow.reference.AddtlCompJobName;
import com.pemc.crss.shared.core.dataflow.reference.JobProcess;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.Page;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.Principal;

@Slf4j
@RestController
@RequestMapping("/task-executions/additional-compensation")
public class AddtlCompensationExecutionResource {

    @Autowired
    @Qualifier("addtlCompensationExecutionService")
    private TaskExecutionService taskExecutionService;

    @Autowired
    private FinalizedAddtlCompensationExecutionService finalizedAddtlCompensationExecutionService;

    @Autowired
    private BatchJobQueueService batchJobQueueService;

    @Autowired
    private AddtlCompJobService addtlCompJobService;

    @PostMapping("/job-instances")
    public ResponseEntity<Page<? extends StubTaskExecutionDto>> findAllJobInstances(@RequestBody PageableRequest pageableRequest) {
        log.debug("Finding job instances request. pageable={}", pageableRequest.getPageable());
        return new ResponseEntity<>(taskExecutionService.findJobInstances(pageableRequest), HttpStatus.OK);
    }

    @PostMapping("/download/job-instances")
    public void exportAllJobInstances(@RequestBody PageableRequest pageableRequest, HttpServletResponse response) throws IOException {
        log.debug("Downloading job instances request. pageable={}", pageableRequest.getPageable());
        finalizedAddtlCompensationExecutionService.exportJobInstances(pageableRequest, response);
    }

    @RequestMapping(value = "/multi", method = RequestMethod.POST)
    public ResponseEntity runGroupJob(@RequestBody AddtlCompensationRunListDto addtlCompensationRunListDto, Principal principal) throws URISyntaxException {
        log.debug("Running job request. addtlCompensationRunDtos={}", addtlCompensationRunListDto);

        ((AddtlCompensationExecutionServiceImpl)taskExecutionService).validateAddtlCompDtos(addtlCompensationRunListDto.getAddtlCompensationRunDtos());

        for (AddtlCompensationRunDto acRunDto : addtlCompensationRunListDto.getAddtlCompensationRunDtos()) {
            TaskRunDto taskRunDto = new TaskRunDto();
            taskRunDto.setMtn(acRunDto.getMtn());
            taskRunDto.setApprovedRate(acRunDto.getApprovedRate());
            taskRunDto.setBillingId(acRunDto.getBillingId());
            taskRunDto.setBillingStartDate(acRunDto.getBillingStartDate());
            taskRunDto.setBillingEndDate(acRunDto.getBillingEndDate());
            taskRunDto.setPricingCondition(acRunDto.getPricingCondition());

            taskRunDto.setRunId(System.currentTimeMillis());
            taskRunDto.setJobName(AddtlCompJobName.AC_CALC);
            taskRunDto.setCurrentUser(SecurityUtil.getCurrentUser(principal));

            log.info("Queueing calculateJob for additional compensation. taskRunDto={}", taskRunDto);

            BatchJobQueue acJobQueue = BatchJobQueueService.newInst(Module.SETTLEMENT, JobProcess.CALC_AC, taskRunDto);

            batchJobQueueService.save(acJobQueue);
        }

        return new ResponseEntity(HttpStatus.OK);
    }

    @RequestMapping(value = "/calc-gmr-vat", method = RequestMethod.POST)
    public ResponseEntity calculateGmrVat(@RequestBody TaskRunDto taskRunDto, Principal principal) throws URISyntaxException {

        taskRunDto.setRunId(System.currentTimeMillis());
        taskRunDto.setJobName(AddtlCompJobName.AC_CALC_GMR_BASE_NAME);
        taskRunDto.setCurrentUser(SecurityUtil.getCurrentUser(principal));

        log.info("Queueing calculate gmr/vat job for additional compensation. taskRunDto={}", taskRunDto);

        BatchJobQueue jobQueue = BatchJobQueueService.newInst(Module.SETTLEMENT, JobProcess.CALC_GMR_VAT_AC, taskRunDto);
        batchJobQueueService.save(jobQueue);

        return new ResponseEntity(HttpStatus.OK);
    }

    @RequestMapping(value = "/finalize", method = RequestMethod.POST)
    public ResponseEntity finalize(@RequestBody TaskRunDto taskRunDto, Principal principal) throws URISyntaxException {

        taskRunDto.setRunId(System.currentTimeMillis());
        taskRunDto.setJobName(AddtlCompJobName.AC_FINALIZE_BASE_NAME);
        taskRunDto.setCurrentUser(SecurityUtil.getCurrentUser(principal));

        log.info("Queueing finalize job for additional compensation. taskRunDto={}", taskRunDto);

        BatchJobQueue jobQueue = BatchJobQueueService.newInst(Module.SETTLEMENT, JobProcess.FINALIZE_AC, taskRunDto);
        batchJobQueueService.save(jobQueue);

        return new ResponseEntity(HttpStatus.OK);
    }

    @PostMapping(value = "/generate-files")
    public ResponseEntity generateFiles(@RequestBody TaskRunDto taskRunDto, Principal principal) throws URISyntaxException {

        taskRunDto.setRunId(System.currentTimeMillis());
        taskRunDto.setJobName(AddtlCompJobName.AC_GEN_FILE);
        taskRunDto.setCurrentUser(SecurityUtil.getCurrentUser(principal));

        log.info("Queueing generate files job for additional compensation. taskRunDto={}", taskRunDto);

        BatchJobQueue jobQueue = BatchJobQueueService.newInst(Module.SETTLEMENT, JobProcess.GEN_FILES_AC, taskRunDto);
        batchJobQueueService.save(jobQueue);

        return new ResponseEntity(HttpStatus.OK);
    }

    @RequestMapping(method = RequestMethod.DELETE)
    public ResponseEntity deleteJob(@RequestParam(value = "jobId") long jobId,
                                    @RequestParam(value = "mtn") String mtn,
                                    @RequestParam(value = "pricingCondition") String pricingCondition,
                                    @RequestParam(value = "groupId") String groupId) throws URISyntaxException {
        addtlCompJobService.deleteJob(jobId, mtn, pricingCondition, groupId);
        return new ResponseEntity(HttpStatus.OK);
    }

    @PostMapping(value = "/validate-billing-period")
    public ResponseEntity<Boolean> validateBillingPeriod(@RequestBody TaskRunDto taskRunDto, Principal principal) throws URISyntaxException {
        return new ResponseEntity(addtlCompJobService.validateBillingPeriod(taskRunDto), HttpStatus.OK);
    }

}
