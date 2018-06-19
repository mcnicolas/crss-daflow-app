package com.pemc.crss.dataflow.app.resource;

import com.pemc.crss.dataflow.app.dto.AddtlCompensationRunDto;
import com.pemc.crss.dataflow.app.dto.AddtlCompensationRunListDto;
import com.pemc.crss.dataflow.app.dto.TaskRunDto;
import com.pemc.crss.dataflow.app.dto.parent.StubTaskExecutionDto;
import com.pemc.crss.dataflow.app.jobqueue.BatchJobQueueService;
import com.pemc.crss.dataflow.app.service.TaskExecutionService;
import com.pemc.crss.dataflow.app.service.impl.AddtlCompensationExecutionServiceImpl;
import com.pemc.crss.dataflow.app.support.PageableRequest;
import com.pemc.crss.dataflow.app.util.SecurityUtil;
import com.pemc.crss.shared.commons.util.reference.Module;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobQueue;
import com.pemc.crss.shared.core.dataflow.reference.AddtlCompJobName;
import com.pemc.crss.shared.core.dataflow.reference.JobProcess;
import com.pemc.crss.shared.core.dataflow.reference.SettlementJobName;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.Page;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

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
    private BatchJobQueueService batchJobQueueService;

    @PostMapping("/job-instances")
    public ResponseEntity<Page<? extends StubTaskExecutionDto>> findAllJobInstances(@RequestBody PageableRequest pageableRequest) {
        log.debug("Finding job instances request. pageable={}", pageableRequest.getPageable());
        return new ResponseEntity<>(taskExecutionService.findJobInstances(pageableRequest), HttpStatus.OK);
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

    @RequestMapping(value = "/finalize", method = RequestMethod.POST)
    public ResponseEntity finalize(@RequestBody TaskRunDto taskRunDto, Principal principal) throws URISyntaxException {

        taskRunDto.setRunId(System.currentTimeMillis());
        taskRunDto.setJobName(AddtlCompJobName.AC_CALC_GMR_BASE_NAME);
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


}
