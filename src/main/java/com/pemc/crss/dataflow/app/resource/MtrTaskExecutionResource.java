package com.pemc.crss.dataflow.app.resource;

import com.pemc.crss.dataflow.app.dto.TaskRunDto;
import com.pemc.crss.dataflow.app.dto.parent.StubTaskExecutionDto;
import com.pemc.crss.dataflow.app.jobqueue.BatchJobQueueService;
import com.pemc.crss.dataflow.app.service.TaskExecutionService;
import com.pemc.crss.dataflow.app.util.SecurityUtil;
import com.pemc.crss.shared.commons.util.reference.Module;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobQueue;
import com.pemc.crss.shared.core.dataflow.reference.JobProcess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.net.URISyntaxException;
import java.security.Principal;

@RestController
@RequestMapping("/task-executions/mtr")
public class MtrTaskExecutionResource {

    private static final Logger LOG = LoggerFactory.getLogger(MtrTaskExecutionResource.class);

    @Autowired
    @Qualifier("mtrTaskExecutionService")
    private TaskExecutionService taskExecutionService;

    @Autowired
    private BatchJobQueueService queueService;

    @RequestMapping(method = RequestMethod.GET)
    public ResponseEntity<Page<? extends StubTaskExecutionDto>> findAllJobInstances(Pageable pageable) {
        LOG.debug("Finding job instances request. pageable={}", pageable);
        return new ResponseEntity<>(taskExecutionService.findJobInstances(pageable), HttpStatus.OK);
    }

    @RequestMapping(method = RequestMethod.POST)
    public ResponseEntity runJob(@RequestBody TaskRunDto taskRunDto, Principal principal) throws URISyntaxException {
        String currentUser = SecurityUtil.getCurrentUser(principal);

        taskRunDto.setRunId(System.currentTimeMillis());
        taskRunDto.setCurrentUser(currentUser);
        LOG.debug("Queueing job request. taskRunDto={}", taskRunDto);

        // For now this resource only runs Generate MTR job
        BatchJobQueue jobQueue = BatchJobQueueService.newInst(Module.METERING, JobProcess.GEN_MTR, taskRunDto);
        queueService.save(jobQueue);

        return new ResponseEntity(HttpStatus.OK);
    }
}
