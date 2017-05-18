package com.pemc.crss.dataflow.app.resource;

import com.pemc.crss.dataflow.app.dto.*;
import com.pemc.crss.dataflow.app.service.TaskExecutionService;
import com.pemc.crss.dataflow.app.service.impl.AddtlCompensationExecutionServiceImpl;
import com.pemc.crss.dataflow.app.support.PageableRequest;
import com.pemc.crss.dataflow.app.util.SecurityUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.Page;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.CollectionUtils;
import org.springframework.web.bind.annotation.*;

import java.net.URISyntaxException;
import java.security.Principal;
import java.util.Iterator;

@RestController
@RequestMapping("/task-executions/additional-compensation")
public class AddtlCompensationExecutionResource {

    private static final Logger LOG = LoggerFactory.getLogger(AddtlCompensationExecutionResource.class);

    @Autowired
    @Qualifier("addtlCompensationExecutionService")
    private TaskExecutionService taskExecutionService;

    @PostMapping("/job-instances")
    public ResponseEntity<Page<? extends BaseTaskExecutionDto>> findAllJobInstances(@RequestBody PageableRequest pageableRequest) {
        LOG.debug("Finding job instances request. pageable={}", pageableRequest.getPageable());
        return new ResponseEntity<>(taskExecutionService.findJobInstances(pageableRequest), HttpStatus.OK);
    }

    @RequestMapping(method = RequestMethod.POST)
    public ResponseEntity runJob(@RequestBody AddtlCompensationRunDto addtlCompensationRunDto, Principal principal) throws URISyntaxException {
        LOG.debug("Running job request. addtlCompensationRunDto={}", addtlCompensationRunDto);
        addtlCompensationRunDto.setCurrentUser(SecurityUtil.getCurrentUser(principal));
        ((AddtlCompensationExecutionServiceImpl)taskExecutionService).launchAddtlCompensation(addtlCompensationRunDto);
        return new ResponseEntity(HttpStatus.OK);
    }

    @RequestMapping(value = "/multi", method = RequestMethod.POST)
    public ResponseEntity runGroupJob(@RequestBody AddtlCompensationRunListDto addtlCompensationRunListDto, Principal principal) throws URISyntaxException {
        LOG.debug("Running job request. addtlCompensationRunDtos={}", addtlCompensationRunListDto);
        ((AddtlCompensationExecutionServiceImpl)taskExecutionService).launchGroupAddtlCompensation(addtlCompensationRunListDto, principal);
        return new ResponseEntity(HttpStatus.OK);
    }

    @RequestMapping(value = "/finalize", method = RequestMethod.POST)
    public ResponseEntity finalize(@RequestBody AddtlCompensationFinalizeDto addtlCompensationFinalizeDto) throws URISyntaxException {
        LOG.debug("Running job request. addtlCompensationFinalizeDto={}", addtlCompensationFinalizeDto);
        ((AddtlCompensationExecutionServiceImpl)taskExecutionService).finalizeAC(addtlCompensationFinalizeDto);
        return new ResponseEntity(HttpStatus.OK);
    }


}
