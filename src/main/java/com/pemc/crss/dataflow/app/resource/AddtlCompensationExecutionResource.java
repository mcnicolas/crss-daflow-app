package com.pemc.crss.dataflow.app.resource;

import com.pemc.crss.dataflow.app.dto.AddtlCompensationFinalizeDto;
import com.pemc.crss.dataflow.app.dto.AddtlCompensationGenFilesDto;
import com.pemc.crss.dataflow.app.dto.AddtlCompensationRunDto;
import com.pemc.crss.dataflow.app.dto.AddtlCompensationRunListDto;
import com.pemc.crss.dataflow.app.dto.parent.StubTaskExecutionDto;
import com.pemc.crss.dataflow.app.service.TaskExecutionService;
import com.pemc.crss.dataflow.app.service.impl.AddtlCompensationExecutionServiceImpl;
import com.pemc.crss.dataflow.app.support.PageableRequest;
import com.pemc.crss.dataflow.app.util.SecurityUtil;
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

    @PostMapping("/job-instances")
    public ResponseEntity<Page<? extends StubTaskExecutionDto>> findAllJobInstances(@RequestBody PageableRequest pageableRequest) {
        log.debug("Finding job instances request. pageable={}", pageableRequest.getPageable());
        return new ResponseEntity<>(taskExecutionService.findJobInstances(pageableRequest), HttpStatus.OK);
    }

    @RequestMapping(method = RequestMethod.POST)
    public ResponseEntity runJob(@RequestBody AddtlCompensationRunDto addtlCompensationRunDto, Principal principal) throws URISyntaxException {
        log.debug("Running job request. addtlCompensationRunDto={}", addtlCompensationRunDto);
        addtlCompensationRunDto.setCurrentUser(SecurityUtil.getCurrentUser(principal));
        ((AddtlCompensationExecutionServiceImpl)taskExecutionService).launchAddtlCompensation(addtlCompensationRunDto);
        return new ResponseEntity(HttpStatus.OK);
    }

    @RequestMapping(value = "/multi", method = RequestMethod.POST)
    public ResponseEntity runGroupJob(@RequestBody AddtlCompensationRunListDto addtlCompensationRunListDto, Principal principal) throws URISyntaxException {
        log.debug("Running job request. addtlCompensationRunDtos={}", addtlCompensationRunListDto);
        ((AddtlCompensationExecutionServiceImpl)taskExecutionService).launchGroupAddtlCompensation(addtlCompensationRunListDto, principal);
        return new ResponseEntity(HttpStatus.OK);
    }

    @RequestMapping(value = "/finalize", method = RequestMethod.POST)
    public ResponseEntity finalize(@RequestBody AddtlCompensationFinalizeDto addtlCompensationFinalizeDto, Principal principal) throws URISyntaxException {
        log.debug("Running job request. addtlCompensationFinalizeDto={}", addtlCompensationFinalizeDto);
        ((AddtlCompensationExecutionServiceImpl)taskExecutionService).finalizeAC(addtlCompensationFinalizeDto, principal);
        return new ResponseEntity(HttpStatus.OK);
    }

    @PostMapping(value = "/generate-files")
    public ResponseEntity generateFiles(@RequestBody AddtlCompensationGenFilesDto addtlCompensationGenFilesDto, Principal principal) throws URISyntaxException {
        log.debug("Running job request. addtlCompensationGenFilesDto={}", addtlCompensationGenFilesDto);
        ((AddtlCompensationExecutionServiceImpl)taskExecutionService).generateFilesAc(addtlCompensationGenFilesDto, principal);
        return new ResponseEntity(HttpStatus.OK);
    }


}
