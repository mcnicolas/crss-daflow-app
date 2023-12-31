package com.pemc.crss.dataflow.app.resource;

import com.pemc.crss.dataflow.app.dto.parent.StubTaskExecutionDto;
import com.pemc.crss.dataflow.app.dto.TaskRunDto;
import com.pemc.crss.dataflow.app.service.TaskExecutionService;
import com.pemc.crss.dataflow.app.util.SecurityUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.URISyntaxException;
import java.security.Principal;

@RestController
@RequestMapping("/task-executions/datainterface")
public class DataInterfaceTaskExecutionResource {

    private static final Logger LOG = LoggerFactory.getLogger(DataInterfaceTaskExecutionResource.class);
    @Autowired
    @Qualifier("dataInterfaceTaskExecutionService")
    private TaskExecutionService taskExecutionService;

    @RequestMapping(method = RequestMethod.GET)
    public ResponseEntity<Page<? extends StubTaskExecutionDto>> findDataInterfaceJobInstances(
            Pageable pageable, @RequestParam String type, @RequestParam String status, @RequestParam String mode,
            @RequestParam String runStartDate, @RequestParam String runEndDate, @RequestParam String tradingStartDate,
            @RequestParam String tradingEndDate, @RequestParam String username) {
        LOG.debug("Finding job instances request. pageable={}", pageable);
        return new ResponseEntity<>(taskExecutionService.findJobInstances(pageable, type, status, mode,
                runStartDate, tradingStartDate, tradingEndDate, username), HttpStatus.OK);
    }

    @RequestMapping(method = RequestMethod.POST)
    public ResponseEntity runJob(@RequestBody TaskRunDto taskRunDto, Principal principal) throws URISyntaxException {
        String currentUser = SecurityUtil.getCurrentUser(principal);
        LOG.debug("Running job request. taskRunDto={}, user={}", taskRunDto, currentUser);
        taskRunDto.setCurrentUser(currentUser);

        taskExecutionService.launchJob(taskRunDto);
        return new ResponseEntity(HttpStatus.OK);
    }

    @RequestMapping(value = "/auto", method = RequestMethod.POST)
    public ResponseEntity runAutomaticJob(@RequestBody TaskRunDto taskRunDto, Principal principal) throws URISyntaxException {
        return runJob(taskRunDto, principal);
    }

    @RequestMapping(value = "/get-dispatch-interval", method = RequestMethod.GET)
    public int getDispatchInterval() {
        return taskExecutionService.getDispatchInterval();
    }

}
