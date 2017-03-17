package com.pemc.crss.dataflow.app.resource;

import com.pemc.crss.dataflow.app.dto.BaseTaskExecutionDto;
import com.pemc.crss.dataflow.app.dto.TaskRunDto;
import com.pemc.crss.dataflow.app.service.DataFlowTaskExecutionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.oauth2.provider.OAuth2Authentication;
import org.springframework.web.bind.annotation.*;

import java.net.URISyntaxException;
import java.security.Principal;

@RestController
@RequestMapping("/task-executions/datainterface")
public class DataInterfaceTaskExecutionResource {

    public static final String ANONYMOUS = "anonymous";
    private static final Logger LOG = LoggerFactory.getLogger(DataInterfaceTaskExecutionResource.class);
    @Autowired
    @Qualifier("dataInterfaceTaskExecutionService")
    private DataFlowTaskExecutionService taskExecutionService;

    @RequestMapping(method = RequestMethod.GET)
    public ResponseEntity<Page<? extends BaseTaskExecutionDto>> findDataInterfaceJobInstances(
            Pageable pageable, @RequestParam String type, @RequestParam String status, @RequestParam String mode,
            @RequestParam String runStartDate, @RequestParam String runEndDate, @RequestParam String tradingStartDate,
            @RequestParam String tradingEndDate) {
        LOG.debug("Finding job instances request. pageable={}", pageable);
        return new ResponseEntity<>(taskExecutionService.findJobInstances(pageable, type, status, mode,
                runStartDate, runEndDate, tradingStartDate, tradingEndDate), HttpStatus.OK);
    }

    @RequestMapping(method = RequestMethod.POST)
    public ResponseEntity runJob(@RequestBody TaskRunDto taskRunDto, Principal principal) throws URISyntaxException {
        String currentUser = getCurrentUser(principal);
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

    private String getCurrentUser(Principal principal) {
        String currentUser = ANONYMOUS;
        if (principal != null) {
            if (principal instanceof OAuth2Authentication) {
                OAuth2Authentication auth = (OAuth2Authentication) principal;
                UserDetails userDetails = (UserDetails) auth.getPrincipal();
                return userDetails.getUsername();
            }
        }
        return currentUser;
    }

}
