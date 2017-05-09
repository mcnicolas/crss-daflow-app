package com.pemc.crss.dataflow.app.resource;

import com.pemc.crss.dataflow.app.dto.BaseTaskExecutionDto;
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
import org.springframework.security.oauth2.provider.OAuth2Authentication;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.net.URISyntaxException;
import java.security.Principal;
import java.util.LinkedHashMap;

@RestController
@RequestMapping("/task-executions/mtr")
public class MtrTaskExecutionResource {

    private static final Logger LOG = LoggerFactory.getLogger(MtrTaskExecutionResource.class);

    @Autowired
    @Qualifier("mtrTaskExecutionService")
    private TaskExecutionService taskExecutionService;

    @RequestMapping(method = RequestMethod.GET)
    public ResponseEntity<Page<? extends BaseTaskExecutionDto>> findAllJobInstances(Pageable pageable) {
        LOG.debug("Finding job instances request. pageable={}", pageable);
        return new ResponseEntity<>(taskExecutionService.findJobInstances(pageable), HttpStatus.OK);
    }

    @RequestMapping(method = RequestMethod.POST)
    public ResponseEntity runJob(@RequestBody TaskRunDto taskRunDto, Principal principal) throws URISyntaxException {
        LOG.debug("Running job request. taskRunDto={}", taskRunDto);
        String currentUser = SecurityUtil.getCurrentUser(principal);
        taskRunDto.setCurrentUser(currentUser);
        taskExecutionService.launchJob(taskRunDto);
        return new ResponseEntity(HttpStatus.OK);
    }
}
