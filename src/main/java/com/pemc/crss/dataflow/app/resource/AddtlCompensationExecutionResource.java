package com.pemc.crss.dataflow.app.resource;

import com.pemc.crss.dataflow.app.dto.AddtlCompensationRunDto;
import com.pemc.crss.dataflow.app.dto.BaseTaskExecutionDto;
import com.pemc.crss.dataflow.app.service.TaskExecutionService;
import com.pemc.crss.dataflow.app.service.impl.AddtlCompensationExecutionServiceImpl;
import com.pemc.crss.dataflow.app.support.PageableRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.Page;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.URISyntaxException;

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
    public ResponseEntity runJob(@RequestBody AddtlCompensationRunDto addtlCompensationRunDto) throws URISyntaxException {
        LOG.debug("Running job request. addtlCompensationRunDto={}", addtlCompensationRunDto);
        ((AddtlCompensationExecutionServiceImpl)taskExecutionService).launchAddtlCompensation(addtlCompensationRunDto);
        return new ResponseEntity(HttpStatus.OK);
    }

}
