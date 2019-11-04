package com.pemc.crss.dataflow.app.resource;

import com.pemc.crss.dataflow.app.dto.TaskRunDto;
import com.pemc.crss.dataflow.app.dto.parent.GroupTaskExecutionDto;
import com.pemc.crss.dataflow.app.dto.parent.StubTaskExecutionDto;
import com.pemc.crss.dataflow.app.jobqueue.BatchJobQueueService;
import com.pemc.crss.dataflow.app.service.TaskExecutionService;
import com.pemc.crss.dataflow.app.util.SecurityUtil;
import com.pemc.crss.shared.commons.util.DateUtil;
import com.pemc.crss.shared.commons.util.reference.Module;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobQueue;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobSkipLog;
import com.pemc.crss.shared.core.dataflow.reference.JobProcess;
import com.pemc.crss.shared.core.dataflow.repository.ExecutionParamRepository;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.repository.dao.JobExecutionDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.URISyntaxException;
import java.security.Principal;
import java.util.Arrays;

import static org.apache.commons.lang3.StringUtils.EMPTY;

@RestController
@RequestMapping("/task-executions/meterprocess")
public class MeterprocessTaskExecutionResource {

    private static final Logger LOG = LoggerFactory.getLogger(MeterprocessTaskExecutionResource.class);

    @Autowired
    @Qualifier("meterprocessTaskExecutionService")
    private TaskExecutionService taskExecutionService;

    @Autowired
    private ExecutionParamRepository executionParamRepository;

    @Autowired
    private BatchJobQueueService queueService;

    @Autowired
    private JobExplorer jobExplorer;

    @Autowired
    private JobExecutionDao jobExecutionDao;

    @RequestMapping(method = RequestMethod.GET)
    public ResponseEntity<Page<? extends StubTaskExecutionDto>> findAllJobInstances(Pageable pageable) {
        LOG.debug("Finding job instances request. pageable={}", pageable);
        return new ResponseEntity<>(taskExecutionService.findJobInstances(pageable), HttpStatus.OK);
    }

    @RequestMapping(value = "/find-by-billing-period", method = RequestMethod.GET)
    public ResponseEntity<Page<GroupTaskExecutionDto>> findAllJobInstancesGroupByBillingPeriod(Pageable pageable) {
        LOG.debug("Finding job instances request. pageable={}", pageable);
        return new ResponseEntity<>(taskExecutionService.findDistinctBillingPeriodAndProcessType(pageable), HttpStatus.OK);
    }

    @RequestMapping(value = "/find-jobs-by-billing-period", method = RequestMethod.GET)
    public ResponseEntity<Page<? extends StubTaskExecutionDto>> findAllJobInstancesGroupByBillingPeriod(Pageable pageable, String billingPeriod, String processType, Long adjNo) {
        LOG.debug("Finding job instances request. pageable={}", pageable);
        return new ResponseEntity<>(taskExecutionService.findJobInstancesByBillingPeriodAndProcessType(pageable, billingPeriod, processType, adjNo), HttpStatus.OK);
    }

    @RequestMapping(method = RequestMethod.POST)
    public ResponseEntity runJob(@RequestBody TaskRunDto taskRunDto, Principal principal) throws URISyntaxException {
        String currentUser = SecurityUtil.getCurrentUser(principal);
        taskRunDto.setCurrentUser(currentUser);
        LOG.debug("Queueing job request. taskRunDto={}", taskRunDto);

        if (taskRunDto.getParentJob() != null) {
            setJobParams(taskRunDto);
        }

        if ("computeWesmMq".equalsIgnoreCase(taskRunDto.getJobName())) {
            Arrays.stream(taskRunDto.getRegionGroup().split(",")).forEach(
                    s -> {
                        TaskRunDto dto = taskRunDto.clone(taskRunDto);
                        dto.setRunId(System.currentTimeMillis());
                        dto.setRegionGroup(s);
                        BatchJobQueue jobQueue = BatchJobQueueService.newInst(Module.METERING,
                                determineMeterJobProcessByJobName(dto.getJobName()), dto);
                        queueService.save(jobQueue);
                    });
        } else {
            taskRunDto.setRunId(System.currentTimeMillis());
            BatchJobQueue jobQueue = BatchJobQueueService.newInst(Module.METERING,
                    determineMeterJobProcessByJobName(taskRunDto.getJobName()), taskRunDto);
            queueService.save(jobQueue);
        }

        return new ResponseEntity(HttpStatus.OK);
    }

    private JobProcess determineMeterJobProcessByJobName(final String jobName) {
        //TODO: (optimization) Metering Job Names probably need to be set in shared repo
        switch (jobName) {
            case "computeWesmMq":
                return JobProcess.RUN_WESM;
            case "computeRcoaMq":
                return JobProcess.RUN_RCOA;
            case "stlNotReady":
                return JobProcess.RUN_STL_READY;
            case "stlReady":
                return JobProcess.FINALIZE_STL_READY;
            case "genReport":
                return JobProcess.GEN_MQ_REPORT;
            default:
                throw new RuntimeException("Unrecognized metering job name: " + jobName);
        }
    }

    private void setJobParams(final TaskRunDto taskRunDto) {
        JobInstance jobInstance = jobExplorer.getJobInstance(Long.valueOf(taskRunDto.getParentJob()));
        if (jobInstance != null) {
            jobExecutionDao.findJobExecutions(jobInstance).stream().findFirst().ifPresent(jobExec -> {
                JobParameters jobParameters = jobExec.getJobParameters();
                taskRunDto.setMeterProcessType(jobParameters.getString("processType"));
                taskRunDto.setStartDate(DateUtil.convertToString(jobParameters.getDate("startDate"), DateUtil.DEFAULT_DATE_FORMAT));
                taskRunDto.setEndDate(DateUtil.convertToString(jobParameters.getDate("endDate"), DateUtil.DEFAULT_DATE_FORMAT));
                taskRunDto.setTradingDate(DateUtil.convertToString(jobParameters.getDate("date"), DateUtil.DEFAULT_DATE_FORMAT));
            });
        }
    }

    @RequestMapping(value = "/get-dispatch-interval", method = RequestMethod.GET)
    public int getDispatchInterval() {
        return taskExecutionService.getDispatchInterval();
    }

    @RequestMapping(method = RequestMethod.DELETE)
    public ResponseEntity deleteJob(@RequestParam(value = "jobId") long jobId) throws URISyntaxException {
        taskExecutionService.deleteJob(jobId);
        return new ResponseEntity(HttpStatus.OK);
    }

    @RequestMapping(value = "/get-batch-job-skip-logs", method = RequestMethod.GET)
    public ResponseEntity<Page<BatchJobSkipLog>> getBatchJobSkipLogs(Pageable pageable, int stepId) {
        LOG.debug("Finding skip logs request. pageable={}", pageable);
        return new ResponseEntity<>(taskExecutionService.getBatchJobSkipLogs(pageable, stepId), HttpStatus.OK);
    }

    @RequestMapping(value = "/get-failed-exit-msg", method = RequestMethod.GET)
    public ResponseEntity<String> getBatchJobSkipLogs(@RequestParam int stepId) {
        LOG.debug("Finding failed exit message for step id {}.", stepId);
        return new ResponseEntity<>(taskExecutionService.getFailedExitMsg(stepId), HttpStatus.OK);
    }

    @RequestMapping(value = "/count-final-run-all-mtn", method = RequestMethod.GET)
    public ResponseEntity<Integer> countFinalRunAllMtn(@RequestParam(required = false) String processType,
                                                       @RequestParam(required = false) String date,
                                                       @RequestParam(required = false) String startDate,
                                                       @RequestParam(required = false) String endDate,
                                                       @RequestParam(required = false) String regionGroup) {
        LOG.debug("Counting final stl ready with processtype={}, date={}, startDate={}, endDate={}.", processType, date, startDate, endDate);
        return new ResponseEntity<>(countAllMtnFinalStlReadyRun(processType, date, startDate, endDate, regionGroup), HttpStatus.OK);
    }

    @RequestMapping(value = "/get-aggr-mtn-final-stl-ready", method = RequestMethod.GET)
    public ResponseEntity<String> getAggregatedSelectedMtnFinalStlReady(@RequestParam(required = false) String processType,
                                                                        @RequestParam(required = false) String date,
                                                                        @RequestParam(required = false) String startDate,
                                                                        @RequestParam(required = false) String endDate) {
        LOG.debug("Counting final stl ready with processtype={}, date={}, startDate={}, endDate={}.", processType, date, startDate, endDate);
        return new ResponseEntity<>(getAggregatedSelectedMtnFinalStlReadyRun(processType, date, startDate, endDate), HttpStatus.OK);
    }

    private String getAggregatedSelectedMtnFinalStlReadyRun(String processType, String date, String startDate, String endDate) {
        if (StringUtils.isNotEmpty(processType)) {
            if ("DAILY".equalsIgnoreCase(processType)) {
                if (date != null) {
                    return executionParamRepository.getAggregatedSelectedMtnsDailyWithinRange(date);
                } else {
                    return EMPTY;
                }
            } else {
                return executionParamRepository.getAggregatedSelectedMtnsMonthlyWithinRange(startDate, endDate, processType);
            }
        } else {
            return EMPTY;
        }
    }

    private Integer countAllMtnFinalStlReadyRun(String processType, String date, String startDate, String endDate, String regionGroup) {
        Integer result;
        if (StringUtils.isNotEmpty(processType)) {
            if ("DAILY".equalsIgnoreCase(processType) && StringUtils.isNotEmpty(date)) {
                if (StringUtils.isEmpty(regionGroup) || "ALL".equalsIgnoreCase(regionGroup)) {
                    result = executionParamRepository.countDailyRunAllMtn(date, "stlReady");
                } else {
                    result = executionParamRepository.countDailyRunAllMtnRegionGroup(date, "stlReady", regionGroup);
                }
            } else {
                if (StringUtils.isEmpty(regionGroup) || "ALL".equalsIgnoreCase(regionGroup)) {
                    result = StringUtils.isNotEmpty(startDate) && StringUtils.isNotEmpty(endDate) ?
                            executionParamRepository.countMonthlyRunAllMtn(startDate, endDate, processType, "stlReady") : 0;
                } else {
                    result = StringUtils.isNotEmpty(startDate) && StringUtils.isNotEmpty(endDate) ?
                            executionParamRepository.countMonthlyRunAllMtnRegionGroup(startDate, endDate, processType, "stlReady", regionGroup) : 0;
                }
            }
        } else {
            result = 0;
        }
        return result;
    }
}
