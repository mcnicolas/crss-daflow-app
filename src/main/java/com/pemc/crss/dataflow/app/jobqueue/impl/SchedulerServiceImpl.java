package com.pemc.crss.dataflow.app.jobqueue.impl;

import com.pemc.crss.dataflow.app.dto.JobExecutionDto;
import com.pemc.crss.dataflow.app.dto.TaskRunDto;
import com.pemc.crss.dataflow.app.jobqueue.SchedulerService;
import com.pemc.crss.dataflow.app.service.TaskExecutionService;
import com.pemc.crss.dataflow.app.service.impl.DataFlowJdbcJobExecutionDao;
import com.pemc.crss.shared.commons.util.DateUtil;
import com.pemc.crss.shared.commons.util.ModelMapper;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobQueue;
import com.pemc.crss.shared.core.dataflow.repository.BatchJobQueueRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.Arrays;

import static com.pemc.crss.shared.core.dataflow.reference.QueueStatus.COMPLETED;
import static com.pemc.crss.shared.core.dataflow.reference.QueueStatus.FAILED_EXECUTION;
import static com.pemc.crss.shared.core.dataflow.reference.QueueStatus.FAILED_RUN;
import static com.pemc.crss.shared.core.dataflow.reference.QueueStatus.ON_QUEUE;
import static com.pemc.crss.shared.core.dataflow.reference.QueueStatus.STARTED;
import static com.pemc.crss.shared.core.dataflow.reference.QueueStatus.STARTING;

@Slf4j
@Service
public class SchedulerServiceImpl implements SchedulerService {

    @Autowired
    private BatchJobQueueRepository queueRepository;

    @Autowired
    private DataFlowJdbcJobExecutionDao dataFlowJdbcJobExecutionDao;

    @Autowired
    @Qualifier("tradingAmountsTaskExecutionService")
    private TaskExecutionService tradingAmountsTaskExecutionService;

    @Autowired
    @Qualifier("energyMarketFeeTaskExecutionService")
    private TaskExecutionService energyMarketFeeTaskExecutionService;

    @Autowired
    @Qualifier("reserveMarketFeeTaskExecutionService")
    private TaskExecutionService reserveMarketFeeTaskExecutionService;

    @Autowired
    @Qualifier("meterprocessTaskExecutionService")
    private TaskExecutionService meterprocessTaskExecutionService;

    @Autowired
    @Qualifier("mtrTaskExecutionService")
    private TaskExecutionService mtrTaskExecutionService;


    @Override
    public void execute() {
        BatchJobQueue nextJob = queueRepository.findFirstByStatusInOrderByRunIdAsc(Arrays.asList(ON_QUEUE, STARTED, STARTING));

        if (nextJob != null) {
            switch (nextJob.getStatus()) {
                case ON_QUEUE:
                    log.info("Running next Job in Queue. RunId: {}. Process: {}, JobName: {}",
                            nextJob.getRunId(), nextJob.getJobProcess(), nextJob.getJobName());
                    runNextQueuedJob(nextJob);
                    break;
                case STARTING:
                    log.info("Checking Job if Started: RunId: {}. Process: {}. JobName: {}",
                            nextJob.getRunId(), nextJob.getJobProcess(), nextJob.getJobName());
                    checkIfJobStarted(nextJob);
                    break;
                case STARTED:
                     log.info("Checking Job if Finished: RunId: {}. Process: {}. JobName: {}",
                            nextJob.getRunId(), nextJob.getJobProcess(), nextJob.getJobName());
                     checkIfJobIsFinished(nextJob);
                     break;
                default:
                    // do nothing
            }

            queueRepository.save(nextJob);
        } else {
            log.info("No Jobs to run at the moment.");
        }
    }

    private void runNextQueuedJob(final BatchJobQueue job) {
        try {
            TaskRunDto taskDto = ModelMapper.toModel(job.getTaskObj(), TaskRunDto.class);
            switch (job.getJobProcess()) {
                case RUN_WESM:
                case RUN_RCOA:
                case RUN_STL_READY:
                case FINALIZE_STL_READY:
                case GEN_MQ_REPORT:
                    meterprocessTaskExecutionService.launchJob(taskDto);
                    break;
                case GEN_MTR:
                    mtrTaskExecutionService.launchJob(taskDto);
                    break;
                case GEN_INPUT_WS_TA:
                case CALC_TA:
                case CALC_GMR_VAT:
                case FINALIZE_TA:
                case GEN_ENERGY_FILES:
                case GEN_RESERVE_FILES:
                    tradingAmountsTaskExecutionService.launchJob(taskDto);
                    break;
                case GEN_INPUT_WS_EMF:
                case CALC_EMF:
                case FINALIZE_EMF:
                case GEN_FILES_EMF:
                    energyMarketFeeTaskExecutionService.launchJob(taskDto);
                    break;
                case GEN_INPUT_WS_RMF:
                case CALC_RMF:
                case FINALIZE_RMF:
                case GEN_FILES_RMF:
                    reserveMarketFeeTaskExecutionService.launchJob(taskDto);
                    break;
                default:
                    // do nothing
            }

            job.setStatus(STARTING);
        } catch (Exception e) {

            log.error("Exception {} encountered when running {}, error: {}", e.getClass(), job.getJobProcess(), e.getMessage());
            job.setStatus(FAILED_EXECUTION);
            job.setDetails(ExceptionUtils.getStackTrace(e));
        }
    }

    private void checkIfJobStarted(final BatchJobQueue job) {
        JobExecutionDto jobExecution = dataFlowJdbcJobExecutionDao.findJobExecutionByRunId(job.getRunId());

        if (jobExecution != null) {
            log.info("Found Job Execution With job execution id: {} and status: {}",
                    jobExecution.getJobExecutionId(), jobExecution.getStatus());
            job.setStatus(STARTED);
            job.setJobExecutionId(jobExecution.getJobExecutionId());
            job.setJobExecStart(DateUtil.convertToLocalDateTime(jobExecution.getStartTime()));
        } else {
            log.info("No Job Execution started yet for run id: {}", job.getRunId());
        }
    }

    private void checkIfJobIsFinished(final BatchJobQueue job) {
        JobExecutionDto jobExecution = dataFlowJdbcJobExecutionDao.findJobExecutionByJobExecutionId(job.getJobExecutionId());

        switch (jobExecution.getBatchStatus()) {
            case COMPLETED:
                log.info("Job {} is COMPLETED given jobExecutionId {}", job.getJobProcess(), job.getJobExecutionId());
                job.setStatus(COMPLETED);
                job.setJobExecEnd(DateUtil.convertToLocalDateTime(jobExecution.getEndTime()));
                break;
            case FAILED:
                log.info("Job {} FAILED given jobExecutionId {}", job.getJobProcess(), job.getJobExecutionId());
                job.setStatus(FAILED_RUN);
                job.setJobExecEnd(DateUtil.convertToLocalDateTime(jobExecution.getEndTime()));
                break;
            default:
                // do nothing. job is probably still running
        }

    }


}