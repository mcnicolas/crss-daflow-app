package com.pemc.crss.dataflow.app.jobqueue.impl;

import com.pemc.crss.dataflow.app.dto.JobExecutionDto;
import com.pemc.crss.dataflow.app.dto.TaskRunDto;
import com.pemc.crss.dataflow.app.jobqueue.SchedulerService;
import com.pemc.crss.dataflow.app.service.TaskExecutionService;
import com.pemc.crss.dataflow.app.service.impl.DataFlowJdbcJobExecutionDao;
import com.pemc.crss.shared.commons.reference.MeterProcessType;
import com.pemc.crss.shared.commons.util.DateUtil;
import com.pemc.crss.shared.commons.util.ModelMapper;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobQueue;
import com.pemc.crss.shared.core.dataflow.repository.BatchJobQueueRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

import static com.pemc.crss.shared.core.dataflow.reference.QueueStatus.COMPLETED;
import static com.pemc.crss.shared.core.dataflow.reference.QueueStatus.FAILED_EXECUTION;
import static com.pemc.crss.shared.core.dataflow.reference.QueueStatus.FAILED_RUN;
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

    @Autowired
    @Qualifier("addtlCompensationExecutionService")
    private TaskExecutionService addtlCompensationExecutionService;

    @Value("${scheduler.launch-timeout-seconds}")
    private Long launchTimeoutSeconds;

    @Autowired
    private NamedParameterJdbcTemplate dataflowJdbcTemplate;

    @Scheduled(fixedDelayString = "${scheduler.interval-milliseconds}")
    @Override
    public void execute() {
        // Add additional methods here
        executeGroup(DAILY_AND_AC);
        executeGroup(MONTHLY);
    }

    private void executeGroup(List<MeterProcessType> meterProcessTypes) {
        try {
            BatchJobQueue nextJob = queueRepository.findFirstByStatusInAndMeterProcessTypeInOrderByRunIdAsc(
                    IN_PROGRESS_STATUS, meterProcessTypes);

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

                String sql = "UPDATE batch_job_queue SET status = :status, details = :details,"
                        + " job_execution_id = :jobExecutionId, job_exec_start = :jobExecStart, job_exec_end = :jobExecEnd, "
                        + " starting_date = :startingDate WHERE id = :id";

                MapSqlParameterSource updateSource = new MapSqlParameterSource()
                        .addValue("status", nextJob.getStatus().name())
                        .addValue("details", nextJob.getDetails())
                        .addValue("jobExecutionId", nextJob.getJobExecutionId())
                        .addValue("jobExecStart", DateUtil.convertToDate(nextJob.getJobExecStart()))
                        .addValue("jobExecEnd", DateUtil.convertToDate(nextJob.getJobExecEnd()))
                        .addValue("startingDate", DateUtil.convertToDate(nextJob.getStartingDate()))
                        .addValue("id", nextJob.getId());

                dataflowJdbcTemplate.update(sql, updateSource);

            } else {
                log.info("No Jobs to run at the moment.");
            }
        } catch (Exception e) {
            log.error("Exception {} encountered when running meter process types {}. Error: {}", e.getClass(),
                    meterProcessTypes, e.getMessage());
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
                case CALC_LR:
                case CALC_GMR_VAT:
                case FINALIZE_TA:
                case FINALIZE_LR:
                case GEN_ENERGY_FILES:
                case GEN_RESERVE_FILES:
                case GEN_LR_FILES:
                case STL_VALIDATION:
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
                case CALC_AC:
                case FINALIZE_AC:
                case GEN_FILES_AC:
                    addtlCompensationExecutionService.launchJob(taskDto);
                    break;
                default:
                    throw new RuntimeException("Unrecognized Job Process: " + job.getJobProcess());
            }

            job.setStatus(STARTING);
            job.setStartingDate(LocalDateTime.now());
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
            if (job.getStartingDate() != null && LocalDateTime.now().isAfter(job.getStartingDate().plusSeconds(launchTimeoutSeconds))) {
                log.error("Job {} with id {} has exceeded timeout limit. Failing Job.", job.getJobName(), job.getId());
                job.setStatus(FAILED_EXECUTION);
                job.setDetails("Job for launch has exceeded timeout limit.");
            } else {
                log.info("No Job Execution started yet for run id: {}", job.getRunId());
            }
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
