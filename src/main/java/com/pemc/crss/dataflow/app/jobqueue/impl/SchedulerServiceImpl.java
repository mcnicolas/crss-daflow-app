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
import com.pemc.crss.shared.core.dataflow.reference.JobProcess;
import com.pemc.crss.shared.core.dataflow.repository.BatchJobQueueRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

import static com.pemc.crss.shared.core.dataflow.reference.QueueStatus.*;

@Slf4j
@Service
public class SchedulerServiceImpl implements SchedulerService {

    private static final String JOB_EXECUTION_TIMEOUT_CONFIG = "scheduler.job-execution-timeout-minutes";

    @Autowired
    private Environment env;

    @Autowired
    private BatchJobQueueRepository queueRepository;

    @Autowired
    private DataFlowJdbcJobExecutionDao dataFlowJdbcJobExecutionDao;

    @Autowired
    protected JobExplorer jobExplorer;

    @Autowired
    @Qualifier("tradingAmountsTaskExecutionService")
    private TaskExecutionService tradingAmountsTaskExecutionService;

    @Autowired
    @Qualifier("reserveTradingAmountsTaskExecutionService")
    private TaskExecutionService reserveTradingAmountsTaskExecutionService;

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

    @Value("${scheduler.job-execution-timeout-minutes.default}")
    private Long defaultJobExecutionTimeout;

    @Autowired
    private NamedParameterJdbcTemplate dataflowJdbcTemplate;

    @Value("${scheduler.parallel-stl-monthly-job}")
    private Long parallelStlMonthlyJob;

    @Value("${scheduler.execute-ac-at-monthly-node}")
    private String executeAcAtMonthlyNode;

    @Scheduled(fixedDelayString = "${scheduler.interval-milliseconds}")
    @Override
    public void execute() {
        if ("Y".equalsIgnoreCase(executeAcAtMonthlyNode)) {
            executeDaily(DAILY);
            executeMonthly(MONTHLY_AND_AC);
        } else {
            executeDaily(DAILY_AND_AC);
            executeMonthly(MONTHLY);
        }
    }

    private void executeDaily(List<MeterProcessType> meterProcessTypes) {
        try {
            BatchJobQueue nextJob = queueRepository.findFirstByStatusInAndMeterProcessTypeInOrderByRunIdAsc(
                    IN_PROGRESS_STATUS, meterProcessTypes);

            if (nextJob != null) {
                handleNextJob(nextJob);
            } else {
                log.info("No Jobs to run at the moment.");
            }
        } catch (Exception e) {
            log.error("Exception {} encountered when running meter process types {}. Error: {}", e.getClass(),
                    meterProcessTypes, e.getMessage());
        }
    }

    private void executeMonthly(List<MeterProcessType> meterProcessTypes) {
        try {
            BatchJobQueue nextJob = queueRepository.findFirstByStatusInAndMeterProcessTypeInOrderByRunIdAsc(
                    IN_PROGRESS_STATUS, meterProcessTypes);

            if (nextJob != null) {
                if (Arrays.asList(JobProcess.GEN_INPUT_WS_TA, JobProcess.CALC_TA, JobProcess.CALC_RTA).contains(nextJob.getJobProcess())) {
                    executeParallelRuns(nextJob);
                } else {
                    handleNextJob(nextJob);
                }
            } else {
                log.info("No Jobs to run at the moment.");
            }
        } catch (Exception e) {
            log.error("Exception {} encountered when running meter process types {}. Error: {}", e.getClass(),
                    meterProcessTypes, e.getMessage());
        }
    }

    private void handleNextJob(BatchJobQueue nextJob) {
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
        updateJobStatus(nextJob);
    }


    private void executeParallelRuns(BatchJobQueue nextJob) {
        List<BatchJobQueue> currentlyRunningJobs = queueRepository
                .findByStatusInAndJobProcessAndMeterProcessTypeAndGroupIdAndRegionGroup(Arrays.asList(STARTING, STARTED),
                        nextJob.getJobProcess(),
                        nextJob.getMeterProcessType(),
                        nextJob.getGroupId(),
                        nextJob.getRegionGroup());

        for (BatchJobQueue currentlyRunningJob : currentlyRunningJobs) {
            handleNextJob(currentlyRunningJob);
        }

        if (currentlyRunningJobs.size() < parallelStlMonthlyJob) {
            BatchJobQueue nextQueuedJob =
                    queueRepository.findFirstByStatusAndJobProcessAndMeterProcessTypeAndGroupIdAndRegionGroupOrderByRunIdAsc(
                            ON_QUEUE,
                            nextJob.getJobProcess(),
                            nextJob.getMeterProcessType(),
                            nextJob.getGroupId(),
                            nextJob.getRegionGroup()
                    );

            if (nextQueuedJob != null) {
                handleNextJob(nextQueuedJob);
            }
        }
    }

    private void updateJobStatus(BatchJobQueue nextJob) {

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
                case GEN_GESQ_REPORT:
                case COPY_STL_READY:
                    meterprocessTaskExecutionService.launchJob(taskDto);
                    break;
                case GEN_MTR:
                    mtrTaskExecutionService.launchJob(taskDto);
                    break;
                case GEN_INPUT_WS_TA:
                case CALC_TA:
                case CALC_LR:
                case GEN_MONTHLY_SUMMARY_TA:
                case CALC_GMR_VAT:
                case FINALIZE_TA:
                case FINALIZE_LR:
                case GEN_ENERGY_FILES:
                case GEN_LR_FILES:
                case STL_VALIDATION:
                case GEN_ENERGY_BILLING_STATEMENTS:
                case CALC_ALLOC:
                case GEN_ALLOC_REPORT:
                    tradingAmountsTaskExecutionService.launchJob(taskDto);
                    break;
                case CALC_RTA:
                case GEN_MONTHLY_SUMMARY_RTA:
                case CALC_RGMR_VAT:
                case FINALIZE_RTA:
                case GEN_RESERVE_FILES:
                case GEN_RESERVE_BILLING_STATEMENTS:
                case CALC_ALLOC_RESERVE:
                case GEN_ALLOC_REPORT_RESERVE:
                    reserveTradingAmountsTaskExecutionService.launchJob(taskDto);
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
                case CALC_GMR_VAT_AC:
                case FINALIZE_AC:
                case GEN_FILES_AC:
                case CALC_ALLOC_AC:
                case GEN_ALLOC_REPORT_AC:
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
        Long jobExecutionId = job.getJobExecutionId();
        JobExecutionDto jobExecution = dataFlowJdbcJobExecutionDao.findJobExecutionByJobExecutionId(jobExecutionId);

        String jobExecTimeoutMinutesStr = env.getProperty(JOB_EXECUTION_TIMEOUT_CONFIG + "." + job.getJobProcess().getEnvKey());

        Long jobExecTimeoutMinutes = jobExecTimeoutMinutesStr != null ? Long.valueOf(jobExecTimeoutMinutesStr) : defaultJobExecutionTimeout;

        if (job.getJobExecStart() != null && LocalDateTime.now().isAfter(job.getJobExecStart().plusMinutes(jobExecTimeoutMinutes))) {
            log.error("Job {} with job_execution_id {} has exceeded timeout limit. Failing Job.", job.getJobName(), jobExecutionId);
            job.setStatus(FAILED_RUN);
            job.setDetails("Job has exceeded timeout limit. Please check mesos logs");


            List<Long> unfinishedStepExecutionIds = dataFlowJdbcJobExecutionDao.findUnfinishedStepExecutionIdsByJobExecutionId(jobExecutionId);

            if (!CollectionUtils.isEmpty(unfinishedStepExecutionIds)) {
                log.info("Manually Updating unfinished step execution ids ({}) to FAILED status.", unfinishedStepExecutionIds);
                // after failing unfinished step executions, parent job would detect failed child job and would terminate itself after
                // Killing the parent job manually will not be necessary.
                dataFlowJdbcJobExecutionDao.failUnfinishedStepExecutionsByJobExecutionId(jobExecutionId);
            }

            log.info("Manually Updating Job Execution with id ({}) to FAILED status. Job might need to be manually"
                    + " killed via Chronos API.", jobExecutionId);
            dataFlowJdbcJobExecutionDao.failUnfinishedJobExecutionByJobExecutionId(jobExecutionId);

        } else {
            switch (jobExecution.getBatchStatus()) {
                case COMPLETED:
                    log.info("Job {} is COMPLETED given jobExecutionId {}", job.getJobProcess(), job.getJobExecutionId());
                    job.setStatus(COMPLETED);
                    job.setJobExecEnd(DateUtil.convertToLocalDateTime(jobExecution.getEndTime()));
                    break;
                case FAILED:
                    log.info("Job {} FAILED given jobExecutionId {}", job.getJobProcess(), job.getJobExecutionId());
                    JobExecution jobExecutionEntity = jobExplorer.getJobExecution(jobExecutionId);

                    job.setStatus(FAILED_RUN);
                    job.setJobExecEnd(DateUtil.convertToLocalDateTime(jobExecution.getEndTime()));
                    job.setDetails(processFailedMessage(jobExecutionEntity));
                    break;
                default:
                    // do nothing. job is probably still running
            }
        }
    }

    private String processFailedMessage(JobExecution jobExecution) {
        return jobExecution.getStepExecutions().parallelStream()
                .filter(stepExecution -> stepExecution.getStepName().matches("(.*)Part(.*)"))
                .filter(stepExecution -> stepExecution.getStatus().isUnsuccessful())
                .findFirst().map(stepExecution -> stepExecution.getExitStatus().getExitDescription()).orElse(null);
    }


}
