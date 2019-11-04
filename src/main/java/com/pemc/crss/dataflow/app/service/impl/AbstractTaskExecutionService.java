package com.pemc.crss.dataflow.app.service.impl;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.pemc.crss.dataflow.app.dto.BaseTaskExecutionDto;
import com.pemc.crss.dataflow.app.dto.TaskProgressDto;
import com.pemc.crss.dataflow.app.dto.TaskRunDto;
import com.pemc.crss.dataflow.app.dto.TaskSummaryDto;
import com.pemc.crss.dataflow.app.service.TaskExecutionService;
import com.pemc.crss.dataflow.app.support.PageableRequest;
import com.pemc.crss.shared.commons.util.DateUtil;
import com.pemc.crss.shared.core.dataflow.entity.AddtlCompParams;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobAddtlParams;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobAdjRun;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobRunLock;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobSkipLog;
import com.pemc.crss.shared.core.dataflow.entity.QBatchJobSkipLog;
import com.pemc.crss.shared.core.dataflow.repository.BatchJobRunLockRepository;
import com.pemc.crss.shared.core.dataflow.repository.BatchJobSkipLogRepository;
import com.pemc.crss.shared.core.dataflow.repository.ExecutionParamRepository;
import com.querydsl.core.BooleanBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.repository.dao.ExecutionContextDao;
import org.springframework.batch.core.repository.dao.JobExecutionDao;
import org.springframework.batch.core.repository.dao.JobInstanceDao;
import org.springframework.batch.core.repository.dao.StepExecutionDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.hateoas.ResourceSupport;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestTemplate;

import java.net.URI;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;

/**
 * Created by jdimayuga on 03/03/2017.
 */
@Slf4j
public abstract class AbstractTaskExecutionService implements TaskExecutionService {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractTaskExecutionService.class);

    protected static final String RUN_ID = "run.id";
    protected static final String SPRING_PROFILES_ACTIVE = "spring.profiles.active";
    protected static final String DATE = "date";
    protected static final String START_DATE = "startDate";
    protected static final String END_DATE = "endDate";
    protected static final String PROCESS_TYPE_DAILY = "DAILY";
    protected static final String GROUP_ID = "groupId";
    protected static final String PARENT_JOB = "parentJob";
    protected static final String METER_TYPE = "meterType";
    protected static final String PROCESS_TYPE = "processType";
    protected static final String ADJUSTMENT_NO = "adjNo";
    protected DateFormat dateFormat = new SimpleDateFormat(DateUtil.DEFAULT_DATE_FORMAT);
    protected static final String USERNAME = "username";
    protected static final String WESM_USERNAME = "wesmUsername";
    protected static final String STL_READY_USERNAME = "stlReadyUsername";
    protected static final String STL_NOT_READY_USERNAME = "stlNotReadyUsername";
    protected static final String MQ_REPORT_USERNAME = "mqReportUsername";
    protected static final String RCOA_USERNAME = "rcoaUsername";
    protected static final String QUOTE = "\"";
    protected static final String PARAMS_TYPE_STRING = "string";
    protected static final String PARAMS_TYPE_LONG = "long";
    protected static final String PARAMS_TYPE_DATE = "date";
    protected static final String PROFILE_DAILY_MQ= "dailyMq";
    protected static final String PROFILE_MONTHLY_MQ= "monthlyMq";
    protected static final String PROFILE_MONTHLY_PRELIM= "monthlyPrelim";
    protected static final String PROFILE_MONTHLY_FINAL= "monthlyFinal";
    protected static final String PROFILE_MONTHLY_ADJUSTED= "monthlyAdjusted";
    protected static final String PROFILE_DAILY_MQ_REPORT= "dailyMqReport";
    protected static final String PROFILE_MONTHLY_MQ_REPORT= "monthlyMqReport";
    protected static final String PROFILE_STL_READY_DAILY = "finalizeDaily";
    protected static final String PROFILE_STL_READY_MONTHLY_PRELIM = "finalizeMonthlyPrelim";
    protected static final String PROFILE_STL_READY_MONTHLY_FINAL = "finalizeMonthlyFinal";
    protected static final String PROFILE_STL_READY_MONTHLY_ADJUSTED = "finalizeMonthlyAdjusted";
    protected static final String METER_TYPE_WESM= "MIRF_MT_WESM";
    protected static final String METER_TYPE_RCOA= "MIRF_MT_RCOA";
    protected static final String MSP = "msp";
    protected static final String MTNS = "mtns";
    protected static final String SEINS = "seins";
    protected static final String REGION_GROUP = "regionGroup";
    protected static final String RG = "rg";
    protected static final String REGION = "region";
    protected static final String ALL = "ALL";

    @Autowired
    protected BatchJobSkipLogRepository batchJobSkipLogRepository;
    @Autowired
    protected ExecutionParamRepository executionParamRepository;
    @Autowired
    protected JobExplorer jobExplorer;
    @Autowired
    protected JobInstanceDao jobInstanceDao;
    @Autowired
    protected JobExecutionDao jobExecutionDao;
    @Autowired
    protected StepExecutionDao stepExecutionDao;
    @Autowired
    protected ExecutionContextDao ecDao;
    @Autowired
    protected RestTemplate restTemplate;
    @Autowired
    protected BatchJobRunLockRepository batchJobRunLockRepository;
    @Autowired
    protected Environment environment;
    @Autowired
    protected RedisTemplate<String, Long> redisTemplate;
    @Autowired
    protected RedisTemplate genericRedisTemplate;
    @Autowired
    protected DataFlowJdbcJobExecutionDao dataFlowJdbcJobExecutionDao;

    @Autowired
    protected NamedParameterJdbcTemplate dataflowJdbcTemplate;

    @Value("${dataflow.url}")
    protected String dataFlowUrl;

    @Value("${crss.dispatch.interval}")
    private int dispatchInterval;

    @Value("${todi-config.max-retry}")
    protected int maxRetry;


    @Override
    public abstract Page<? extends BaseTaskExecutionDto> findJobInstances(Pageable pageable);

    @Override
    public abstract void launchJob(TaskRunDto taskRunDto) throws URISyntaxException;

    @Override
    public int getDispatchInterval() {
        return this.dispatchInterval;
    }


    @Override
    @Transactional(value = "transactionManager")
    public void deleteJob(long jobId) {
        executionParamRepository.deleteCascadeJob(jobId);
    }


    @Override
    public Page<BatchJobSkipLog> getBatchJobSkipLogs(Pageable pageable, int stepId) {
        List<BatchJobSkipLog> skipLogs = Lists.newArrayList();
        int count = 0;

        count += executionParamRepository.getSkipLogsCount(stepId);

        if (count > 0) {
            skipLogs = executionParamRepository.getBatchJobSkipLogs(pageable.getOffset(), pageable.getPageSize(), stepId);
        }

        return new PageImpl<>(skipLogs, pageable, count);
    }

    @Override
    public Page<BatchJobSkipLog> getBatchJobSkipLogs(final PageableRequest pageableRequest) {
        String parentStepName = pageableRequest.getMapParams().getOrDefault("parentStepName", "");
        Long jobExecutionId = Long.valueOf(pageableRequest.getMapParams().getOrDefault("jobExecutionId", "0"));

        BooleanBuilder predicate = new BooleanBuilder();
        predicate.and(QBatchJobSkipLog.batchJobSkipLog.jobExecutionId.eq(jobExecutionId)
        .and(QBatchJobSkipLog.batchJobSkipLog.parentStepName.eq(parentStepName)));

        return batchJobSkipLogRepository.findAll(predicate, pageableRequest.getPageable());
    }

    @Override
    public String getFailedExitMsg(int stepId) {
        return executionParamRepository.getFailedExitMsg(stepId);
    }

    protected String convertStatus(BatchStatus batchStatus, String suffix) {
        return batchStatus.toString().concat("-").concat(suffix);
    }

    protected TaskProgressDto processStepProgress(StepExecution runningStep, String stepStr) {
        TaskProgressDto progressDto = new TaskProgressDto();
        progressDto.setRunningStep(stepStr);
        Long stepProg = redisTemplate.opsForValue().get(String.valueOf(runningStep.getId()));
        if (stepProg != null) {
            progressDto.setTotalCount(redisTemplate.opsForValue().get(runningStep.getId() + "_total"));
            progressDto.setExecutedCount(Math.min(stepProg,
                    progressDto.getTotalCount()));
        }
        return progressDto;
    }

    protected List<JobExecution> getJobExecutions(JobInstance jobInstance, String status, String mode,
                                                  String runStartDate, String tradingStartDate,
                                                  String tradingEndDate, String username) {
        List<JobExecution> executions = dataFlowJdbcJobExecutionDao.findJobExecutions(jobInstance,
                status, mode, runStartDate, tradingStartDate, tradingEndDate, username);
        for (JobExecution jobExecution : executions) {
            getJobExecutionDependencies(jobExecution);
            for (StepExecution stepExecution : jobExecution.getStepExecutions()) {
                getStepExecutionDependencies(stepExecution);
            }
        }
        return executions;
    }

    protected List<JobExecution> getJobExecutions(JobInstance jobInstance) {
        List<JobExecution> executions = jobExecutionDao.findJobExecutions(jobInstance);
        for (JobExecution jobExecution : executions) {
            getJobExecutionDependencies(jobExecution);
            for (StepExecution stepExecution : jobExecution.getStepExecutions()) {
                getStepExecutionDependencies(stepExecution);
            }
        }
        return executions;
    }

    protected void getJobExecutionDependencies(JobExecution jobExecution) {
        JobInstance jobInstance = jobInstanceDao.getJobInstance(jobExecution);
        stepExecutionDao.addStepExecutions(jobExecution);
        jobExecution.setJobInstance(jobInstance);
        jobExecution.setExecutionContext(ecDao.getExecutionContext(jobExecution));
    }

    protected void getStepExecutionDependencies(StepExecution stepExecution) {
        if (stepExecution != null && stepExecution.getStepName().endsWith("Step")) {
            stepExecution.setExecutionContext(ecDao.getExecutionContext(stepExecution));
        }
    }

    protected List<TaskSummaryDto> showSummary(JobExecution jobExecution, List<String> filterList) {
        return jobExecution.getStepExecutions().parallelStream()
                .filter(stepExecution -> filterList == null || filterList.isEmpty()
                    ? stepExecution.getStepName().endsWith("Step")
                    : filterList.contains(stepExecution.getStepName()))
                .map(stepExecution -> {
                    TaskSummaryDto taskSummaryDto = new TaskSummaryDto();
                    taskSummaryDto.setStepCode(stepExecution.getStepName());
                    taskSummaryDto.setStepName(stepExecution.getStepName());
                    taskSummaryDto.setReadCount(stepExecution.getReadCount());
                    taskSummaryDto.setWriteCount(stepExecution.getWriteCount());
                    taskSummaryDto.setSkipCount(stepExecution.getSkipCount());
                    taskSummaryDto.setStepId(stepExecution.getId());
                    taskSummaryDto.setJobExecutionId(stepExecution.getJobExecutionId());

                    return taskSummaryDto;
                })
                .sorted(comparing(TaskSummaryDto::getStepId))
                .collect(toList());
    }

    protected List<TaskSummaryDto> showSummaryWithLabel(JobExecution jobExecution, Map<String, String> stepWithLabelMap) {
        return jobExecution.getStepExecutions().parallelStream()
                .filter(stepExecution -> stepWithLabelMap.containsKey(stepExecution.getStepName()))
                .map(stepExecution -> {
                    TaskSummaryDto taskSummaryDto = new TaskSummaryDto();
                    taskSummaryDto.setStepCode(stepExecution.getStepName());
                    taskSummaryDto.setStepName(stepWithLabelMap.get(stepExecution.getStepName()));
                    taskSummaryDto.setReadCount(stepExecution.getReadCount());
                    taskSummaryDto.setWriteCount(stepExecution.getWriteCount());
                    taskSummaryDto.setSkipCount(stepExecution.getSkipCount());
                    taskSummaryDto.setStepId(stepExecution.getId());
                    taskSummaryDto.setJobExecutionId(stepExecution.getJobExecutionId());

                    return taskSummaryDto;
                })
                .sorted(comparing(TaskSummaryDto::getStepId))
                .collect(toList());
    }


    protected String fetchSpringProfilesActive(String profile) {
        List<String> profiles = Lists.newArrayList(environment.getActiveProfiles());
        profiles.add(profile);
        return StringUtils.join(profiles, ",");
    }

    protected String concatKeyValue(String key, String value, String dataType) {
        return key.concat(dataType != null ? "(".concat(dataType).concat(")") : "").concat("=").concat(value != null ? value : "");
    }

    protected String concatKeyValue(String key, String value) {
        return concatKeyValue(key, value, null);
    }

    protected String processFailedMessage(JobExecution jobExecution) {
        return jobExecution.getStepExecutions().parallelStream()
                .filter(stepExecution -> stepExecution.getStepName().matches("(.*)StepPartition(.*)"))
                .filter(stepExecution -> stepExecution.getStatus().isUnsuccessful())
                .findFirst().map(stepExecution -> stepExecution.getExitStatus().getExitDescription()).orElse(null);
    }

    protected void calculateProgress(JobExecution jobExecution, BaseTaskExecutionDto taskExecutionDto) {
        TaskProgressDto progressDto = null;
        if (!jobExecution.getStepExecutions().isEmpty()) {
            Optional<StepExecution> stepOpt = jobExecution.getStepExecutions().parallelStream()
                    .filter(stepExecution -> stepExecution.getStatus().isRunning())
                    .filter(stepExecution -> stepExecution.getStepName().endsWith("Step"))
                    .findFirst();
            if (stepOpt.isPresent()) {
                StepExecution runningStep = stepOpt.get();
                LOGGER.debug("RUNNING STEP NAME: {}, ID: {}", runningStep.getStepName(), runningStep.getId());
                if (runningStep.getStepName().equals("processGapStep")) {
                    progressDto = processStepProgress(runningStep, "Generate gap records");
                } else if (runningStep.getStepName().equals("computeMqStep")) {
                    progressDto = processStepProgress(runningStep, "Generate raw mq data");
                } else if (runningStep.getStepName().equals("applySSLAStep")) {
                    progressDto = processStepProgress(runningStep, "Applying SSLA Computation");
                } else if (runningStep.getStepName().equals("generateReportStep")) {
                    progressDto = processStepProgress(runningStep, "Generate Report");
                } else if (runningStep.getStepName().equals("processStlReadyStep")) {
                    progressDto = processStepProgress(runningStep, "Process GESQ");
                } else if (runningStep.getStepName().equals("finalStlReadyStep")) {
                    progressDto = processStepProgress(runningStep, "Finalize STL Ready");
                } else if (runningStep.getStepName().equals("generateMtrStep")) {
                    progressDto = processStepProgress(runningStep, "Generate MTR");
                }
            }
        }
        taskExecutionDto.setProgress(progressDto);
    }

    protected void launchJob(String jobName, List<String> properties, List<String> arguments) throws URISyntaxException {
        ResourceSupport resourceSupport = restTemplate.getForObject(new URI(dataFlowUrl), ResourceSupport.class);
        restTemplate.postForObject(resourceSupport.getLink("tasks/deployments/deployment").expand(jobName).getHref().concat(
                "?arguments={arguments}&properties={properties}"), null, Object.class, ImmutableMap.of("arguments", StringUtils.join(arguments, ","),
                "properties", StringUtils.join(properties, ",")));
    }

    protected void lockJob(TaskRunDto taskRunDto) {
        if (batchJobRunLockRepository.lockJob(taskRunDto.getJobName()) == 0) {
            BatchJobRunLock batchJobRunLock = new BatchJobRunLock();
            batchJobRunLock.setJobName(taskRunDto.getJobName());
            batchJobRunLock.setLocked(true);
            batchJobRunLock.setLockedDate(new Date());
            batchJobRunLockRepository.save(batchJobRunLock);
        }
    }

    protected void lockJobJdbc(final TaskRunDto taskRunDto) {
        MapSqlParameterSource paramSource = new MapSqlParameterSource()
                .addValue("jobName", taskRunDto.getJobName());
        String updateSql = "update batch_job_run_lock set locked = true, locked_date = now() where job_name = :jobName";

        if (dataflowJdbcTemplate.update(updateSql, paramSource) == 0) {
            log.info("Inserting new job lock with name {}", taskRunDto.getJobName());
            String insertSql = "insert into batch_job_run_lock(id, locked, job_name, locked_date, created_datetime) "
                    + " values (nextval('hibernate_sequence'), true, :jobName, now(), now())";

            dataflowJdbcTemplate.update(insertSql, paramSource);
        }
    }

    protected void saveBatchJobAdjRun(final BatchJobAdjRun batchJobAdjRun) {
        MapSqlParameterSource paramSource = new MapSqlParameterSource()
                .addValue("addtlComp", batchJobAdjRun.isAdditionalCompensation() ? "Y" : "N")
                .addValue("jobId", batchJobAdjRun.getJobId())
                .addValue("groupId", batchJobAdjRun.getGroupId())
                .addValue("meterProcessType", batchJobAdjRun.getMeterProcessType().name())
                .addValue("billingPeriodStart", DateUtil.convertToDate(batchJobAdjRun.getBillingPeriodStart()))
                .addValue("billingPeriodEnd", DateUtil.convertToDate(batchJobAdjRun.getBillingPeriodEnd()))
                .addValue("outputReady", batchJobAdjRun.isOutputReady() ? "Y" : "N");

        String insertSql = "insert into batch_job_adj_run(id, created_datetime, addtl_comp, job_id, group_id, meter_process_type, "
                + " billing_period_start, billing_period_end, output_ready) values (nextval('hibernate_sequence'), now(), "
                + " :addtlComp, :jobId, :groupId, :meterProcessType, :billingPeriodStart, :billingPeriodEnd, :outputReady)";

        dataflowJdbcTemplate.update(insertSql, paramSource);
    }

    protected void saveAddtlCompParamJdbc(AddtlCompParams addtlCompParams) {
        MapSqlParameterSource paramSource = new MapSqlParameterSource()
                .addValue("billingStartDate", DateUtil.convertToDate(addtlCompParams.getBillingStartDate()))
                .addValue("billingEndDate", DateUtil.convertToDate(addtlCompParams.getBillingEndDate()))
                .addValue("pricingCondition", addtlCompParams.getPricingCondition())
                .addValue("billingId", addtlCompParams.getBillingId())
                .addValue("mtn", addtlCompParams.getMtn())
                .addValue("approvedRate", addtlCompParams.getApprovedRate())
                .addValue("groupId", addtlCompParams.getGroupId())
                .addValue("status", addtlCompParams.getStatus());

        String insertSql = "insert into addtl_comp_params(id, billing_start_date, billing_end_date, pricing_condition, "
                + " billing_id, mtn, approved_rate, group_id, status) values (nextval('hibernate_sequence'), "
                + " :billingStartDate, :billingEndDate, :pricingCondition, :billingId, :mtn, :approvedRate, :groupId, :status)";

        dataflowJdbcTemplate.update(insertSql, paramSource);
    }

    protected void saveBatchJobAddtlParamsJdbc(final BatchJobAddtlParams addtlParams) {
        MapSqlParameterSource paramSource = new MapSqlParameterSource()
                .addValue("runId", addtlParams.getRunId())
                .addValue("typeCd", addtlParams.getType())
                .addValue("keyName", addtlParams.getKey())
                .addValue("stringVal", addtlParams.getStringVal())
                .addValue("longVal", addtlParams.getLongVal())
                .addValue("dateVal", DateUtil.convertToDate(addtlParams.getDateVal()))
                .addValue("doubleVal", addtlParams.getDoubleVal());

        String insertSql = "insert into batch_job_addtl_params(id, created_datetime, run_id, type_cd, key_name, "
                + " string_val, long_val, date_val, double_val) values (nextval('hibernate_sequence'), now(), "
                + " :runId, :typeCd, :keyName, :stringVal, :longVal, :dateVal, :doubleVal)";

        dataflowJdbcTemplate.update(insertSql, paramSource);

    }
}
