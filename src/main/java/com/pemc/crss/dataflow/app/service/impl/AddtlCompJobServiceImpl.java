package com.pemc.crss.dataflow.app.service.impl;

import com.google.common.collect.Sets;
import com.pemc.crss.dataflow.app.dto.TaskRunDto;
import com.pemc.crss.dataflow.app.service.AddtlCompJobService;
import com.pemc.crss.shared.commons.util.DateUtil;
import com.pemc.crss.shared.core.dataflow.entity.AddtlCompParams;
import com.pemc.crss.shared.core.dataflow.repository.AddtlCompParamsRepository;
import com.pemc.crss.shared.core.dataflow.repository.ExecutionParamRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.repository.dao.JobExecutionDao;
import org.springframework.batch.core.repository.dao.StepExecutionDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.stream.Collectors;

import static com.pemc.crss.shared.commons.util.TaskUtil.END_DATE;
import static com.pemc.crss.shared.commons.util.TaskUtil.START_DATE;
import static com.pemc.crss.shared.core.dataflow.reference.SettlementJobName.GEN_EBRSV_INPUT_WS;

@Slf4j
@Service
public class AddtlCompJobServiceImpl implements AddtlCompJobService {

    @Autowired
    protected ExecutionParamRepository executionParamRepository;

    @Autowired
    private AddtlCompParamsRepository addtlCompParamsRepository;

    @Autowired
    private DataFlowJdbcJobExecutionDao dataFlowJdbcJobExecutionDao;

    @Autowired
    protected JobExecutionDao jobExecutionDao;

    @Autowired
    protected StepExecutionDao stepExecutionDao;

    @Override
    @Transactional(value = "transactionManager")
    public void deleteJob(long runId, String mtn, String pricingCondition, String groupId) {
        //for addtlcomp, Job ID in UI is the runId job params.

        Long jobId = executionParamRepository.findJobIdByRunId(runId);
        AddtlCompParams acParams = addtlCompParamsRepository.findTopByMtnAndPricingConditionAndGroupIdOrderByBillingStartDateDesc(mtn, pricingCondition, groupId);

        if (jobId != null && acParams != null) {
            addtlCompParamsRepository.delete(acParams);
            executionParamRepository.deleteCascadeJob(jobId);
        }
    }

    @Override
    public Boolean validateBillingPeriod(TaskRunDto taskRunDto) {
        //get dates
        String startDate = taskRunDto.getStartDate();
        String endDate = taskRunDto.getEndDate();
        //prev bp dates
        LocalDate sDate = DateUtil.parseLocalDate(startDate).minusMonths(1);
        LocalDate eDate = DateUtil.parseLocalDate(endDate).minusMonths(1);

        //just for FINAL
        List<JobInstance> jobInstances = dataFlowJdbcJobExecutionDao.findJobInstancesByNameAndBillingPeriod(GEN_EBRSV_INPUT_WS,
                derivePrevBillingPeriod(sDate, eDate));

        return determineIWSCompleteness(jobInstances, sDate, eDate);
    }

    private Boolean determineIWSCompleteness(List<JobInstance> jobInstances, LocalDate sDate, LocalDate eDate) {
        Set<LocalDate> startDateSet = Sets.newHashSet();

        for (JobInstance genWsStlJobInstance : jobInstances) {
            JobExecution genWsJobExec = getJobExecutionFromJobInstance(genWsStlJobInstance);

            JobParameters genInputWsJobParameters = genWsJobExec.getJobParameters();
            Date genInputWsStartDate = genInputWsJobParameters.getDate(START_DATE);
            Date genInputWsEndDate = genInputWsJobParameters.getDate(END_DATE);
            log.info("genInputWsStartDate = {}, genInputWsEndDate = {}", genInputWsStartDate, genInputWsEndDate);

            startDateSet.add(DateUtil.convertToLocalDate(genInputWsStartDate));
        }

        SortedSet<LocalDate> referencebillingPeriodDates = DateUtil.createRange(sDate, eDate);
        SortedSet<LocalDate> billingPeriodDates = DateUtil.createRange(sDate, eDate);
        referencebillingPeriodDates.forEach(billingPeriodDate -> {
            if (startDateSet.contains(billingPeriodDate)) {
                billingPeriodDates.remove(billingPeriodDate);
            }
        });

        log.info("billingPeriodDates = {}", billingPeriodDates.stream().map(o -> o.toString())
                .collect(Collectors.joining(",")));

        return billingPeriodDates.isEmpty();
    }

    private String derivePrevBillingPeriod(LocalDate startDate, LocalDate endDate) {

        String prevBp = DateUtil.convertToString(startDate, DateUtil.REPORT_FILENAME_FORMAT).substring(2)
                + DateUtil.convertToString(endDate, DateUtil.REPORT_FILENAME_FORMAT).substring(2);
        log.info("prevBp = {}", prevBp);
        return prevBp;
    }

    private JobExecution getJobExecutionFromJobInstance(final JobInstance jobInstance) {
        List<JobExecution> jobExecutions = getJobExecutionsNoStepContext(jobInstance);

        // most of the time one jobInstance contains only one jobExecution.
        if (jobExecutions.size() > 1) {
            log.info("Found multiple job executions for JobInstance with id: {}", jobInstance.getInstanceId());
        }

        // by default jobExecutions are ordered by job_execution_id desc. We need to only get the first instance
        // since spring batch does not allow repeated job executions if the previous job execution is COMPLETED
        return jobExecutions.isEmpty() ? null : jobExecutions.get(0);
    }

    private List<JobExecution> getJobExecutionsNoStepContext(JobInstance jobInstance) {
        List<JobExecution> executions = jobExecutionDao.findJobExecutions(jobInstance);
        for (JobExecution jobExecution : executions) {
            getJobExecutionDependencies(jobExecution, jobInstance);
        }
        return executions;
    }

    private void getJobExecutionDependencies(JobExecution jobExecution, JobInstance jobInstance) {
        stepExecutionDao.addStepExecutions(jobExecution);
        jobExecution.setJobInstance(jobInstance);
    }

}
