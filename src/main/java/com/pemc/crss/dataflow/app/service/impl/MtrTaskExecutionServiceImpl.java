package com.pemc.crss.dataflow.app.service.impl;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.pemc.crss.dataflow.app.dto.BaseTaskExecutionDto;
import com.pemc.crss.dataflow.app.dto.MtrTaskExecutionDto;
import com.pemc.crss.dataflow.app.dto.TaskRunDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.stream.Collectors.toList;

@Service("mtrTaskExecutionService")
@Transactional(readOnly = true, value = "transactionManager")
public class MtrTaskExecutionServiceImpl extends AbstractTaskExecutionService {

    private static final Logger LOG = LoggerFactory.getLogger(MeterprocessTaskExecutionServiceImpl.class);

    private static final String RUN_MTR_JOB_NAME = "generateMtr";

    @Override
    public Page<MtrTaskExecutionDto> findJobInstances(Pageable pageable) {
        int count = 0;

        try {
            count = jobExplorer.getJobInstanceCount(RUN_MTR_JOB_NAME.concat("Daily"));
            count += jobExplorer.getJobInstanceCount(RUN_MTR_JOB_NAME.concat("Monthly"));
        } catch (NoSuchJobException e) {
            LOG.error("Exception: " + e);
        }

        List<MtrTaskExecutionDto> mtrTaskExecutionDtos = Lists.newArrayList();

        if (count > 0) {
            mtrTaskExecutionDtos = jobExplorer.findJobInstancesByJobName(RUN_MTR_JOB_NAME.concat("*"),
                    pageable.getOffset(), pageable.getPageSize()).stream()
                    .map((JobInstance jobInstance) -> {

                        if (getJobExecutions(jobInstance).iterator().hasNext()) {
                            JobExecution jobExecution = getJobExecutions(jobInstance).iterator().next();

                            Map jobParameters = Maps.transformValues(jobExecution.getJobParameters().getParameters(), JobParameter::getValue);
                            String user = (String) jobParameters.getOrDefault(USERNAME, "");

                            MtrTaskExecutionDto mtrTaskExecutionDto = new MtrTaskExecutionDto();
                            mtrTaskExecutionDto.setId(jobInstance.getId());
                            mtrTaskExecutionDto.setRunDateTime(jobExecution.getStartTime());
                            mtrTaskExecutionDto.setParams(jobParameters);
                            mtrTaskExecutionDto.setStatus(jobExecution.getStatus().toString());
                            mtrTaskExecutionDto.setUser(user);


                            if (jobExecution.getStatus().isRunning()) {
                                calculateProgress(jobExecution, mtrTaskExecutionDto);
                            } else if (jobExecution.getStatus().isUnsuccessful()) {
                                mtrTaskExecutionDto.setExitMessage(processFailedMessage(jobExecution));
                            } else if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
                                mtrTaskExecutionDto.getSummary().put(RUN_MTR_JOB_NAME, showSummary(jobExecution));
                            }
                            return mtrTaskExecutionDto;
                        } else {
                            return null;
                        }
                    })
                    .filter(Objects::nonNull)
                    .collect(toList());
        }
        return new PageImpl<>(mtrTaskExecutionDtos, pageable, count);
    }

    @Override
    public Page<? extends BaseTaskExecutionDto> findJobInstances(Pageable pageable, String type, String status, String mode,
                                                                 String runStartDate, String tradingStartDate, String tradingEndDate,
                                                                 String username) {
        return null;
    }

    @Override
    @Transactional(value = "transactionManager")
    public void launchJob(TaskRunDto taskRunDto) throws URISyntaxException {
        Preconditions.checkNotNull(taskRunDto.getJobName());

        String jobName = null;
        List<String> properties = Lists.newArrayList();
        List<String> arguments = Lists.newArrayList();

        if (RUN_MTR_JOB_NAME.equals(taskRunDto.getJobName())) {
            arguments.add(concatKeyValue(METER_TYPE, taskRunDto.getMeterType(), ""));
            if (PROCESS_TYPE_DAILY.equals(taskRunDto.getMeterProcessType())) {
                arguments.add(concatKeyValue(DATE, taskRunDto.getTradingDate(), "date"));
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("dailyMtr")));
            } else {
                arguments.add(concatKeyValue(START_DATE, taskRunDto.getStartDate(), "date"));
                arguments.add(concatKeyValue(END_DATE, taskRunDto.getEndDate(), "date"));
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyMtr")));
            }
            arguments.add(concatKeyValue(RUN_ID, String.valueOf(System.currentTimeMillis()), "long"));
            arguments.add(concatKeyValue(USERNAME, taskRunDto.getCurrentUser()));
            jobName = "crss-meterprocess-task-mtr";
        }

        if (jobName != null) {
            LOG.debug("Running job name={}, properties={}, arguments={}", taskRunDto.getJobName(), properties, arguments);
            launchJob(jobName, properties, arguments);
        }

    }

    @Override
    public void relaunchFailedJob(long jobId) throws URISyntaxException {

    }


}
