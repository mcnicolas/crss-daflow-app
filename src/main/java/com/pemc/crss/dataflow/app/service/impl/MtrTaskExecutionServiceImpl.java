package com.pemc.crss.dataflow.app.service.impl;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.pemc.crss.dataflow.app.dto.BaseTaskExecutionDto;
import com.pemc.crss.dataflow.app.dto.MtrTaskExecutionDto;
import com.pemc.crss.dataflow.app.dto.TaskRunDto;
import com.pemc.crss.dataflow.app.dto.parent.GroupTaskExecutionDto;
import com.pemc.crss.dataflow.app.dto.parent.StubTaskExecutionDto;
import com.pemc.crss.dataflow.app.support.PageableRequest;
import com.pemc.crss.shared.commons.util.DateUtil;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobAddtlParams;
import com.pemc.crss.shared.core.dataflow.repository.BatchJobAddtlParamsRepository;
import com.pemc.crss.shared.core.dataflow.repository.ViewMtrIssuanceRepository;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

@Service("mtrTaskExecutionService")
@Transactional(readOnly = true, value = "transactionManager")
public class MtrTaskExecutionServiceImpl extends AbstractTaskExecutionService {

    private static final Logger LOG = LoggerFactory.getLogger(MeterprocessTaskExecutionServiceImpl.class);

    private static final String RUN_MTR_JOB_NAME = "generateMtr";

    @Autowired
    private BatchJobAddtlParamsRepository batchJobAddtlParamsRepository;

    @Autowired
    private ViewMtrIssuanceRepository viewMtrIssuanceRepository;


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

                            Map<String, Object> jobParameters = jobExecution.getJobParameters().getParameters()
                                    .entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                            Long runId = (Long) ((JobParameter) jobParameters.get(RUN_ID)).getValue();
                            BatchJobAddtlParams params = batchJobAddtlParamsRepository.findByRunIdAndKey(runId, SEINS);
                            String seins = params != null ? params.getStringVal() : StringUtils.EMPTY;
                            jobParameters.put("seins", seins);
                            String user = jobParameters.getOrDefault(USERNAME, "").toString();

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
                                mtrTaskExecutionDto.getSummary().put(RUN_MTR_JOB_NAME, showSummary(jobExecution, null));
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
    public Page<GroupTaskExecutionDto> findDistinctBillingPeriodAndProcessType(Pageable pageable) {
        return null;
    }

    @Override
    public Page<? extends StubTaskExecutionDto> findJobInstancesByBillingPeriodAndProcessType(Pageable pageable, String billingPeriod, String processType) {
        return null;
    }

    @Override
    public Page<? extends BaseTaskExecutionDto> findJobInstances(Pageable pageable, String type, String status, String mode,
                                                                 String runStartDate, String tradingStartDate, String tradingEndDate,
                                                                 String username) {
        return null;
    }

    @Override
    public Page<? extends BaseTaskExecutionDto> findJobInstances(PageableRequest pageableRequest) {
        return null;
    }

    @Override
    @Transactional(value = "transactionManager")
    public void launchJob(TaskRunDto taskRunDto) throws URISyntaxException {
        Preconditions.checkNotNull(taskRunDto.getRunId());
        Preconditions.checkNotNull(taskRunDto.getJobName());

        String jobName = null;
        List<String> properties = Lists.newArrayList();
        List<String> arguments = Lists.newArrayList();

        if (RUN_MTR_JOB_NAME.equals(taskRunDto.getJobName())) {

            checkIfIssued(taskRunDto.getMsp(), taskRunDto.getMeterProcessType(), taskRunDto.getTradingDate(), taskRunDto.getStartDate(), taskRunDto.getEndDate());

            if (PROCESS_TYPE_DAILY.equals(taskRunDto.getMeterProcessType())) {
                arguments.add(concatKeyValue(DATE, taskRunDto.getTradingDate(), "date"));
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("dailyMtr")));
            } else {
                arguments.add(concatKeyValue(START_DATE, taskRunDto.getStartDate(), "date"));
                arguments.add(concatKeyValue(END_DATE, taskRunDto.getEndDate(), "date"));
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive("monthlyMtr")));
            }
            String runId = String.valueOf(taskRunDto.getRunId());
            arguments.add(concatKeyValue(METER_TYPE, taskRunDto.getMeterType()));
            arguments.add(concatKeyValue(RUN_ID, runId, "long"));
            arguments.add(concatKeyValue(USERNAME, taskRunDto.getCurrentUser()));
            arguments.add(concatKeyValue(MSP, StringUtils.isNotEmpty(taskRunDto.getMsp()) ? taskRunDto.getMsp() : StringUtils.EMPTY));
            jobName = "crss-meterprocess-task-mtr";
            arguments.add(concatKeyValue(REGION_GROUP, taskRunDto.getRegionGroup()));
            arguments.add(concatKeyValue(REGION, taskRunDto.getRegion()));

            if (StringUtils.isNotEmpty(taskRunDto.getSeins())) {
                BatchJobAddtlParams mtrSeins = new BatchJobAddtlParams();
                mtrSeins.setRunId(Long.valueOf(runId));
                mtrSeins.setType("string");
                mtrSeins.setKey(SEINS);
                mtrSeins.setStringVal(taskRunDto.getSeins());
                saveBatchJobAddtlParamsJdbc(mtrSeins);
            }
        }

        if (jobName != null) {
            LOG.debug("Running job name={}, properties={}, arguments={}", taskRunDto.getJobName(), properties, arguments);
            launchJob(jobName, properties, arguments);
        }

    }

    private void checkIfIssued(String msp, String meterprocessType, String tradingDate, String startDate, String endDate) {
        msp = StringUtils.isNotEmpty(msp) ? msp : StringUtils.EMPTY;
        if (PROCESS_TYPE_DAILY.equalsIgnoreCase(meterprocessType)) {
            String errorMessage = "Cannot run Generate MTR on Trading Day ( %s ). MSP%s already have MTR Issued on that date";
            Preconditions.checkState(
                    StringUtils.isNotEmpty(msp) ?
                    viewMtrIssuanceRepository.countIssuedWithMsp(msp, DateUtil.convertToDate(tradingDate, DateUtil.DEFAULT_DATE_FORMAT)) < 1:
                            viewMtrIssuanceRepository.countIssued(DateUtil.convertToDate(tradingDate, DateUtil.DEFAULT_DATE_FORMAT)) < 1,
                    String.format(errorMessage, tradingDate, StringUtils.isNotEmpty(msp) ? " (" + msp + ")" : msp)
            );
        } else {
            String errorMessage = "Cannot run Generate MTR on Billing Period ( %s ). MSP%s already have MTR Issued on that date";
            Preconditions.checkState(
                    StringUtils.isNotEmpty(msp) ?
                            viewMtrIssuanceRepository.countIssuedWithMsp(msp, DateUtil.convertToDate(startDate, DateUtil.DEFAULT_DATE_FORMAT),
                                    DateUtil.convertToDate(endDate, DateUtil.DEFAULT_DATE_FORMAT)) < 1 :
                            viewMtrIssuanceRepository.countIssued( DateUtil.convertToDate(startDate, DateUtil.DEFAULT_DATE_FORMAT),
                                    DateUtil.convertToDate(endDate, DateUtil.DEFAULT_DATE_FORMAT)) < 1,
                    String.format(errorMessage, startDate + (endDate != null ? " / " + endDate : ""),
                            StringUtils.isNotEmpty(msp) ? " (" + msp + ")" : msp)
            );
        }

    }

    @Override
    public void relaunchFailedJob(long jobId) throws URISyntaxException {

    }


}
