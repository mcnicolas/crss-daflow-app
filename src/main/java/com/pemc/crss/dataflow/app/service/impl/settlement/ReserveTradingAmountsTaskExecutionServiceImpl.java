package com.pemc.crss.dataflow.app.service.impl.settlement;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.pemc.crss.dataflow.app.dto.JobCalculationDto;
import com.pemc.crss.dataflow.app.dto.SettlementTaskExecutionDto;
import com.pemc.crss.dataflow.app.dto.StlJobGroupDto;
import com.pemc.crss.dataflow.app.dto.TaskRunDto;
import com.pemc.crss.dataflow.app.dto.parent.GroupTaskExecutionDto;
import com.pemc.crss.dataflow.app.dto.parent.StubTaskExecutionDto;
import com.pemc.crss.dataflow.app.service.StlReadyJobQueryService;
import com.pemc.crss.dataflow.app.support.PageableRequest;
import com.pemc.crss.dataflow.app.support.StlJobStage;
import com.pemc.crss.shared.commons.reference.MeterProcessType;
import com.pemc.crss.shared.commons.reference.SettlementStepUtil;
import com.pemc.crss.shared.commons.util.DateUtil;
import com.pemc.crss.shared.core.dataflow.dto.DistinctStlReadyJob;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobAddtlParams;
import com.pemc.crss.shared.core.dataflow.entity.ViewSettlementJob;
import com.pemc.crss.shared.core.dataflow.reference.SettlementJobProfile;
import com.pemc.crss.shared.core.dataflow.reference.StlCalculationType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameters;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import javax.transaction.Transactional;
import java.net.URISyntaxException;
import java.time.LocalDateTime;
import java.util.*;

import static com.pemc.crss.dataflow.app.support.StlJobStage.GENERATE_IWS;
import static com.pemc.crss.shared.commons.reference.MeterProcessType.*;
import static com.pemc.crss.shared.commons.reference.MeterProcessType.DAILY;
import static com.pemc.crss.shared.commons.reference.SettlementStepUtil.*;
import static com.pemc.crss.shared.core.dataflow.reference.SettlementJobName.*;

@Slf4j
@Service("reserveTradingAmountsTaskExecutionService")
@Transactional
public class ReserveTradingAmountsTaskExecutionServiceImpl extends StlTaskExecutionServiceImpl {

    private static final Map<String, String> STL_GMR_CALC_STEP_WITH_SKIP_LOGS =
            Collections.singletonMap(SettlementStepUtil.CALC_GMR_RESERVE_VAT, "Calculate GMR / VAT");

    @Autowired
    private StlReadyJobQueryService stlReadyJobQueryService;

    @Override
    public Page<? extends StubTaskExecutionDto> findJobInstances(PageableRequest pageableRequest) {

        Page<DistinctStlReadyJob> stlReadyJobs = stlReadyJobQueryService.findDistinctStlReadyJobsForTradingAmounts(pageableRequest);
        List<DistinctStlReadyJob> distinctStlReadyJobs = stlReadyJobs.getContent();

        List<SettlementTaskExecutionDto> taskExecutionDtos = new ArrayList<>();

        for (DistinctStlReadyJob stlReadyJob : distinctStlReadyJobs) {

            String parentIdStr = stlReadyJob.getParentId();

            final Long parentId = Long.valueOf(parentIdStr);
            final String regionGroup = stlReadyJob.getRegionGroup();

            SettlementTaskExecutionDto taskExecutionDto = initializeTaskExecutionDto(stlReadyJob, parentIdStr);
            String stlReadyGroupId = taskExecutionDto.getStlReadyGroupId();

            StlJobGroupDto initialJobGroupDto = new StlJobGroupDto();
            initialJobGroupDto.setGroupId(stlReadyGroupId);
            initialJobGroupDto.setHeader(true);

            taskExecutionDto.setParentStlJobGroupDto(initialJobGroupDto);

            Map<String, StlJobGroupDto> stlJobGroupDtoMap = new HashMap<>();
            stlJobGroupDtoMap.put(stlReadyGroupId, initialJobGroupDto);

            final MeterProcessType processType = taskExecutionDto.getProcessType();

            /* GENERATE INPUT WORKSPACE START */
            List<JobInstance> generateInputWsJobInstances = findJobInstancesByNameAndProcessTypeAndParentIdAndRegionGroup(
                    GEN_RSV_INPUT_WS, processType, parentId, regionGroup);

            initializeGenInputWorkSpace(generateInputWsJobInstances, stlJobGroupDtoMap, taskExecutionDto, stlReadyGroupId);

            /* RESERVE SETTLEMENT CALCULATION START */
            List<JobInstance> calculationJobInstances = findJobInstancesByNameAndProcessTypeAndParentIdAndRegionGroup(
                    CALC_RSV_STL, processType, parentId, regionGroup);

            initializeStlCalculation(calculationJobInstances, stlJobGroupDtoMap, taskExecutionDto, stlReadyGroupId);

            /* GENERATE MONTHLY SUMMARY START */
            List<JobInstance> genMonthlySummaryJobInstances = findJobInstancesByNameAndProcessTypeAndParentIdAndRegionGroup(
                    GEN_MONTHLY_SUMMARY_RTA, processType, parentId, regionGroup);

            initializeGenMonthlySummary(genMonthlySummaryJobInstances, stlJobGroupDtoMap, taskExecutionDto, stlReadyGroupId);

            /* CALCULATE GMR START */
            List<JobInstance> calculateRgmrJobInstances = findJobInstancesByNameAndProcessTypeAndParentIdAndRegionGroup(
                    CALC_RGMR, processType, parentId, regionGroup);

            initializeCalculateRgmr(calculateRgmrJobInstances, stlJobGroupDtoMap, taskExecutionDto, stlReadyGroupId);

            /* FINALIZE START */
            List<JobInstance> taggingJobInstances = findJobInstancesByNameAndProcessTypeAndParentIdAndRegionGroup(
                    TAG_RTA, processType, parentId, regionGroup);

            initializeTagging(taggingJobInstances, stlJobGroupDtoMap, taskExecutionDto, stlReadyGroupId);

            /* GEN FILES RESERVE TA START */
            List<JobInstance> genFileReserveTaJobInstances = findJobInstancesByNameAndProcessTypeAndParentIdAndRegionGroup(
                    FILE_RSV_TA, processType, parentId, regionGroup);

            initializeFileGenReserveTa(genFileReserveTaJobInstances, stlJobGroupDtoMap, taskExecutionDto, stlReadyGroupId);

            /* GEN FILES RESERVE TA START */
            List<JobInstance> genStatementFileReserveTaJobInstances = findJobInstancesByNameAndProcessTypeAndParentIdAndRegionGroup(
                    FILE_RSV_BILL_STATEMENT_TA, processType, parentId, regionGroup);

            initializeBillStatementFileGenReserveTa(genStatementFileReserveTaJobInstances, stlJobGroupDtoMap, taskExecutionDto, stlReadyGroupId);


            taskExecutionDto.setStlJobGroupDtoMap(stlJobGroupDtoMap);

            if (Arrays.asList(FINAL, ADJUSTED, PRELIM).contains(taskExecutionDto.getProcessType())) {
                determineIfJobsAreLocked(taskExecutionDto, stlReadyJob.getBillingPeriod());
            }

            taskExecutionDto.getStlJobGroupDtoMap().values().forEach(stlJobGroupDto -> {

                boolean isDaily = taskExecutionDto.getProcessType().equals(DAILY);

                List<JobCalculationDto> jobDtos = stlJobGroupDto.getJobCalculationDtos();
                Date billPeriodStart = taskExecutionDto.getBillPeriodStartDate();
                Date billPeriodEnd = taskExecutionDto.getBillPeriodEndDate();

                stlJobGroupDto.setRemainingDatesCalc(getRemainingDatesForCalculation(jobDtos, billPeriodStart, billPeriodEnd));

                stlJobGroupDto.setRemainingDatesGenInputWs(getRemainingDatesForGenInputWs(jobDtos, billPeriodStart, billPeriodEnd));

                determineStlJobGroupDtoStatus(stlJobGroupDto, isDaily, billPeriodStart, billPeriodEnd);

                if (!isDaily && stlJobGroupDto.isHeader()) {

                    List<ViewSettlementJob> viewSettlementJobs = stlReadyJobQueryService
                            .getStlReadyJobsByParentIdAndProcessTypeAndRegionGroup(processType, parentIdStr, regionGroup);

                    stlJobGroupDto.setOutdatedTradingDates(getOutdatedTradingDates(jobDtos,
                            viewSettlementJobs, billPeriodStart, billPeriodEnd));
                }

                // determine if line rental is finalized
                if (!isDaily) {
                    // all non headers are of ADJUSTED type
                    MeterProcessType lrMeterProcessType = stlJobGroupDto.isHeader() ? processType : MeterProcessType.ADJUSTED;

                    LocalDateTime finalizedLrDate = settlementJobLockRepository
                            .getLockDateByCalculationTypeGroupIdAndProcessTypeAndRegionGroup(stlJobGroupDto.getGroupId(),
                                    StlCalculationType.LINE_RENTAL, lrMeterProcessType, regionGroup);

                    stlJobGroupDto.setLockedLr(finalizedLrDate != null);

                    // lock lr if trading amounts can no longer be finalized (only possible for ADJUSTED parent / child runs)
                    if (!stlJobGroupDto.isLockedLr() && stlJobGroupDto.isLocked()
                            && stlJobGroupDto.getTaggingStatus() != BatchStatus.COMPLETED) {
                        stlJobGroupDto.setLockedLr(true);
                    }
                }

                if (isDaily) {

                    LocalDateTime latestStlReadyJobExecStartTime = viewSettlementJobRepository
                            .getLatestJobExecStartTimeByProcessTypeAndParentId(processType.name(), parentIdStr);

                    stlJobGroupDto.getSortedJobCalculationDtos().stream()
                            .filter(jobDto -> Objects.equals(GENERATE_IWS, jobDto.getJobStage())
                                    && jobDto.getJobExecStatus() == BatchStatus.COMPLETED)
                            .findFirst()
                            .ifPresent(genIwsDto -> {
                                        if (latestStlReadyJobExecStartTime != null && latestStlReadyJobExecStartTime.isAfter(
                                                DateUtil.convertToLocalDateTime(genIwsDto.getRunDate()))) {
                                            stlJobGroupDto.getOutdatedTradingDates()
                                                    .add(DateUtil.convertToLocalDate(taskExecutionDto.getDailyDate()));
                                        }
                                    }
                            );
                }
            });

            taskExecutionDtos.add(taskExecutionDto);
        }

        return new PageImpl<>(taskExecutionDtos, pageableRequest.getPageable(), stlReadyJobs.getTotalElements());
    }

    @Override
    public Page<GroupTaskExecutionDto> findDistinctBillingPeriodAndProcessType(Pageable pageable) {
        return null;
    }

    @Override
    public void launchJob(TaskRunDto taskRunDto) throws URISyntaxException {
        log.info("Running JobName=[{}], type=[{}], baseType=[{}]", taskRunDto.getJobName(), taskRunDto.getMeterProcessType(),
                taskRunDto.getBaseType());

        switch (taskRunDto.getJobName()) {
            case GEN_RSV_INPUT_WS:
                launchGenerateInputWorkspaceJob(taskRunDto);
                break;
            case CALC_RSV_STL:
                launchCalculateJob(taskRunDto);
                break;
            case GEN_MONTHLY_SUMMARY_RTA:
                launchGenMonthlySummaryJob(taskRunDto);
                break;
            case CALC_RGMR:
                launchCalculateGmrJob(taskRunDto);
                break;
            case TAG_RTA:
                launchFinalizeJob(taskRunDto);
                break;
            case FILE_TA:
                launchGenerateFileJob(taskRunDto);
                break;
            case FILE_BILL_STATEMENT_TA:
                launchGenerateBillStatementFileJob(taskRunDto);
                break;
            case FILE_RSV_BILL_STATEMENT_TA:
                launchGenerateBillStatementReserveTaJob(taskRunDto);
                break;
            case FILE_RSV_TA:
                launchGenerateFileReserveTaJob(taskRunDto);
                break;
            case STL_VALIDATION:
                launchStlValidateJob(taskRunDto);
                break;
            case CALC_ALLOC_RESERVE:
                launchCalculateAllocReserveJob(taskRunDto);
                break;
            case FILE_ALLOC_RESERVE:
                launchGenerateAllocReserveReportJob(taskRunDto);
                break;
            default:
                throw new RuntimeException("Job launch failed. Unhandled Job Name: " + taskRunDto.getJobName());
        }
    }

    @Override
    public Page<? extends StubTaskExecutionDto> findJobInstancesByBillingPeriodAndProcessType(Pageable pageable, String billingPeriod, String processType, Long adjNo) {
        return null;
    }

    // Generate Monthly Summary is exclusive for Trading Amounts
    private void initializeGenMonthlySummary(final List<JobInstance> genMonthlySummaryJobInstances,
                                             final Map<String, StlJobGroupDto> stlJobGroupDtoMap,
                                             final SettlementTaskExecutionDto taskExecutionDto,
                                             final String stlReadyGroupId) {

        Set<String> genMonthlySummaryNames = Sets.newHashSet();
        Map<String, List<JobCalculationDto>> genMonthlySummaryJobCalculationDtoMap = getJobCalculationMap(genMonthlySummaryJobInstances,
                // using empty map since gen monthly summary steps consists of tasklets only
                StlJobStage.GEN_MONTHLY_SUMMARY, new HashMap<>());

        for (JobInstance genMonthlySummaryJobInstance : genMonthlySummaryJobInstances) {
            String genMonthlySummaryJobName = genMonthlySummaryJobInstance.getJobName();
            if (genMonthlySummaryNames.contains(genMonthlySummaryJobName)) {
                continue;
            }

            JobExecution genMonthlySummaryJobExecution = getJobExecutionFromJobInstance(genMonthlySummaryJobInstance);

            JobParameters genMonthlySummaryJobParameters = genMonthlySummaryJobExecution.getJobParameters();
            String groupId = genMonthlySummaryJobParameters.getString(GROUP_ID);
            StlJobGroupDto stlJobGroupDto = stlJobGroupDtoMap.getOrDefault(groupId, new StlJobGroupDto());
            BatchStatus currentStatus = genMonthlySummaryJobExecution.getStatus();

            // add gen monthly summary calculations for view calculations
            Optional.ofNullable(genMonthlySummaryJobCalculationDtoMap.get(genMonthlySummaryJobName)).ifPresent(
                    jobCalcDtoList -> stlJobGroupDto.getJobCalculationDtos().addAll(jobCalcDtoList)
            );

            stlJobGroupDto.setGenRsvMonthlySummaryStatus(currentStatus);
            stlJobGroupDto.setGroupId(groupId);
            stlJobGroupDto.setGenMonthlySummaryRunDate(genMonthlySummaryJobExecution.getStartTime());

            Date latestJobExecStartDate = stlJobGroupDto.getLatestJobExecStartDate();
            if (latestJobExecStartDate == null || !latestJobExecStartDate.after(genMonthlySummaryJobExecution.getStartTime())) {
                updateProgress(genMonthlySummaryJobExecution, stlJobGroupDto);
            }

            stlJobGroupDtoMap.put(groupId, stlJobGroupDto);

            if (stlReadyGroupId.equals(groupId)) {
                stlJobGroupDto.setHeader(true);
                taskExecutionDto.setParentStlJobGroupDto(stlJobGroupDto);
            }

            genMonthlySummaryNames.add(genMonthlySummaryJobName);
        }

    }

    // Calculate GMR is exclusive for Trading Amounts
    private void initializeCalculateRgmr(final List<JobInstance> calculateRgmrJobInstances,
                                         final Map<String, StlJobGroupDto> stlJobGroupDtoMap,
                                         final SettlementTaskExecutionDto taskExecutionDto,
                                         final String stlReadyGroupId) {

        Set<String> calcGmrNames = Sets.newHashSet();
        Map<String, List<JobCalculationDto>> gmrJobCalculationDtoMap = getJobCalculationMap(calculateRgmrJobInstances,
                StlJobStage.CALCULATE_GMR, STL_GMR_CALC_STEP_WITH_SKIP_LOGS);

        for (JobInstance calcGmrStlJobInstance : calculateRgmrJobInstances) {
            String calcGmrStlJobName = calcGmrStlJobInstance.getJobName();
            if (calcGmrNames.contains(calcGmrStlJobName)) {
                continue;
            }

            JobExecution calcGmrJobExecution = getJobExecutionFromJobInstance(calcGmrStlJobInstance);

            JobParameters calcGmrJobParameters = calcGmrJobExecution.getJobParameters();
            String groupId = calcGmrJobParameters.getString(GROUP_ID);
            StlJobGroupDto stlJobGroupDto = stlJobGroupDtoMap.getOrDefault(groupId, new StlJobGroupDto());
            BatchStatus currentStatus = calcGmrJobExecution.getStatus();

            // add gmr calculations for view calculations
            Optional.ofNullable(gmrJobCalculationDtoMap.get(calcGmrStlJobName)).ifPresent(
                    jobCalcDtoList -> stlJobGroupDto.getJobCalculationDtos().addAll(jobCalcDtoList)
            );

            stlJobGroupDto.setRgmrVatMFeeCalculationStatus(currentStatus);
            stlJobGroupDto.setGroupId(groupId);
            stlJobGroupDto.setGmrCalcRunDate(calcGmrJobExecution.getStartTime());

            Date latestJobExecStartDate = stlJobGroupDto.getLatestJobExecStartDate();
            if (latestJobExecStartDate == null || !latestJobExecStartDate.after(calcGmrJobExecution.getStartTime())) {
                updateProgress(calcGmrJobExecution, stlJobGroupDto);
            }

            stlJobGroupDtoMap.put(groupId, stlJobGroupDto);

            if (stlReadyGroupId.equals(groupId)) {
                stlJobGroupDto.setHeader(true);
                taskExecutionDto.setParentStlJobGroupDto(stlJobGroupDto);
            }

            calcGmrNames.add(calcGmrStlJobName);
        }

    }

    private Map<String, List<JobCalculationDto>> getJobCalculationMap(List<JobInstance> jobInstances, StlJobStage stlJobStage,
                                                                      Map<String, String> stepWithLabelMap) {
        Map<String, List<JobCalculationDto>> jobCalculationDtoMap = new HashMap<>();

        // add distinct jobNames for multiple same parentId-groupId job instances
        jobInstances.stream().map(JobInstance::getJobName).distinct().forEach(calcJobInstanceName ->
                jobCalculationDtoMap.put(calcJobInstanceName, new ArrayList<>()));

        jobInstances.forEach(jobInstance -> getJobExecutions(jobInstance).forEach(jobExecution -> {
            JobParameters jobParameters = jobExecution.getJobParameters();
            Date startDate = jobParameters.getDate(START_DATE);
            Date endDate = jobParameters.getDate(END_DATE);
            Long runId = jobParameters.getLong(RUN_ID);
            JobCalculationDto jobCalcDto = new JobCalculationDto(jobExecution.getStartTime(), jobExecution.getEndTime(),
                    startDate, endDate, convertStatus(jobExecution.getStatus(), stlJobStage.getLabel()),
                    stlJobStage, jobExecution.getStatus());
            String segregateNss = Optional.ofNullable(batchJobAddtlParamsRepository.findByRunIdAndKey(runId, "segregateNssByLlccPay"))
                    .map(BatchJobAddtlParams::getStringVal).orElse(null);
            jobCalcDto.setSegregateNss(segregateNss);
            jobCalcDto.setTaskSummaryList(showSummaryWithLabel(jobExecution, stepWithLabelMap));
            jobCalculationDtoMap.get(jobInstance.getJobName()).add(jobCalcDto);
        }));

        return jobCalculationDtoMap;
    }

    private void initializeFileGenReserveTa(final List<JobInstance> fileGenJobInstances,
                                            final Map<String, StlJobGroupDto> stlJobGroupDtoMap,
                                            final SettlementTaskExecutionDto taskExecutionDto,
                                            final String stlReadyGroupId) {

        Set<String> generationNames = Sets.newHashSet();

        for (JobInstance genFileJobInstance : fileGenJobInstances) {

            String generationStlJobName = genFileJobInstance.getJobName();
            if (generationNames.contains(generationStlJobName)) {
                continue;
            }

            JobExecution generationJobExecution = getJobExecutionFromJobInstance(genFileJobInstance);
            JobParameters generationJobParameters = generationJobExecution.getJobParameters();
            String groupId = generationJobParameters.getString(GROUP_ID);

            StlJobGroupDto stlJobGroupDto = stlJobGroupDtoMap.getOrDefault(groupId, new StlJobGroupDto());
            BatchStatus currentStatus = generationJobExecution.getStatus();
            stlJobGroupDto.setInvoiceGenerationRsvTaStatus(currentStatus);
            stlJobGroupDto.setGroupId(groupId);
            stlJobGroupDto.setRunEndDateTimeFileRsvTa(generationJobExecution.getEndTime());

            Date latestJobExecStartDate = stlJobGroupDto.getLatestJobExecStartDate();
            if (latestJobExecStartDate == null || !latestJobExecStartDate.after(generationJobExecution.getStartTime())) {
                updateProgress(generationJobExecution, stlJobGroupDto);
            }

            stlJobGroupDtoMap.put(groupId, stlJobGroupDto);

            Optional.ofNullable(generationJobExecution.getExecutionContext()
                    .get(INVOICE_GENERATION_FILENAME_KEY)).ifPresent(val ->
                    stlJobGroupDto.setInvoiceGenFolderRsvTa((String) val));

            if (stlReadyGroupId.equals(groupId)) {
                stlJobGroupDto.setHeader(true);
                taskExecutionDto.setParentStlJobGroupDto(stlJobGroupDto);
            }

            generationNames.add(generationStlJobName);
        }
    }

    private void initializeBillStatementFileGenReserveTa(final List<JobInstance> fileGenJobInstances,
                                                         final Map<String, StlJobGroupDto> stlJobGroupDtoMap,
                                                         final SettlementTaskExecutionDto taskExecutionDto,
                                                         final String stlReadyGroupId) {

        Set<String> generationNames = Sets.newHashSet();

        for (JobInstance genFileJobInstance : fileGenJobInstances) {

            String generationStlJobName = genFileJobInstance.getJobName();
            if (generationNames.contains(generationStlJobName)) {
                continue;
            }

            JobExecution generationJobExecution = getJobExecutionFromJobInstance(genFileJobInstance);
            JobParameters generationJobParameters = generationJobExecution.getJobParameters();
            String groupId = generationJobParameters.getString(GROUP_ID);

            StlJobGroupDto stlJobGroupDto = stlJobGroupDtoMap.getOrDefault(groupId, new StlJobGroupDto());
            BatchStatus currentStatus = generationJobExecution.getStatus();
            stlJobGroupDto.setReserveBillStatementGenerationTaStatus(currentStatus);
            stlJobGroupDto.setGroupId(groupId);
            stlJobGroupDto.setRunEndDateTimeFileReserveBillStatementTa(generationJobExecution.getEndTime());

            Date latestJobExecStartDate = stlJobGroupDto.getLatestJobExecStartDate();
            if (latestJobExecStartDate == null || !latestJobExecStartDate.after(generationJobExecution.getStartTime())) {
                updateProgress(generationJobExecution, stlJobGroupDto);
            }

            stlJobGroupDtoMap.put(groupId, stlJobGroupDto);

            Optional.ofNullable(generationJobExecution.getExecutionContext()
                    .get(INVOICE_GENERATION_FILENAME_KEY)).ifPresent(val ->
                    stlJobGroupDto.setReserveBillStatementGenFolderTa((String) val));

            if (stlReadyGroupId.equals(groupId)) {
                stlJobGroupDto.setHeader(true);
                taskExecutionDto.setParentStlJobGroupDto(stlJobGroupDto);
            }

            generationNames.add(generationStlJobName);
        }
    }

    @Override
    StlCalculationType getStlCalculationType() {
        return StlCalculationType.RESERVE_TRADING_AMOUNTS;
    }

    @Override
    String getDailyGenInputWorkspaceProfile() {
        return SettlementJobProfile.GEN_DAILY_RSV_INPUT_WS;
    }

    @Override
    String getPrelimGenInputWorkspaceProfile() {
        return SettlementJobProfile.GEN_MONTHLY_PRELIM_RSV_INPUT_WS;
    }

    @Override
    String getFinalGenInputWorkspaceProfile() {
        return SettlementJobProfile.GEN_MONTHLY_FINAL_RSV_INPUT_WS;
    }

    @Override
    String getAdjustedMtrAdjGenInputWorkSpaceProfile() {
        return SettlementJobProfile.GEN_MONTHLY_ADJ_MTR_ADJ_EBRSV_INPUT_WS;
    }

    @Override
    String getAdjustedMtrFinGenInputWorkSpaceProfile() {
        return SettlementJobProfile.GEN_MONTHLY_ADJ_MTR_FIN_EBRSV_INPUT_WS;
    }

    @Override
    Map<String, String> getCalculateStepsForSkipLogs() {
        Map<String, String> calcSteps = new LinkedHashMap<>();
        calcSteps.put(DISAGGREGATE_BCQ, "Disaggregate BCQ");
        calcSteps.put(CALC_SCALING_FACTOR, "Calculate Scaling Factor");
        calcSteps.put(CALC_RCRA, "Calculate RTA");

        return calcSteps;
    }

    @Override
    Map<String, String> getInputWorkSpaceStepsForSkipLogs() {
        Map<String, String> iwsSteps = new LinkedHashMap<>();
        iwsSteps.put(RETRIEVE_DATA_STEP, "Retrieve Data Step");
        iwsSteps.put(RETRIEVE_BCQ_STEP, "Retrieve Bcq Step");
        iwsSteps.put(GEN_RESERVE_IW_STEP, "Generate Reserve Workspace step");

        return iwsSteps;
    }

    @Override
    String getDailyCalculateProfile() {
        return SettlementJobProfile.CALC_DAILY_RSV_STL_AMTS;
    }

    @Override
    String getPrelimCalculateProfile() {
        return SettlementJobProfile.CALC_MONTHLY_PRELIM_RSV_STL_AMTS;
    }

    @Override
    String getFinalCalculateProfile() {
        return SettlementJobProfile.CALC_MONTHLY_FINAL_RSV_STL_AMTS;
    }

    @Override
    String getAdjustedMtrAdjCalculateProfile() {
        return SettlementJobProfile.CALC_MONTHLY_ADJ_RSV_STL_AMTS_MTR_ADJ;
    }

    @Override
    String getAdjustedMtrFinCalculateProfile() {
        return SettlementJobProfile.CALC_MONTHLY_ADJ_RSV_STL_AMTS_MTR_FIN;
    }

    @Override
    String getPrelimTaggingProfile() {
        return SettlementJobProfile.TAG_RSV_MONTHLY_PRELIM;
    }

    @Override
    String getFinalTaggingProfile() {
        return SettlementJobProfile.TAG_RSV_MONTHLY_FINAL;
    }

    @Override
    String getAdjustedTaggingProfile() {
        return SettlementJobProfile.TAG_RSV_MONTHLY_ADJ;
    }

    @Override
    String getDailyTaggingProfile() {
        return SettlementJobProfile.TAG_RSV_DAILY;
    }

    @Override
    String getPrelimGenFileProfile() {
        return null;
    }

    @Override
    String getPrelimGenBillStatementProfile() {
        return null;
    }

    @Override
    String getFinalGenFileProfile() {
        return null;
    }

    @Override
    String getAdjustedGenFileProfile() {
        return null;
    }


    // Generate Monthly Summary is exclusive for TA
    private void launchGenMonthlySummaryJob(final TaskRunDto taskRunDto) throws URISyntaxException {
        Preconditions.checkNotNull(taskRunDto.getRunId());
        final Long runId = taskRunDto.getRunId();
        final String groupId = taskRunDto.getGroupId();
        final String type = taskRunDto.getMeterProcessType();
        MeterProcessType processType = MeterProcessType.valueOf(type);

        validateFinalized(groupId, processType, StlCalculationType.RESERVE_TRADING_AMOUNTS, taskRunDto.getRegionGroup());

        List<String> arguments = initializeJobArguments(taskRunDto, runId, groupId, type);
        arguments.add(concatKeyValue(START_DATE, taskRunDto.getBaseStartDate(), "date"));
        arguments.add(concatKeyValue(END_DATE, taskRunDto.getBaseEndDate(), "date"));

        List<String> properties = Lists.newArrayList();

        switch (processType) {
            case PRELIM:
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(
                        SettlementJobProfile.RTA_MONTHLY_SUMMARY_PRELIM)));
                break;
            case FINAL:
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(
                        SettlementJobProfile.RTA_MONTHLY_SUMMARY_FINAL)));
                break;
            case ADJUSTED:
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(
                        SettlementJobProfile.RTA_MONTHLY_SUMMARY_ADJUSTED)));
                break;
            default:
                throw new RuntimeException("Failed to launch job. Unhandled processType: " + type);
        }

        log.info("Running generate monthly reserve summary job name={}, properties={}, arguments={}", taskRunDto.getJobName(), properties, arguments);

        launchJob(SPRING_BATCH_MODULE_STL_CALC, properties, arguments);
    }

    // Calculate GMR is exclusive for TTA
    private void launchCalculateGmrJob(final TaskRunDto taskRunDto) throws URISyntaxException {
        Preconditions.checkNotNull(taskRunDto.getRunId());
        final Long runId = taskRunDto.getRunId();
        final String groupId = taskRunDto.getGroupId();
        final String type = taskRunDto.getMeterProcessType();
        MeterProcessType processType = MeterProcessType.valueOf(type);

        validateFinalized(groupId, processType, StlCalculationType.RESERVE_TRADING_AMOUNTS, taskRunDto.getRegionGroup());

        List<String> arguments = initializeJobArguments(taskRunDto, runId, groupId, type);
        arguments.add(concatKeyValue(START_DATE, taskRunDto.getBaseStartDate(), "date"));
        arguments.add(concatKeyValue(END_DATE, taskRunDto.getBaseEndDate(), "date"));

        List<String> properties = Lists.newArrayList();

        switch (processType) {
            case PRELIM:
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(
                        SettlementJobProfile.CALC_MONTHLY_PRELIM_RGMR_VAT)));
                break;
            case FINAL:
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(
                        SettlementJobProfile.CALC_MONTHLY_FINAL_RGMR_VAT)));
                break;
            case ADJUSTED:
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(
                        SettlementJobProfile.CALC_MONTHLY_ADJ_RGMR_VAT)));
                break;
            default:
                throw new RuntimeException("Failed to launch job. Unhandled processType: " + type);
        }

        log.info("Running calculate reserve gmr job name={}, properties={}, arguments={}", taskRunDto.getJobName(), properties, arguments);

        launchJob(SPRING_BATCH_MODULE_STL_CALC, properties, arguments);
        lockJobJdbc(taskRunDto);
    }

    private void launchGenerateBillStatementReserveTaJob(final TaskRunDto taskRunDto) throws URISyntaxException {
        Preconditions.checkNotNull(taskRunDto.getRunId());
        final Long runId = taskRunDto.getRunId();
        final String groupId = taskRunDto.getGroupId();
        final String type = taskRunDto.getMeterProcessType();

        List<String> arguments = initializeJobArguments(taskRunDto, runId, groupId, type);
        arguments.add(concatKeyValue(START_DATE, taskRunDto.getBaseStartDate(), "date"));
        arguments.add(concatKeyValue(END_DATE, taskRunDto.getBaseEndDate(), "date"));

        List<String> properties = Lists.newArrayList();

        properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(
                SettlementJobProfile.GEN_STATEMENT_FILE_PRELIM_RSV)));

        log.info("Running generate reserve billing statement file job name={}, properties={}, arguments={}",
                taskRunDto.getJobName(), properties, arguments);

        launchJob(SPRING_BATCH_MODULE_FILE_GEN, properties, arguments);
        lockJobJdbc(taskRunDto);
    }

    private void launchGenerateFileReserveTaJob(final TaskRunDto taskRunDto) throws URISyntaxException {
        Preconditions.checkNotNull(taskRunDto.getRunId());
        final Long runId = taskRunDto.getRunId();
        final String groupId = taskRunDto.getGroupId();
        final String type = taskRunDto.getMeterProcessType();

        List<String> arguments = initializeJobArguments(taskRunDto, runId, groupId, type);
        arguments.add(concatKeyValue(START_DATE, taskRunDto.getBaseStartDate(), "date"));
        arguments.add(concatKeyValue(END_DATE, taskRunDto.getBaseEndDate(), "date"));

        List<String> properties = Lists.newArrayList();

        switch (MeterProcessType.valueOf(type)) {
            case PRELIM:
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(
                        SettlementJobProfile.GEN_FILE_PRELIM_RSV)));
                break;
            case FINAL:
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(
                        SettlementJobProfile.GEN_FILE_FINAL_RSV)));
                saveAMSadditionalParams(runId, taskRunDto);
                break;
            case ADJUSTED:
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(
                        SettlementJobProfile.GEN_FILE_ADJ_RSV)));
                saveAMSadditionalParams(runId, taskRunDto);
                break;
            case DAILY:
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(
                        getDailyTaggingProfile())));
                break;
            default:
                throw new RuntimeException("Failed to launch job. Unhandled processType: " + type);
        }

        log.info("Running generate reserve file job name={}, properties={}, arguments={}", taskRunDto.getJobName(), properties, arguments);

        launchJob(SPRING_BATCH_MODULE_FILE_GEN, properties, arguments);
        lockJobJdbc(taskRunDto);
    }

    private void launchCalculateAllocReserveJob(final TaskRunDto taskRunDto) throws URISyntaxException {
        Preconditions.checkNotNull(taskRunDto.getRunId());
        final Long runId = taskRunDto.getRunId();
        final String groupId = taskRunDto.getGroupId();
        final String type = taskRunDto.getMeterProcessType();
        MeterProcessType processType = MeterProcessType.valueOf(type);

        List<String> arguments = initializeJobArguments(taskRunDto, runId, groupId, type);
        arguments.add(concatKeyValue(START_DATE, taskRunDto.getBaseStartDate(), "date"));
        arguments.add(concatKeyValue(END_DATE, taskRunDto.getBaseEndDate(), "date"));

        List<String> properties = Lists.newArrayList();

        switch (processType) {
            case PRELIM:
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(
                        SettlementJobProfile.CALC_RESERVE_MONTHLY_PRELIM_ALLOC)));
                break;
            case FINAL:
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(
                        SettlementJobProfile.CALC_RESERVE_MONTHLY_FINAL_ALLOC)));
                break;
            case ADJUSTED:
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(
                        SettlementJobProfile.CALC_RESERVE_MONTHLY_ADJ_ALLOC)));
                break;
            default:
                throw new RuntimeException("Failed to launch job. Unhandled processType: " + type);
        }

        saveAllocAdditionalParams(runId, taskRunDto);

        log.info("Running rmf calculate allocation job name={}, properties={}, arguments={}", taskRunDto.getJobName(), properties, arguments);

        launchJob(SPRING_BATCH_MODULE_STL_CALC, properties, arguments);
        lockJobJdbc(taskRunDto);
    }

    private void launchGenerateAllocReserveReportJob(final TaskRunDto taskRunDto) throws URISyntaxException {
        Preconditions.checkNotNull(taskRunDto.getRunId());
        final Long runId = taskRunDto.getRunId();
        final String groupId = taskRunDto.getGroupId();
        final String type = taskRunDto.getMeterProcessType();
        List<String> arguments = initializeJobArguments(taskRunDto, runId, groupId, type);
        arguments.add(concatKeyValue(START_DATE, taskRunDto.getBaseStartDate(), "date"));
        arguments.add(concatKeyValue(END_DATE, taskRunDto.getBaseEndDate(), "date"));

        List<String> properties = Lists.newArrayList();

        properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(
                SettlementJobProfile.GEN_RESERVE_FILE_ALLOC_REPORT)));

        log.info("Running generate reserve alloc report job name={}, properties={}, arguments={}", taskRunDto.getJobName(), properties, arguments);

        launchJob(SPRING_BATCH_MODULE_FILE_GEN, properties, arguments);
        lockJobJdbc(taskRunDto);
    }

    // STL VALIDATION JOB LAUNCH
    private void launchStlValidateJob(final TaskRunDto taskRunDto) throws URISyntaxException {
        Preconditions.checkNotNull(taskRunDto.getRunId());
        final Long runId = taskRunDto.getRunId();
        final String groupId = taskRunDto.getGroupId();
        final String type = taskRunDto.getMeterProcessType();

        MeterProcessType processType = MeterProcessType.valueOf(type);

        List<String> arguments = initializeJobArguments(taskRunDto, runId, groupId, type);

        List<String> properties = Lists.newArrayList();

        switch (processType) {
            case DAILY:
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(
                        SettlementJobProfile.VALIDATION_STL_DAILY)));
                arguments.add(concatKeyValue(START_DATE, taskRunDto.getTradingDate(), "date"));
                break;
            case PRELIM:
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(
                        SettlementJobProfile.VALIDATION_STL_MONTHLY_PRELIM)));
                arguments.add(concatKeyValue(START_DATE, taskRunDto.getBaseStartDate(), "date"));
                arguments.add(concatKeyValue(END_DATE, taskRunDto.getBaseEndDate(), "date"));
                break;
            case FINAL:
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(
                        SettlementJobProfile.VALIDATION_STL_MONTHLY_FINAL)));
                arguments.add(concatKeyValue(START_DATE, taskRunDto.getBaseStartDate(), "date"));
                arguments.add(concatKeyValue(END_DATE, taskRunDto.getBaseEndDate(), "date"));
                break;
            case ADJUSTED:
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(
                        SettlementJobProfile.VALIDATION_STL_MONTHLY_ADJ)));
                arguments.add(concatKeyValue(START_DATE, taskRunDto.getBaseStartDate(), "date"));
                arguments.add(concatKeyValue(END_DATE, taskRunDto.getBaseEndDate(), "date"));
                break;
            default:
                throw new RuntimeException("Failed to launch job. Unhandled processType: " + type);
        }

        log.info("Running stl validation job name={}, properties={}, arguments={}", taskRunDto.getJobName(), properties, arguments);

        launchJob("crss-settlement-task-validation", properties, arguments);
        lockJobJdbc(taskRunDto);
    }

}
