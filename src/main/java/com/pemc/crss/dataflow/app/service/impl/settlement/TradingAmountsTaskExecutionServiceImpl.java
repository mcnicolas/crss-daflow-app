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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.stream.Collectors;

import static com.pemc.crss.dataflow.app.support.StlJobStage.CALCULATE_LR;
import static com.pemc.crss.dataflow.app.support.StlJobStage.FINALIZE_LR;
import static com.pemc.crss.dataflow.app.support.StlJobStage.GENERATE_IWS;
import static com.pemc.crss.shared.commons.reference.MeterProcessType.ADJUSTED;
import static com.pemc.crss.shared.commons.reference.MeterProcessType.DAILY;
import static com.pemc.crss.shared.commons.reference.MeterProcessType.FINAL;
import static com.pemc.crss.shared.commons.reference.MeterProcessType.PRELIM;
import static com.pemc.crss.shared.commons.reference.SettlementStepUtil.CALC_BUYER_LINE_RENTAL;
import static com.pemc.crss.shared.commons.reference.SettlementStepUtil.CALC_SCALING_FACTOR;
import static com.pemc.crss.shared.commons.reference.SettlementStepUtil.CALC_SELLER_LINE_RENTAL;
import static com.pemc.crss.shared.commons.reference.SettlementStepUtil.DISAGGREGATE_BCQ;
import static com.pemc.crss.shared.commons.reference.SettlementStepUtil.GEN_RESERVE_IW_STEP;
import static com.pemc.crss.shared.commons.reference.SettlementStepUtil.RETRIEVE_BCQ_STEP;
import static com.pemc.crss.shared.commons.reference.SettlementStepUtil.RETRIEVE_DATA_STEP;
import static com.pemc.crss.shared.core.dataflow.reference.SettlementJobName.CALC_GMR;
import static com.pemc.crss.shared.core.dataflow.reference.SettlementJobName.CALC_LR;
import static com.pemc.crss.shared.core.dataflow.reference.SettlementJobName.CALC_STL;
import static com.pemc.crss.shared.core.dataflow.reference.SettlementJobName.FILE_LR;
import static com.pemc.crss.shared.core.dataflow.reference.SettlementJobName.FILE_RSV_TA;
import static com.pemc.crss.shared.core.dataflow.reference.SettlementJobName.FILE_TA;
import static com.pemc.crss.shared.core.dataflow.reference.SettlementJobName.GEN_EBRSV_INPUT_WS;
import static com.pemc.crss.shared.core.dataflow.reference.SettlementJobName.TAG_LR;
import static com.pemc.crss.shared.core.dataflow.reference.SettlementJobName.TAG_TA;

@Slf4j
@Service("tradingAmountsTaskExecutionService")
@Transactional
public class TradingAmountsTaskExecutionServiceImpl extends StlTaskExecutionServiceImpl {

    private static final Map<String, String> STL_GMR_CALC_STEP_WITH_SKIP_LOGS =
            Collections.singletonMap(SettlementStepUtil.CALC_GMR_VAT, "Calculate GMR / VAT");

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

            SettlementTaskExecutionDto taskExecutionDto = initializeTaskExecutionDto(stlReadyJob, parentIdStr);
            String stlReadyGroupId = taskExecutionDto.getStlReadyGroupId();

            StlJobGroupDto initialJobGroupDto = new StlJobGroupDto();
            initialJobGroupDto.setGroupId(stlReadyGroupId);

            taskExecutionDto.setParentStlJobGroupDto(initialJobGroupDto);

            Map<String, StlJobGroupDto> stlJobGroupDtoMap = new HashMap<>();
            stlJobGroupDtoMap.put(stlReadyGroupId, initialJobGroupDto);

            final MeterProcessType processType = taskExecutionDto.getProcessType();

            /* GENERATE INPUT WORKSPACE START */
            List<JobInstance> generateInputWsJobInstances = findJobInstancesByNameAndProcessTypeAndParentId(
                    GEN_EBRSV_INPUT_WS, processType, parentId);

            initializeGenInputWorkSpace(generateInputWsJobInstances, stlJobGroupDtoMap, taskExecutionDto, stlReadyGroupId);

            /* SETTLEMENT CALCULATION START */
            List<JobInstance> calculationJobInstances = findJobInstancesByNameAndProcessTypeAndParentId(
                    CALC_STL, processType, parentId);

            initializeStlCalculation(calculationJobInstances, stlJobGroupDtoMap, taskExecutionDto, stlReadyGroupId);

            /* CALCULATE LINE RENTAL START*/
            List<JobInstance> calcLineRentalJobInstances = findJobInstancesByNameAndProcessTypeAndParentId(CALC_LR,
                    processType, parentId);

            initializeCalculateLr(calcLineRentalJobInstances, stlJobGroupDtoMap, taskExecutionDto, stlReadyGroupId);

            /* CALCULATE GMR START */
            List<JobInstance> calculateGmrJobInstances = findJobInstancesByNameAndProcessTypeAndParentId(
                    CALC_GMR, processType, parentId);

            initializeCalculateGmr(calculateGmrJobInstances, stlJobGroupDtoMap, taskExecutionDto, stlReadyGroupId);

            /* FINALIZE START */
            List<JobInstance> taggingJobInstances = findJobInstancesByNameAndProcessTypeAndParentId(
                    TAG_TA, processType, parentId);

            initializeTagging(taggingJobInstances, stlJobGroupDtoMap, taskExecutionDto, stlReadyGroupId);

            /* FINALIZE LR START */
            List<JobInstance> taggingLineRentalJobInstances = findJobInstancesByNameAndProcessTypeAndParentId(
                    TAG_LR, processType, parentId);

            initializeTaggingLineRental(taggingLineRentalJobInstances, stlJobGroupDtoMap, taskExecutionDto, stlReadyGroupId);

            /* GEN FILES ENERGY TA START */
            List<JobInstance> genFileJobInstances = findJobInstancesByNameAndProcessTypeAndParentId(
                    FILE_TA, processType, parentId);

            initializeFileGen(genFileJobInstances, stlJobGroupDtoMap, taskExecutionDto, stlReadyGroupId);

            /* GEN FILES RESERVE TA START */
            List<JobInstance> genFileReserveTaJobInstances = findJobInstancesByNameAndProcessTypeAndParentId(
                    FILE_RSV_TA, processType, parentId);

            initializeFileGenReserveTa(genFileReserveTaJobInstances, stlJobGroupDtoMap, taskExecutionDto, stlReadyGroupId);

            /* GEN FILES LINE RENTAL START */
            List<JobInstance> genFileLineRentalJobInstances = findJobInstancesByNameAndProcessTypeAndParentId(
                    FILE_LR, processType, parentId);

            initializeFileGenLineRental(genFileLineRentalJobInstances, stlJobGroupDtoMap, taskExecutionDto, stlReadyGroupId);

            taskExecutionDto.setStlJobGroupDtoMap(stlJobGroupDtoMap);

            if (Arrays.asList(FINAL, ADJUSTED, PRELIM).contains(taskExecutionDto.getProcessType())) {
                determineIfJobsAreLocked(taskExecutionDto);
            }

            // determine if line rental is finalized
            if (Arrays.asList(FINAL, PRELIM, ADJUSTED).contains(taskExecutionDto.getProcessType())) {
                boolean lrIsFinalized = settlementJobLockRepository.billingPeriodIsFinalized(
                        DateUtil.convertToLocalDateTime(taskExecutionDto.getBillPeriodStartDate()),
                        DateUtil.convertToLocalDateTime(taskExecutionDto.getBillPeriodEndDate()),
                        taskExecutionDto.getProcessType().name(), StlCalculationType.LINE_RENTAL.name());

                taskExecutionDto.getParentStlJobGroupDto().setLockedLr(lrIsFinalized);
            }

            taskExecutionDto.getStlJobGroupDtoMap().values().forEach(stlJobGroupDto -> {

                boolean isDaily = taskExecutionDto.getProcessType().equals(DAILY);

                List<JobCalculationDto> jobDtos = stlJobGroupDto.getJobCalculationDtos();
                Date billPeriodStart = taskExecutionDto.getBillPeriodStartDate();
                Date billPeriodEnd = taskExecutionDto.getBillPeriodEndDate();

                stlJobGroupDto.setRemainingDatesCalc(getRemainingDatesForCalculation(jobDtos, billPeriodStart, billPeriodEnd));

                stlJobGroupDto.setRemainingDatesCalcLr(getRemainingDatesForLineRentalCalc(jobDtos, billPeriodStart, billPeriodEnd));

                stlJobGroupDto.setRemainingDatesGenInputWs(getRemainingDatesForGenInputWs(jobDtos, billPeriodStart, billPeriodEnd));

                determineStlJobGroupDtoStatus(stlJobGroupDto, isDaily);

                determineStlJobGroupDtoLrStatus(stlJobGroupDto, isDaily);

                if (!isDaily && stlJobGroupDto.isHeader()) {

                    List<ViewSettlementJob> viewSettlementJobs = stlReadyJobQueryService
                            .getStlReadyJobsByParentIdAndProcessType(processType, parentIdStr);

                    stlJobGroupDto.setOutdatedTradingDates(getOutdatedTradingDates(jobDtos,
                            viewSettlementJobs, billPeriodStart, billPeriodEnd));
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
    public Page<? extends StubTaskExecutionDto> findJobInstancesByBillingPeriodAndProcessType(Pageable pageable, String billingPeriod, String processType) {
        return null;
    }

    @Override
    public void launchJob(TaskRunDto taskRunDto) throws URISyntaxException {
        validateJobName(taskRunDto.getJobName());

        log.info("Running JobName=[{}], type=[{}], baseType=[{}]", taskRunDto.getJobName(), taskRunDto.getMeterProcessType(),
                taskRunDto.getBaseType());

        switch (taskRunDto.getJobName()) {
            case GEN_EBRSV_INPUT_WS:
                validateJobName(CALC_STL);
                validateJobName(CALC_GMR);
                validateJobName(TAG_TA);
                launchGenerateInputWorkspaceJob(taskRunDto);
                break;
            case CALC_STL:
                validateJobName(GEN_EBRSV_INPUT_WS);
                validateJobName(CALC_GMR);
                validateJobName(TAG_TA);
                launchCalculateJob(taskRunDto);
                break;
            case CALC_LR:
                validateJobName(GEN_EBRSV_INPUT_WS);
                validateJobName(TAG_LR);
                launchCalculateLineRentalJob(taskRunDto);
                break;
            case CALC_GMR:
                validateJobName(GEN_EBRSV_INPUT_WS);
                validateJobName(CALC_STL);
                validateJobName(TAG_TA);
                launchCalculateGmrJob(taskRunDto);
                break;
            case TAG_TA:
                validateJobName(GEN_EBRSV_INPUT_WS);
                validateJobName(CALC_STL);
                validateJobName(CALC_GMR);
                launchFinalizeJob(taskRunDto);
                break;
            case TAG_LR:
                validateJobName(GEN_EBRSV_INPUT_WS);
                validateJobName(CALC_LR);
                launchFinalizeLineRentalJob(taskRunDto);
                break;
            case FILE_TA:
                validateJobName(FILE_RSV_TA);
                launchGenerateFileJob(taskRunDto);
                break;
            case FILE_RSV_TA:
                validateJobName(FILE_TA);
                launchGenerateFileReserveTaJob(taskRunDto);
                break;
            case FILE_LR:
                launchGenerateFileLineRentalJob(taskRunDto);
                break;
            default:
                throw new RuntimeException("Job launch failed. Unhandled Job Name: " + taskRunDto.getJobName());
        }
    }


    // Line Rental job instances start
    private void initializeCalculateLr(final List<JobInstance> calculateLineRentalInstances,
                                       final Map<String, StlJobGroupDto> stlJobGroupDtoMap,
                                       final SettlementTaskExecutionDto taskExecutionDto,
                                       final String stlReadyGroupId) {

        for (JobInstance calcLrJobInstance : calculateLineRentalInstances) {

            JobExecution calcLrJobExec = getJobExecutionFromJobInstance(calcLrJobInstance);

            Date billPeriodStartDate = taskExecutionDto.getBillPeriodStartDate();
            Date billPeriodEndDate = taskExecutionDto.getBillPeriodEndDate();

            BatchStatus currentBatchStatus = calcLrJobExec.getStatus();
            JobParameters calcLrJobParameters = calcLrJobExec.getJobParameters();
            String groupId = calcLrJobParameters.getString(GROUP_ID);
            Date calcLrStartDate = calcLrJobParameters.getDate(START_DATE);
            Date calcLrEndDate = calcLrJobParameters.getDate(END_DATE);

            final StlJobGroupDto stlJobGroupDto = stlJobGroupDtoMap.getOrDefault(groupId, new StlJobGroupDto());

            if (currentBatchStatus.isRunning()) {
                stlJobGroupDto.setRunningLrCalculation(true);
            }

            boolean fullLrCalculation = billPeriodStartDate != null && billPeriodEndDate != null
                    && calcLrStartDate.compareTo(billPeriodStartDate) == 0 && calcLrEndDate.compareTo(billPeriodEndDate) == 0;

            final String jobCalcLrStatus = fullLrCalculation
                    ? convertStatus(currentBatchStatus, FULL + CALCULATE_LR.getLabel())
                    : convertStatus(currentBatchStatus, PARTIAL + CALCULATE_LR.getLabel());

            List<JobCalculationDto> jobCalculationDtoList = stlJobGroupDto.getJobCalculationDtos();

            JobCalculationDto partialCalcLrDto = new JobCalculationDto(calcLrJobExec.getStartTime(),
                    calcLrJobExec.getEndTime(), calcLrStartDate, calcLrEndDate,
                    jobCalcLrStatus, CALCULATE_LR, currentBatchStatus);

            partialCalcLrDto.setTaskSummaryList(showSummaryWithLabel(calcLrJobExec, getCalculateLrStepsForSkipLogs()));

            jobCalculationDtoList.add(partialCalcLrDto);

            stlJobGroupDto.setJobCalculationDtos(jobCalculationDtoList);
            stlJobGroupDto.setGroupId(groupId);

            Date latestJobExecStartDate = stlJobGroupDto.getLatestJobExecStartDate();
            if (latestJobExecStartDate == null || !latestJobExecStartDate.after(calcLrJobExec.getStartTime())) {
                updateProgress(calcLrJobExec, stlJobGroupDto);
            }

            Date maxPartialCalcLrDate = stlJobGroupDto.getJobCalculationDtos().stream()
                    .filter(jobCalc -> jobCalc.getJobStage().equals(CALCULATE_LR))
                    .map(JobCalculationDto::getRunDate)
                    .max(Date::compareTo).get();

            stlJobGroupDto.setMaxPartialCalcLrRunDate(maxPartialCalcLrDate);
            stlJobGroupDtoMap.put(groupId, stlJobGroupDto);

            if (stlReadyGroupId.equals(groupId)) {
                stlJobGroupDto.setHeader(true);
                taskExecutionDto.setParentStlJobGroupDto(stlJobGroupDto);
            }
        }
    }

    private void initializeTaggingLineRental(final List<JobInstance> taggingLineRentalJobInstances,
                           final Map<String, StlJobGroupDto> stlJobGroupDtoMap,
                           final SettlementTaskExecutionDto taskExecutionDto,
                           final String stlReadyGroupId) {

        Set<String> tagNames = Sets.newHashSet();

        for (JobInstance taggingLrJobInstance : taggingLineRentalJobInstances) {

            String tagLrJobName = taggingLrJobInstance.getJobName();
            if (tagNames.contains(tagLrJobName)) {
                continue;
            }

            JobExecution tagLrJobExecution = getJobExecutionFromJobInstance(taggingLrJobInstance);
            JobParameters tagLrJobParameters = tagLrJobExecution.getJobParameters();
            Date tagLrStartDate = tagLrJobParameters.getDate(START_DATE);
            Date tagLrEndDate = tagLrJobParameters.getDate(END_DATE);
            String groupId = tagLrJobParameters.getString(GROUP_ID);

            StlJobGroupDto stlJobGroupDto = stlJobGroupDtoMap.getOrDefault(groupId, new StlJobGroupDto());
            BatchStatus currentStatus = tagLrJobExecution.getStatus();
            stlJobGroupDto.setTaggingLrStatus(currentStatus);
            stlJobGroupDto.setGroupId(groupId);

            JobCalculationDto finalizeLrJobDto = new JobCalculationDto(tagLrJobExecution.getStartTime(),
                    tagLrJobExecution.getEndTime(), tagLrStartDate, tagLrEndDate,
                    convertStatus(currentStatus, FINALIZE_LR.getLabel()), FINALIZE_LR, currentStatus);

            stlJobGroupDto.getJobCalculationDtos().add(finalizeLrJobDto);

            Date latestJobExecStartDate = stlJobGroupDto.getLatestJobExecStartDate();
            if (latestJobExecStartDate == null || !latestJobExecStartDate.after(tagLrJobExecution.getStartTime())) {
                updateProgress(tagLrJobExecution, stlJobGroupDto);
            }

            stlJobGroupDtoMap.put(groupId, stlJobGroupDto);

            if (stlReadyGroupId.equals(groupId)) {
                stlJobGroupDto.setHeader(true);
                taskExecutionDto.setParentStlJobGroupDto(stlJobGroupDto);
            }

            tagNames.add(tagLrJobName);
        }
    }

    private void initializeFileGenLineRental(final List<JobInstance> fileGenJobLineRentalInstances,
                                            final Map<String, StlJobGroupDto> stlJobGroupDtoMap,
                                            final SettlementTaskExecutionDto taskExecutionDto,
                                            final String stlReadyGroupId) {

        Set<String> generationNames = Sets.newHashSet();

        for (JobInstance genFileLrJobInstance : fileGenJobLineRentalInstances) {

            String genFileLrJobName = genFileLrJobInstance.getJobName();
            if (generationNames.contains(genFileLrJobName)) {
                continue;
            }

            JobExecution genFileLrJobExecution = getJobExecutionFromJobInstance(genFileLrJobInstance);
            JobParameters genFileLrJobParameters = genFileLrJobExecution.getJobParameters();
            String groupId = genFileLrJobParameters.getString(GROUP_ID);

            StlJobGroupDto stlJobGroupDto = stlJobGroupDtoMap.getOrDefault(groupId, new StlJobGroupDto());
            BatchStatus currentStatus = genFileLrJobExecution.getStatus();
            stlJobGroupDto.setInvoiceGenerationLrStatus(currentStatus);
            stlJobGroupDto.setGroupId(groupId);
            stlJobGroupDto.setRunEndDateTimeFileLr(genFileLrJobExecution.getEndTime());

            if (!stlJobGroupDto.getLatestJobExecStartDate().after(genFileLrJobExecution.getStartTime())) {
                updateProgress(genFileLrJobExecution, stlJobGroupDto);
            }

            stlJobGroupDtoMap.put(groupId, stlJobGroupDto);

            Optional.ofNullable(genFileLrJobExecution.getExecutionContext()
                    .get(INVOICE_GENERATION_FILENAME_KEY)).ifPresent(val ->
                    stlJobGroupDto.setInvoiceGenFolderLr((String) val));

            if (stlReadyGroupId.equals(groupId)) {
                stlJobGroupDto.setHeader(true);
                taskExecutionDto.setParentStlJobGroupDto(stlJobGroupDto);
            }

            generationNames.add(genFileLrJobName);
        }
    }

    private SortedSet<LocalDate> getRemainingDatesForLineRentalCalc(final List<JobCalculationDto> jobDtos,
                                                                    final Date billPeriodStart,
                                                                    final Date billPeriodEnd) {

        SortedSet<LocalDate> remainingCalcLrDates = createRange(billPeriodStart, billPeriodEnd);

        List<JobCalculationDto> filteredJobDtosAsc = jobDtos.stream().filter(jobDto ->
                Arrays.asList(CALCULATE_LR, GENERATE_IWS).contains(jobDto.getJobStage())
                        && jobDto.getJobExecStatus() == BatchStatus.COMPLETED)
                .sorted(Comparator.comparing(JobCalculationDto::getRunDate)).collect(Collectors.toList());

        for (JobCalculationDto jobDto : filteredJobDtosAsc) {
            switch (jobDto.getJobStage()) {
                case GENERATE_IWS:
                    addDateRangeTo(remainingCalcLrDates, jobDto.getStartDate(), jobDto.getEndDate());
                    break;
                case CALCULATE_LR:
                    removeDateRangeFrom(remainingCalcLrDates, jobDto.getStartDate(), jobDto.getEndDate());
                    break;
                default:
                    // do nothing
            }
        }

        return remainingCalcLrDates;
    }

    private void determineStlJobGroupDtoLrStatus(final StlJobGroupDto stlJobGroupDto, final boolean isDaily) {
        List<StlJobStage> includedJobStages = Arrays.asList(StlJobStage.GENERATE_IWS, StlJobStage.CALCULATE_LR,
                StlJobStage.FINALIZE_LR);

        stlJobGroupDto.getSortedJobCalculationDtos().stream().filter(dto -> includedJobStages.contains(dto.getJobStage()))
                .findFirst().ifPresent(jobDto -> {

            if (jobDto.getJobExecStatus().isRunning() || isDaily) {
                stlJobGroupDto.setLineRentalTopStatus(jobDto.getStatus());
            } else {
                // special rules for generate input ws and line rental calculations
                switch (jobDto.getJobStage()) {
                    case GENERATE_IWS:
                        if (stlJobGroupDto.getRemainingDatesGenInputWs().isEmpty()) {
                            stlJobGroupDto.setLineRentalTopStatus(convertStatus(jobDto.getJobExecStatus(), FULL + GENERATE_IWS.getLabel()));
                        } else {
                            stlJobGroupDto.setLineRentalTopStatus(convertStatus(jobDto.getJobExecStatus(), PARTIAL + GENERATE_IWS.getLabel()));
                        }
                        break;
                    case CALCULATE_LR:
                        if (stlJobGroupDto.getRemainingDatesCalcLr().isEmpty()) {
                            stlJobGroupDto.setLineRentalTopStatus(convertStatus(jobDto.getJobExecStatus(), FULL + CALCULATE_LR.getLabel()));
                        } else {
                            stlJobGroupDto.setLineRentalTopStatus(convertStatus(jobDto.getJobExecStatus(), PARTIAL + CALCULATE_LR.getLabel()));
                        }
                        break;
                    default:
                        stlJobGroupDto.setLineRentalTopStatus(jobDto.getStatus());
                }
            }

        });
    }

    // Line Rental job instances end

    // Calculate GMR is exclusive for Trading Amounts
    private void initializeCalculateGmr(final List<JobInstance> calculateGmrJobInstances,
                                        final Map<String, StlJobGroupDto> stlJobGroupDtoMap,
                                        final SettlementTaskExecutionDto taskExecutionDto,
                                        final String stlReadyGroupId) {

        Set<String> calcGmrNames = Sets.newHashSet();
        Map<String, List<JobCalculationDto>> gmrJobCalculationDtoMap = getCalcGmrJobCalculationMap(calculateGmrJobInstances);

        for (JobInstance calcGmrStlJobInstance : calculateGmrJobInstances) {
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

            stlJobGroupDto.setGmrVatMFeeCalculationStatus(currentStatus);
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

            if (!stlJobGroupDto.getLatestJobExecStartDate().after(generationJobExecution.getStartTime())) {
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

    private Map<String, List<JobCalculationDto>> getCalcGmrJobCalculationMap(List<JobInstance> calcGmrStlJobInstances) {
        Map<String, List<JobCalculationDto>> jobCalculationDtoMap = new HashMap<>();

        // add distinct jobNames for multiple same parentId-groupId job instances
        calcGmrStlJobInstances.stream().map(JobInstance::getJobName).distinct().forEach(calcJobInstanceName ->
                jobCalculationDtoMap.put(calcJobInstanceName, new ArrayList<>()));

        calcGmrStlJobInstances.forEach(calcGmrInstance -> getJobExecutions(calcGmrInstance).forEach(jobExecution -> {
            JobParameters calcGmrJobParameters = jobExecution.getJobParameters();
            Date calcGmrStartDate = calcGmrJobParameters.getDate(START_DATE);
            Date calcGmrEndDate = calcGmrJobParameters.getDate(END_DATE);
            JobCalculationDto gmrCalcDto = new JobCalculationDto(jobExecution.getStartTime(), jobExecution.getEndTime(),
                    calcGmrStartDate, calcGmrEndDate, convertStatus(jobExecution.getStatus(), StlJobStage.CALCULATE_GMR.getLabel()),
                    StlJobStage.CALCULATE_GMR, jobExecution.getStatus());
            gmrCalcDto.setTaskSummaryList(showSummaryWithLabel(jobExecution, STL_GMR_CALC_STEP_WITH_SKIP_LOGS));
            jobCalculationDtoMap.get(calcGmrInstance.getJobName()).add(gmrCalcDto);
        }));

        return jobCalculationDtoMap;
    }

    // Launch Line Rental Jobs start (Line rental calc is not applicable to ADJUSTED)
    private void launchCalculateLineRentalJob(final TaskRunDto taskRunDto) throws URISyntaxException {
        Preconditions.checkNotNull(taskRunDto.getRunId());
        final Long runId = taskRunDto.getRunId();
        final String groupId = taskRunDto.getGroupId();
        final String type = taskRunDto.getMeterProcessType();

        List<String> arguments = initializeJobArguments(taskRunDto, runId, groupId, type);

        List<String> properties = Lists.newArrayList();

        MeterProcessType processType = MeterProcessType.valueOf(type);

        switch (processType) {
            case DAILY:
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(
                        SettlementJobProfile.CALC_DAILY_LR)));
                arguments.add(concatKeyValue(START_DATE, taskRunDto.getTradingDate(), "date"));
                break;
            case PRELIM:
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(
                        SettlementJobProfile.CALC_MONTHLY_PRELIM_LR)));
                arguments.add(concatKeyValue(START_DATE, taskRunDto.getStartDate(), "date"));
                arguments.add(concatKeyValue(END_DATE, taskRunDto.getEndDate(), "date"));
                break;
            case FINAL:
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(
                        SettlementJobProfile.CALC_MONTHLY_FINAL_LR)));
                arguments.add(concatKeyValue(START_DATE, taskRunDto.getStartDate(), "date"));
                arguments.add(concatKeyValue(END_DATE, taskRunDto.getEndDate(), "date"));
                break;
            case ADJUSTED:
                boolean finalBased = MeterProcessType.valueOf(taskRunDto.getBaseType()).equals(FINAL);

                final String activeProfile = finalBased ? SettlementJobProfile.CALC_MONTHLY_ADJ_LR_MTR_FIN :
                        SettlementJobProfile.CALC_MONTHLY_ADJ_LR_MTR_ADJ;

                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(activeProfile)));
                arguments.add(concatKeyValue(START_DATE, taskRunDto.getStartDate(), "date"));
                arguments.add(concatKeyValue(END_DATE, taskRunDto.getEndDate(), "date"));
                break;
            default:
                throw new RuntimeException("Failed to launch job. Unhandled processType: " + processType);
        }

        // Create SettlementJobLock. Do not include daily since it does not have finalize job
        if (processType != DAILY) {
            saveSettlementJobLock(groupId, processType, taskRunDto, StlCalculationType.LINE_RENTAL);
        }

        log.info("Running calculate job name={}, properties={}, arguments={}", taskRunDto.getJobName(), properties, arguments);

        launchJob(SPRING_BATCH_MODULE_STL_CALC, properties, arguments);
        lockJobJdbc(taskRunDto);
    }

    private void launchFinalizeLineRentalJob(final TaskRunDto taskRunDto) throws URISyntaxException {
        final Long runId = System.currentTimeMillis();
        final String groupId = taskRunDto.getGroupId();
        final String type = taskRunDto.getMeterProcessType();

        List<String> arguments = initializeJobArguments(taskRunDto, runId, groupId, type);
        arguments.add(concatKeyValue(START_DATE, taskRunDto.getBaseStartDate(), "date"));
        arguments.add(concatKeyValue(END_DATE, taskRunDto.getBaseEndDate(), "date"));

        List<String> properties = Lists.newArrayList();

        switch (MeterProcessType.valueOf(type)) {
            case PRELIM:
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE,
                        fetchSpringProfilesActive(SettlementJobProfile.TAG_MONTHLY_PRELIM_LR)));
                break;
            case FINAL:
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE,
                        fetchSpringProfilesActive(SettlementJobProfile.TAG_MONTHLY_FINAL_LR)));
                break;
            case ADJUSTED:
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE,
                        fetchSpringProfilesActive(SettlementJobProfile.TAG_MONTHLY_ADJ_LR)));
                break;
            default:
                throw new RuntimeException("Failed to launch job. Unhandled processType: " + type);
        }

        log.info("Running finalize line rental job name={}, properties={}, arguments={}", taskRunDto.getJobName(), properties, arguments);

        launchJob(SPRING_BATCH_MODULE_STL_CALC, properties, arguments);
        lockJobJdbc(taskRunDto);
    }

    private void launchGenerateFileLineRentalJob(final TaskRunDto taskRunDto) throws URISyntaxException {
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
                        SettlementJobProfile.GEN_FILE_PRELIM_LR)));
                break;
            case FINAL:
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(
                        SettlementJobProfile.GEN_FILE_FINAL_LR)));
                break;
            case ADJUSTED:
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(
                        SettlementJobProfile.GEN_FILE_ADJ_LR)));
                // TODO: remove this once job in stl is deployed
                throw new RuntimeException("Failed to launch job. Unhandled processType: " + type);
            default:
                throw new RuntimeException("Failed to launch job. Unhandled processType: " + type);
        }

        log.info("Running generate line rental file job name={}, properties={}, arguments={}", taskRunDto.getJobName(), properties, arguments);

        launchJob(SPRING_BATCH_MODULE_FILE_GEN, properties, arguments);
        lockJobJdbc(taskRunDto);
    }


    // Launch Line Rental Jobs end

    // Calculate GMR is exclusive for TTA
    private void launchCalculateGmrJob(final TaskRunDto taskRunDto) throws URISyntaxException {
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
                        SettlementJobProfile.CALC_MONTHLY_PRELIM_GMR_VAT)));
                break;
            case FINAL:
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(
                        SettlementJobProfile.CALC_MONTHLY_FINAL_GMR_VAT)));
                break;
            case ADJUSTED:
                properties.add(concatKeyValue(SPRING_PROFILES_ACTIVE, fetchSpringProfilesActive(
                        SettlementJobProfile.CALC_MONTHLY_ADJ_GMR_VAT)));
                break;
            default:
                throw new RuntimeException("Failed to launch job. Unhandled processType: " + type);
        }

        log.info("Running calculate gmr job name={}, properties={}, arguments={}", taskRunDto.getJobName(), properties, arguments);

        launchJob(SPRING_BATCH_MODULE_STL_CALC, properties, arguments);
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
            default:
                throw new RuntimeException("Failed to launch job. Unhandled processType: " + type);
        }

        log.info("Running generate reserve file job name={}, properties={}, arguments={}", taskRunDto.getJobName(), properties, arguments);

        launchJob(SPRING_BATCH_MODULE_FILE_GEN, properties, arguments);
        lockJobJdbc(taskRunDto);
    }


    @Override
    StlCalculationType getStlCalculationType() {
        return StlCalculationType.TRADING_AMOUNTS;
    }

    @Override
    String getDailyGenInputWorkspaceProfile() {
        return SettlementJobProfile.GEN_DAILY_EBRSV_INPUT_WS;
    }

    @Override
    String getPrelimGenInputWorkspaceProfile() {
        return SettlementJobProfile.GEN_MONTHLY_PRELIM_EBRSV_INPUT_WS;
    }

    @Override
    String getFinalGenInputWorkspaceProfile() {
        return SettlementJobProfile.GEN_MONTHLY_FINAL_EBRSV_INPUT_WS;
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
    Map<String, String> getInputWorkSpaceStepsForSkipLogs() {
        Map<String, String> iwsSteps = new LinkedHashMap<>();
        iwsSteps.put(RETRIEVE_DATA_STEP, "Retrieve Data Step");
        iwsSteps.put(RETRIEVE_BCQ_STEP, "Retrieve Bcq Step");
        iwsSteps.put(GEN_RESERVE_IW_STEP, "Generate Reserve Workspace step");


        return iwsSteps;
    }

    @Override
    String getDailyCalculateProfile() {
        return SettlementJobProfile.CALC_DAILY_STL_AMTS;
    }

    @Override
    String getPrelimCalculateProfile() {
        return SettlementJobProfile.CALC_MONTHLY_PRELIM_STL_AMTS;
    }

    @Override
    String getFinalCalculateProfile() {
        return SettlementJobProfile.CALC_MONTHLY_FINAL_STL_AMTS;
    }

    @Override
    String getAdjustedMtrAdjCalculateProfile() {
        return SettlementJobProfile.CALC_MONTHLY_ADJ_STL_AMTS_MTR_ADJ;
    }

    @Override
    String getAdjustedMtrFinCalculateProfile() {
        return SettlementJobProfile.CALC_MONTHLY_ADJ_STL_AMTS_MTR_FIN;
    }

    @Override
    Map<String, String> getCalculateStepsForSkipLogs() {
        Map<String, String> calcSteps = new LinkedHashMap<>();
        calcSteps.put(DISAGGREGATE_BCQ, "Disaggregate BCQ");
        calcSteps.put(CALC_SCALING_FACTOR, "Calculate Scaling Factor");

        return calcSteps;
    }

    private Map<String, String> getCalculateLrStepsForSkipLogs() {
        Map<String, String> calcLrSteps = new LinkedHashMap<>();
        calcLrSteps.put(CALC_BUYER_LINE_RENTAL, "Calculate Buyer Line Rental");
        calcLrSteps.put(CALC_SELLER_LINE_RENTAL, "Calculate Seller Line Rental");

        return calcLrSteps;
    }

    @Override
    String getPrelimTaggingProfile() {
        return SettlementJobProfile.TAG_MONTHLY_PRELIM;
    }

    @Override
    String getFinalTaggingProfile() {
        return SettlementJobProfile.TAG_MONTHLY_FINAL;
    }

    @Override
    String getAdjustedTaggingProfile() {
        return SettlementJobProfile.TAG_MONTHLY_ADJ;
    }

    @Override
    String getPrelimGenFileProfile() {
        return SettlementJobProfile.GEN_FILE_PRELIM;
    }

    @Override
    String getFinalGenFileProfile() {
        return SettlementJobProfile.GEN_FILE_FINAL;
    }

    @Override
    String getAdjustedGenFileProfile() {
        return SettlementJobProfile.GEN_FILE_ADJ;
    }
}
