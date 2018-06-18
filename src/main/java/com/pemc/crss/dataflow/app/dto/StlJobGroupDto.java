package com.pemc.crss.dataflow.app.dto;

import com.pemc.crss.dataflow.app.support.StlJobStage;
import com.pemc.crss.shared.commons.util.DateUtil;
import lombok.Data;
import org.springframework.batch.core.BatchStatus;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static com.pemc.crss.dataflow.app.support.StlJobStage.CALCULATE_GMR;
import static com.pemc.crss.dataflow.app.support.StlJobStage.CALCULATE_LR;
import static com.pemc.crss.dataflow.app.support.StlJobStage.FINALIZE_LR;

@Data
public class StlJobGroupDto {

    private BatchStatus gmrVatMFeeCalculationStatus;
    private BatchStatus taggingStatus;
    private BatchStatus invoiceGenerationStatus;
    private BatchStatus invoiceGenerationRsvTaStatus;
    private String status;

    private String lineRentalTopStatus;
    private BatchStatus taggingLrStatus;
    private BatchStatus invoiceGenerationLrStatus;

    private boolean header;
    private String groupId;
    private Date runStartDateTime;
    private Date runEndDateTime;
    private Date runEndDateTimeFileRsvTa;
    private Date runEndDateTimeFileLr;
    private Long runId;

    // progress bar and status
    private Date latestJobExecStartDate;
    private Date latestJobExecEndDate;
    private List<String> runningSteps;

    // folder in sftp server where files are uploaded
    private String invoiceGenFolder;
    private String invoiceGenFolderRsvTa;
    private String invoiceGenFolderLr;

    private String billingPeriodStr;

    // determines if job with group id and parent id is locked (applied only to FINAL / ADJUSTED)
    private boolean locked = false;

    // determines if line rental calculation is finalized
    private boolean lockedLr = false;

    private boolean canRunAdjustment = false;

    private List<JobCalculationDto> jobCalculationDtos = new ArrayList<>();

    private SortedSet<LocalDate> remainingDatesCalc = new TreeSet<>();

    private SortedSet<LocalDate> remainingDatesGenInputWs = new TreeSet<>();

    private SortedSet<LocalDate> remainingDatesCalcLr = new TreeSet<>();

    private SortedSet<LocalDate> outdatedTradingDates = new TreeSet<>();

    private boolean runningStlCalculation;

    private boolean runningGenInputWorkspace;

    private boolean runningLrCalculation;

    private Date maxPartialCalcRunDate;

    private Date maxPartialGenIwRunDate;

    private Date maxPartialCalcLrRunDate;

    private Date gmrCalcRunDate;

    private boolean hasCompletedGenInputWs;

    private String regionGroup;

    // helper methods / properties

    public String getRunStartDateTimeStr() {
        return runEndDateTime != null
                ? DateUtil.convertToString(runStartDateTime, DateUtil.DEFAULT_DATETIME_FORMAT) : null;
    }

    public String getRunEndDateTimeStr() {
        return runEndDateTime != null
                ? DateUtil.convertToString(runEndDateTime, DateUtil.DEFAULT_DATETIME_FORMAT) : null;
    }

    public String getRunEndDateTimeFileRsvTaStr() {
        return runEndDateTimeFileRsvTa != null
                ? DateUtil.convertToString(runEndDateTimeFileRsvTa, DateUtil.DEFAULT_DATETIME_FORMAT) : null;
    }

    public String getRunEndDateTimeFileLrStr() {
        return runEndDateTimeFileLr != null
                ? DateUtil.convertToString(runEndDateTimeFileLr, DateUtil.DEFAULT_DATETIME_FORMAT) : null;
    }

    // consider gmr/vat recalculation if max partial calculation runDate > gmr calculation runDate
    public boolean isForGmrRecalculation() {
        return (maxPartialCalcRunDate != null && gmrCalcRunDate != null) &&
                maxPartialCalcRunDate.compareTo(gmrCalcRunDate) > 0;
    }

    public boolean isCalculateGmrIsLatestJob() {
        List<StlJobStage> lineRentalJobStages = Arrays.asList(CALCULATE_LR, FINALIZE_LR);

        List<JobCalculationDto> sortedCalcDtosWithoutLr = !jobCalculationDtos.isEmpty() ?
                jobCalculationDtos.stream()
                        .filter(jobDto -> !lineRentalJobStages.contains(jobDto.getJobStage()))
                        .sorted(Collections.reverseOrder(Comparator.comparing(JobCalculationDto::getRunDate)))
                        .collect(Collectors.toList()) : new ArrayList<>();

        return !sortedCalcDtosWithoutLr.isEmpty()
                && Objects.equals(sortedCalcDtosWithoutLr.get(0).getJobStage(), CALCULATE_GMR);
    }

    public List<JobCalculationDto> getSortedJobCalculationDtos() {
        return jobCalculationDtos
                .stream()
                // sort by runDate desc
                .sorted(Collections.reverseOrder(Comparator.comparing(JobCalculationDto::getRunDate)))
                .collect(Collectors.toList());
    }
}
