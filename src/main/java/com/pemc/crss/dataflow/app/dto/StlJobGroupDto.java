package com.pemc.crss.dataflow.app.dto;

import com.pemc.crss.dataflow.app.support.StlJobStage;
import com.pemc.crss.shared.commons.util.DateUtil;
import lombok.Data;
import org.springframework.batch.core.BatchStatus;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

@Data
public class StlJobGroupDto {

    private BatchStatus gmrVatMFeeCalculationStatus;
    private BatchStatus taggingStatus;
    private BatchStatus invoiceGenerationStatus;
    private String status;

    @Deprecated
    private boolean currentlyRunning;
    @Deprecated
    private boolean latestAdjustment;

    private boolean header;
    private String groupId;
    private Date runStartDateTime;
    private Date runEndDateTime;
    private Long runId;

    // progress bar and status
    private Date latestJobExecStartDate;
    private Date latestJobExecEndDate;
    private List<String> runningSteps;

    // folder in sftp server where files are uploaded
    private String invoiceGenFolder;

    private String billingPeriodStr;

    // determines if job with group id and parent id is locked (applied only to FINAL / ADJUSTED)
    private boolean locked = false;
    private boolean canRunAdjustment = false;

    private List<JobCalculationDto> jobCalculationDtos = new ArrayList<>();

    private SortedSet<LocalDate> remainingDatesCalc = new TreeSet<>();

    private SortedSet<LocalDate> remainingDatesGenInputWs = new TreeSet<>();

    private boolean runningStlCalculation;

    private boolean runningGenInputWorkspace;

    private Date maxPartialCalcRunDate;

    private Date maxPartialGenIwRunDate;

    private Date gmrCalcRunDate;

    private boolean hasCompletedGenInputWs;

    // helper methods / properties

    public String getRunStartDateTimeStr() {
        return runEndDateTime != null
                ? DateUtil.convertToString(runStartDateTime, DateUtil.DEFAULT_DATETIME_FORMAT) : null;
    }

    public String getRunEndDateTimeStr() {
        return runEndDateTime != null
                ? DateUtil.convertToString(runEndDateTime, DateUtil.DEFAULT_DATETIME_FORMAT) : null;
    }

    // consider gmr/vat recalculation if max partial calculation runDate > gmr calculation runDate
    public boolean isForGmrRecalculation() {
        return (maxPartialCalcRunDate != null && gmrCalcRunDate != null) &&
                maxPartialCalcRunDate.compareTo(gmrCalcRunDate) > 0;
    }

    public boolean isCalculateGmrIsLatestJob() {
        return !getSortedJobCalculationDtos().isEmpty() &&
                Objects.equals(getSortedJobCalculationDtos().get(0).getJobStage(), StlJobStage.CALCULATE_GMR);
    }

    public List<JobCalculationDto> getSortedJobCalculationDtos() {
        return jobCalculationDtos
                .stream()
                // sort by runDate desc
                .sorted(Collections.reverseOrder(Comparator.comparing(JobCalculationDto::getRunDate)))
                .collect(Collectors.toList());
    }
}
