package com.pemc.crss.dataflow.app.dto;

import com.pemc.crss.shared.commons.reference.MeterProcessType;
import com.pemc.crss.shared.commons.util.ModelMapper;
import com.pemc.crss.shared.commons.util.reference.Module;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobQueue;
import com.pemc.crss.shared.core.dataflow.reference.JobProcess;
import com.pemc.crss.shared.core.dataflow.reference.QueueStatus;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.pemc.crss.shared.core.dataflow.reference.JobProcess.*;

@Data
@NoArgsConstructor
public class BatchJobQueueDisplay {

    private Long id;
    private LocalDateTime queueDate;
    private LocalDateTime jobExecStart;
    private LocalDateTime jobExecEnd;
    private Module module;
    private JobProcess jobProcess;
    private QueueStatus status;
    private String user;
    private String details;
    private Map<String, String> paramMap;
    private Long meteringParentId;

    public BatchJobQueueDisplay(BatchJobQueue batchJobQueue) {
        this.id = batchJobQueue.getId();
        this.queueDate = batchJobQueue.getQueueDate();
        this.module = batchJobQueue.getModule();
        this.jobProcess = batchJobQueue.getJobProcess();
        this.status = batchJobQueue.getStatus();
        this.user = batchJobQueue.getUsername();
        this.paramMap = buildRunDetails(batchJobQueue);
        this.details = batchJobQueue.getDetails();
        this.jobExecStart = batchJobQueue.getJobExecStart();
        this.jobExecEnd = batchJobQueue.getJobExecEnd();
    }

    private Map<String, String> buildRunDetails(final BatchJobQueue jobQueue) {
        Map<String, String> paramMap = new LinkedHashMap<>();
        TaskRunDto taskRunDto = ModelMapper.toModel(jobQueue.getTaskObj(), TaskRunDto.class);
        JobProcess jobProcess = jobQueue.getJobProcess();

        switch (jobQueue.getModule()) {
            case SETTLEMENT:
                if (jobQueue.getMeterProcessType() == MeterProcessType.AC) {
                    paramMap.put("Process Type", MeterProcessType.AC.name());
                    putIfPresent(paramMap, "Pricing Condition", taskRunDto.getPricingCondition());
                    if (JobProcess.CALC_AC == jobProcess) {
                        putIfPresent(paramMap, "Start Date", taskRunDto.getBillingStartDate());
                        putIfPresent(paramMap, "End Date", taskRunDto.getBillingEndDate());
                        putIfPresent(paramMap, "Billing ID", taskRunDto.getBillingId());
                        putIfPresent(paramMap, "MTN", taskRunDto.getMtn());
                    } else {
                        putIfPresent(paramMap, "Start Date", taskRunDto.getStartDate());
                        putIfPresent(paramMap, "End Date", taskRunDto.getEndDate());
                    }
                } else {
                    // file gen jobs use baseStartDate / baseEndDate
                    final List<JobProcess> jobProcessThatUseBaseDates = Arrays.asList(
                            GEN_ENERGY_FILES, GEN_RESERVE_FILES, GEN_LR_FILES, GEN_FILES_EMF, GEN_FILES_RMF, CALC_GMR_VAT,
                            CALC_RGMR_VAT, FINALIZE_TA , FINALIZE_RTA, FINALIZE_LR, FINALIZE_EMF, FINALIZE_RMF,
                            GEN_MONTHLY_SUMMARY_TA, GEN_MONTHLY_SUMMARY_RTA, STL_VALIDATION, CALC_ALLOC,
                            CALC_ALLOC_RESERVE, GEN_ALLOC_REPORT, GEN_ALLOC_REPORT_RESERVE);
                    putIfPresent(paramMap, "Process Type", taskRunDto.getMeterProcessType());
                    if (Objects.equals(taskRunDto.getMeterProcessType(), MeterProcessType.DAILY.name())) {
                        putIfPresent(paramMap, "Trading Date", taskRunDto.getTradingDate());
                    } else {
                        putIfPresent(paramMap, "Start Date", jobProcessThatUseBaseDates.contains(jobProcess) ?
                                taskRunDto.getBaseStartDate() : taskRunDto.getStartDate());
                        putIfPresent(paramMap, "End Date", jobProcessThatUseBaseDates.contains(jobProcess) ?
                                taskRunDto.getBaseEndDate() : taskRunDto.getEndDate());
                    }
                }
                putIfPresent(paramMap,"Region", taskRunDto.getRegionGroup());
                break;
            case METERING:
                if (taskRunDto.getParentJob() != null) {
                    this.meteringParentId = Long.valueOf(taskRunDto.getParentJob());
                }

                if (taskRunDto.getMeterProcessType() != null) {
                    putIfPresent(paramMap, "Process Type", taskRunDto.getMeterProcessType());
                } else {
                    paramMap.put("Process Type", "DAILY");
                }

                // DAILY
                if (Objects.equals(taskRunDto.getMeterProcessType(), null) ||
                        Objects.equals(taskRunDto.getMeterProcessType(), MeterProcessType.DAILY.name())) {
                    putIfPresent(paramMap, "Trading Date", taskRunDto.getTradingDate());
                } else {
                    // for PRELIM / FINAL / ADJUSTED processTypes
                    putIfPresent(paramMap, "Start Date",  taskRunDto.getStartDate());
                    putIfPresent(paramMap, "End Date", taskRunDto.getEndDate());
                }
                putIfPresent(paramMap, "Meter Type", taskRunDto.getMeterType());
                putIfPresent(paramMap, "MSP", taskRunDto.getMsp());
                putIfPresent(paramMap, "SEIN", taskRunDto.getSeins());

                if (jobProcess == RUN_WESM) {
                    // no selected MTNS means "ALL" mtns are included
                    String mtns = StringUtils.isNotEmpty(taskRunDto.getMtns()) ? taskRunDto.getMtns() : "ALL";
                    paramMap.put("MTN", mtns);
                } else {
                    putIfPresent(paramMap, "MTN", taskRunDto.getMtns());
                }
                putIfPresent(paramMap,"Region Group", taskRunDto.getRegionGroup());
                putIfPresent(paramMap,"Region", taskRunDto.getRegion());
                break;
            default:
                // do nothing
        }

        return paramMap;
    }

    private void putIfPresent(final Map<String, String> map, final String key, final String value) {
        if (StringUtils.isNotEmpty(value)) {
            map.put(key, value);
        }
    }

}
