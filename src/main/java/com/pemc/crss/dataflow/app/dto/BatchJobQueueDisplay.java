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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

@Data
@NoArgsConstructor
public class BatchJobQueueDisplay {

    private Long id;
    private LocalDateTime queueDate;
    private Module module;
    private JobProcess jobProcess;
    private QueueStatus status;
    private String user;
    private Map<String, String> detailsMap;

    public BatchJobQueueDisplay(BatchJobQueue batchJobQueue) {
        this.id = batchJobQueue.getId();
        this.queueDate = batchJobQueue.getQueueDate();
        this.module = batchJobQueue.getModule();
        this.jobProcess = batchJobQueue.getJobProcess();
        this.status = batchJobQueue.getStatus();
        this.user = batchJobQueue.getUsername();
        this.detailsMap = buildRunDetails(batchJobQueue);
    }

    private Map<String, String> buildRunDetails(final BatchJobQueue jobQueue) {
        Map<String, String> detailsMap = new LinkedHashMap<>();
        TaskRunDto taskRunDto = ModelMapper.toModel(jobQueue.getTaskObj(), TaskRunDto.class);

        switch (jobQueue.getModule()) {
            case SETTLEMENT:
                putIfPresent(detailsMap, "Process Type", taskRunDto.getMeterProcessType());
                if (Objects.equals(taskRunDto.getMeterProcessType(), MeterProcessType.DAILY.name())) {
                    putIfPresent(detailsMap, "Trading Date", taskRunDto.getTradingDate());
                } else {
                    putIfPresent(detailsMap, "Start Date",  taskRunDto.getStartDate());
                    putIfPresent(detailsMap, "End Date", taskRunDto.getEndDate());
                }
                break;
            case METERING:
                putIfPresent(detailsMap, "Process Type", taskRunDto.getMeterProcessType());
                if (Objects.equals(taskRunDto.getMeterProcessType(), MeterProcessType.DAILY.name())) {
                    putIfPresent(detailsMap, "Trading Date", taskRunDto.getTradingDate());
                } else {
                    putIfPresent(detailsMap, "Start Date",  taskRunDto.getStartDate());
                    putIfPresent(detailsMap, "End Date", taskRunDto.getEndDate());
                }
                putIfPresent(detailsMap, "Meter Type", taskRunDto.getMeterType());
                putIfPresent(detailsMap, "MSP", taskRunDto.getMsp());
                putIfPresent(detailsMap, "SEIN", taskRunDto.getSeins());
                putIfPresent(detailsMap, "MTN", taskRunDto.getMtns());
                break;
            default:
                // do nothing
        }

        return detailsMap;
    }

    private void putIfPresent(final Map<String, String> map, final String key, final String value) {
        if (StringUtils.isNotEmpty(value)) {
            map.put(key, value);
        }
    }

}
