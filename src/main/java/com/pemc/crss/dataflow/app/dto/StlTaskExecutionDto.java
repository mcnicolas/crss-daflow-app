package com.pemc.crss.dataflow.app.dto;

import org.springframework.batch.core.BatchStatus;

import java.util.Map;

@Deprecated
public class StlTaskExecutionDto extends BaseTaskExecutionDto {

    private BatchStatus stlReadyStatus;

    private Long currentlyRunningId;

    private Long latestAdjustmentId;

    private StlJobGroupDto parentStlJobGroupDto;

    private Map<Long, StlJobGroupDto> stlJobGroupDtoMap;

    public BatchStatus getStlReadyStatus() {
        return stlReadyStatus;
    }

    public void setStlReadyStatus(BatchStatus stlReadyStatus) {
        this.stlReadyStatus = stlReadyStatus;
    }

    public Long getCurrentlyRunningId() {
        return currentlyRunningId;
    }

    public void setCurrentlyRunningId(Long currentlyRunningId) {
        this.currentlyRunningId = currentlyRunningId;
    }

    public Long getLatestAdjustmentId() {
        return latestAdjustmentId;
    }

    public void setLatestAdjustmentId(Long latestAdjustmentId) {
        this.latestAdjustmentId = latestAdjustmentId;
    }

    public StlJobGroupDto getParentStlJobGroupDto() {
        return parentStlJobGroupDto;
    }

    public void setParentStlJobGroupDto(StlJobGroupDto parentStlJobGroupDto) {
        this.parentStlJobGroupDto = parentStlJobGroupDto;
    }

    public Map<Long, StlJobGroupDto> getStlJobGroupDtoMap() {
        return stlJobGroupDtoMap;
    }

    public void setStlJobGroupDtoMap(Map<Long, StlJobGroupDto> stlJobGroupDtoMap) {
        this.stlJobGroupDtoMap = stlJobGroupDtoMap;
    }
}
