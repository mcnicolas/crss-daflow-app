package com.pemc.crss.dataflow.app.dto;

import com.pemc.crss.dataflow.app.dto.parent.StubTaskExecutionDto;
import org.springframework.batch.core.BatchStatus;

import java.util.Date;
import java.util.Map;

public class SettlementTaskExecutionDto extends StubTaskExecutionDto {

    private Long parentId;

    private Long stlReadyJobId;

    private String status;

    private Date runDateTime;

    private BatchStatus stlReadyStatus;

    private StlJobGroupDto parentStlJobGroupDto;

    private Map<Long, StlJobGroupDto> stlJobGroupDtoMap;

    private Date billPeriodStartDate;

    private Date billPeriodEndDate;

    private Date dailyDate;

    private String processType;

    public Long getParentId() {
        return parentId;
    }

    public void setParentId(Long parentId) {
        this.parentId = parentId;
    }

    public Long getStlReadyJobId() {
        return stlReadyJobId;
    }

    public void setStlReadyJobId(Long stlReadyJobId) {
        this.stlReadyJobId = stlReadyJobId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Date getRunDateTime() {
        return runDateTime;
    }

    public void setRunDateTime(Date runDateTime) {
        this.runDateTime = runDateTime;
    }

    public BatchStatus getStlReadyStatus() {
        return stlReadyStatus;
    }

    public void setStlReadyStatus(BatchStatus stlReadyStatus) {
        this.stlReadyStatus = stlReadyStatus;
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

    public Date getBillPeriodStartDate() {
        return billPeriodStartDate;
    }

    public void setBillPeriodStartDate(Date billPeriodStartDate) {
        this.billPeriodStartDate = billPeriodStartDate;
    }

    public Date getBillPeriodEndDate() {
        return billPeriodEndDate;
    }

    public void setBillPeriodEndDate(Date billPeriodEndDate) {
        this.billPeriodEndDate = billPeriodEndDate;
    }

    public Date getDailyDate() {
        return dailyDate;
    }

    public void setDailyDate(Date dailyDate) {
        this.dailyDate = dailyDate;
    }

    public String getProcessType() {
        return processType;
    }

    public void setProcessType(String processType) {
        this.processType = processType;
    }
}
