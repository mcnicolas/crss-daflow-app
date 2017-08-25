package com.pemc.crss.dataflow.app.dto.parent;

import com.pemc.crss.dataflow.app.dto.TaskExecutionDto;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class GroupTaskExecutionDto {

    private String billingPeriod;
    private Date date;
    private Date startDate;
    private Date endDate;
    private String billingPeriodName;
    private String processType;
    // this list should contain all the same billing period or within the billing period
    private List<TaskExecutionDto> taskExecutionDtoList = new ArrayList<>();

    public GroupTaskExecutionDto() {
    }

    public GroupTaskExecutionDto(Date date, String processType) {
        this.date = date;
        this.processType = processType;
    }

    // Monthly
    public GroupTaskExecutionDto(String billingPeriodName, Date startDate, Date endDate, String processType) {
        this.billingPeriodName = billingPeriodName;
        this.startDate = startDate;
        this.endDate = endDate;
        this.processType = processType;
    }

    public String getBillingPeriod() {
        return billingPeriod;
    }

    public void setBillingPeriod(String billingPeriod) {
        this.billingPeriod = billingPeriod;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public Date getStartDate() {
        return startDate;
    }

    public void setStartDate(Date startDate) {
        this.startDate = startDate;
    }

    public Date getEndDate() {
        return endDate;
    }

    public void setEndDate(Date endDate) {
        this.endDate = endDate;
    }

    public String getBillingPeriodName() {
        return billingPeriodName;
    }

    public void setBillingPeriodName(String billingPeriodName) {
        this.billingPeriodName = billingPeriodName;
    }

    public String getProcessType() {
        return processType;
    }

    public void setProcessType(String processType) {
        this.processType = processType;
    }

    public List<TaskExecutionDto> getTaskExecutionDtoList() {
        return taskExecutionDtoList;
    }

    public void setTaskExecutionDtoList(List<TaskExecutionDto> taskExecutionDtoList) {
        this.taskExecutionDtoList = taskExecutionDtoList;
    }
}
