package com.pemc.crss.dataflow.app.service;


import com.pemc.crss.dataflow.app.dto.TaskRunDto;

public interface AddtlCompJobService {
    void deleteJob(long jobId, String mtn, String pricingCondition, String groupId);

    Boolean validateBillingPeriod(TaskRunDto taskRunDto);
}
