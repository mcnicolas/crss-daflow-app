package com.pemc.crss.dataflow.app.service;


public interface AddtlCompJobService {
    void deleteJob(long jobId, String mtn, String pricingCondition, String groupId);
}
