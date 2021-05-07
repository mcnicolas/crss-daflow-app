package com.pemc.crss.dataflow.app.service.impl;

import com.pemc.crss.dataflow.app.service.AddtlCompJobService;
import com.pemc.crss.shared.core.dataflow.entity.AddtlCompParams;
import com.pemc.crss.shared.core.dataflow.repository.AddtlCompParamsRepository;
import com.pemc.crss.shared.core.dataflow.repository.ExecutionParamRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
public class AddtlCompJobServiceImpl implements AddtlCompJobService {

    @Autowired
    protected ExecutionParamRepository executionParamRepository;

    @Autowired
    private AddtlCompParamsRepository addtlCompParamsRepository;


    @Override
    @Transactional(value = "transactionManager")
    public void deleteJob(long runId, String mtn, String pricingCondition, String groupId) {
        //for addtlcomp, Job ID in UI is the runId job params.

        Long jobId = executionParamRepository.findJobIdByRunId(runId);
        AddtlCompParams acParams = addtlCompParamsRepository.findTopByMtnAndPricingConditionAndGroupIdOrderByBillingStartDateDesc(mtn, pricingCondition, groupId);

        if (jobId != null && acParams != null) {
            addtlCompParamsRepository.delete(acParams);
            executionParamRepository.deleteCascadeJob(jobId);
        }
    }

}
