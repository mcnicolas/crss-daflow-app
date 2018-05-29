package com.pemc.crss.dataflow.app.jobqueue;

import com.pemc.crss.shared.core.dataflow.reference.JobProcess;
import com.pemc.crss.shared.core.dataflow.reference.QueueStatus;

import java.util.Arrays;
import java.util.List;

import static com.pemc.crss.shared.core.dataflow.reference.JobProcess.*;
import static com.pemc.crss.shared.core.dataflow.reference.QueueStatus.*;

public interface SchedulerService {

    void execute();

    // Define here jobs groups that can run in parallel. For now we'll group them between METERING and STL jobs
    List<JobProcess> METERING_JOBS = Arrays.asList(RUN_WESM, RUN_RCOA, RUN_STL_READY, FINALIZE_STL_READY,
            GEN_MQ_REPORT, GEN_MTR);

    List<JobProcess> STL_JOBS = Arrays.asList(GEN_INPUT_WS_TA, CALC_TA, CALC_GMR_VAT, FINALIZE_TA, GEN_ENERGY_FILES,
            GEN_RESERVE_FILES, STL_VALIDATION, CALC_LR, FINALIZE_LR, GEN_LR_FILES, GEN_INPUT_WS_EMF,
            CALC_EMF, FINALIZE_EMF, GEN_FILES_EMF, GEN_INPUT_WS_RMF, CALC_RMF, FINALIZE_RMF, GEN_FILES_RMF);

    List<QueueStatus> IN_PROGRESS_STATUS = Arrays.asList(ON_QUEUE, STARTED, STARTING);
}
