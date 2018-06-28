package com.pemc.crss.dataflow.app.jobqueue;

import com.pemc.crss.shared.commons.reference.MeterProcessType;
import com.pemc.crss.shared.core.dataflow.reference.QueueStatus;

import java.util.Arrays;
import java.util.List;

import static com.pemc.crss.shared.core.dataflow.reference.QueueStatus.*;

public interface SchedulerService {

    void execute();

    // Define here jobs groups that can run in parallel

    // including here DEFAULT types just in case
    List<MeterProcessType> DAILY_AND_AC = Arrays.asList(MeterProcessType.DAILY, MeterProcessType.AC, MeterProcessType.DEFAULT);

    List<MeterProcessType> MONTHLY = Arrays.asList(MeterProcessType.PRELIM, MeterProcessType.PRELIMINARY,
            MeterProcessType.FINAL, MeterProcessType.ADJUSTED, MeterProcessType.MONTHLY);

    List<QueueStatus> IN_PROGRESS_STATUS = Arrays.asList(ON_QUEUE, STARTED, STARTING);
}
