package com.pemc.crss.dataflow.app.support;

import java.util.Arrays;
import java.util.List;

// process type passed from ui
public enum StlQueryProcessType {
    ALL,
    ALL_MONTHLY,
    ADJUSTED,
    DAILY,
    PRELIM,
    FINAL;

    public static final List<StlQueryProcessType> MONTHLY_PROCESS_TYPES = Arrays.asList(ADJUSTED, PRELIM, FINAL);
}
