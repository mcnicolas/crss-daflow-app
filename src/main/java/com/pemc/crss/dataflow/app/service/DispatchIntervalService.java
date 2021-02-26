package com.pemc.crss.dataflow.app.service;

import com.pemc.crss.dataflow.app.support.PageableRequest;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Created by jadona on 3/4/21.
 */
public interface DispatchIntervalService {
    void exportBatchJobSkipLogs(PageableRequest pageableRequest, HttpServletResponse response) throws IOException;

    void exportProcessedLogs(PageableRequest pageableRequest, HttpServletResponse response) throws IOException;
}
