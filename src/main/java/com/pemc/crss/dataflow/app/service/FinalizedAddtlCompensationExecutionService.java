package com.pemc.crss.dataflow.app.service;

import com.pemc.crss.dataflow.app.support.PageableRequest;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Created by jadona on 4/22/21.
 */
public interface FinalizedAddtlCompensationExecutionService {
    void exportJobInstances(PageableRequest pageableRequest, HttpServletResponse response) throws IOException;
}
