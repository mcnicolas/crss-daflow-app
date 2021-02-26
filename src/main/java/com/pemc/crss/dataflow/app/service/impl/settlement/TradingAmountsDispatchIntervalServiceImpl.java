package com.pemc.crss.dataflow.app.service.impl.settlement;

import com.pemc.crss.dataflow.app.service.DispatchIntervalService;
import com.pemc.crss.dataflow.app.support.PageableRequest;
import com.pemc.crss.dataflow.app.util.ExportDispatchIntervalUtil;
import com.pemc.crss.dataflow.app.util.WebCsvUtils;
import com.pemc.crss.shared.core.dataflow.dto.ProcessedInterval;
import com.pemc.crss.shared.core.dataflow.dto.SkippedInterval;
import com.pemc.crss.shared.core.dataflow.entity.BatchJobSkipLog;
import com.pemc.crss.shared.core.dataflow.entity.ViewTxnOutputAddtlCompensation;
import com.pemc.crss.shared.core.dataflow.repository.BatchJobSkipLogRepository;
import com.pemc.crss.shared.core.dataflow.repository.ViewTxnOutputAddtlCompensationRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.supercsv.cellprocessor.ift.CellProcessor;

import javax.servlet.http.HttpServletResponse;
import javax.transaction.Transactional;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by jadona on 3/4/21.
 */
@Slf4j
@Service("tradingAmountsDispatchIntervalService")
@Transactional
public class TradingAmountsDispatchIntervalServiceImpl implements DispatchIntervalService {
    @Autowired
    private BatchJobSkipLogRepository batchJobSkipLogRepository;

    @Autowired
    private ViewTxnOutputAddtlCompensationRepository viewTxnOutputAddtlCompensationRepository;

    @Override
    public void exportBatchJobSkipLogs(PageableRequest page, HttpServletResponse response) throws IOException {
        log.info("exportBatchJobSkipLogs({}, {})", page, response);
        String filename = ExportDispatchIntervalUtil.getSkipLogsFilename(page);
        List<BatchJobSkipLog> batchJobSkipLogsDtos = getBatchJobSkipLogsDtos(page);
        List<SkippedInterval> skippedIntervals = batchJobSkipLogsDtos.stream().map(BatchJobSkipLog::getError).map(SkippedInterval::fromError).collect(Collectors.toList());
        String[] headers = ExportDispatchIntervalUtil.getSkippedHeaders();
        String[] fields = ExportDispatchIntervalUtil.getSkippedFields();
        CellProcessor[] processors = ExportDispatchIntervalUtil.getSkippedCellProcessors();

        WebCsvUtils.writeToCsv(filename, skippedIntervals, response, headers, fields, processors);
    }

    @Override
    public void exportProcessedLogs(PageableRequest page, HttpServletResponse response) throws IOException {
        log.info("exportProcessedLogs({}, {})", page, response);
        String filename = ExportDispatchIntervalUtil.getProcessedLogsFilename(page);
        List<ViewTxnOutputAddtlCompensation> viewTxnOutputAddtlCompensationDtos = getViewTxnOutputAddtlCompensationDtos(page);
        List<ProcessedInterval> processedIntervals = viewTxnOutputAddtlCompensationDtos.stream().map(ProcessedInterval::fromViewTxnOutputAddtlCompensation).collect(Collectors.toList());
        String[] headers = ExportDispatchIntervalUtil.getProcessedHeaders();
        String[] fields = ExportDispatchIntervalUtil.getProcessedFields();
        CellProcessor[] processors = ExportDispatchIntervalUtil.getProcessedCellProcessors();

        WebCsvUtils.writeToCsv(filename, processedIntervals, response, headers, fields, processors);
    }

    private List<ViewTxnOutputAddtlCompensation> getViewTxnOutputAddtlCompensationDtos(PageableRequest page) {
        log.info("getViewTxnOutputAddtlCompensationDtos({})", page);
        String acGroupId = page.getMapParams().get("groupId");
        Long acJobId = Long.valueOf(page.getMapParams().get("acJobId"));

        return viewTxnOutputAddtlCompensationRepository.findByAcGroupIdAndAcJobId(acGroupId, acJobId);
    }

    private List<BatchJobSkipLog> getBatchJobSkipLogsDtos(PageableRequest page) {
        log.info("getBatchJobSkipLogsDtos({})", page);
        String parentStepName = page.getMapParams().get("parentStepName");
        Long jobExecutionId = Long.valueOf(page.getMapParams().get("jobExecutionId"));

        return batchJobSkipLogRepository.findByJobExecutionIdAndParentStepName(jobExecutionId, parentStepName);
    }
}
