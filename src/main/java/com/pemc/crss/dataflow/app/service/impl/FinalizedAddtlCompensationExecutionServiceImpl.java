package com.pemc.crss.dataflow.app.service.impl;

import com.pemc.crss.dataflow.app.dto.AddtlCompensationExecutionDto;
import com.pemc.crss.dataflow.app.dto.FinalizedAC;
import com.pemc.crss.dataflow.app.service.FinalizedAddtlCompensationExecutionService;
import com.pemc.crss.dataflow.app.support.PageableRequest;
import com.pemc.crss.dataflow.app.util.ExportFinalizedACUtil;
import com.pemc.crss.dataflow.app.util.WebCsvUtils;
import com.pemc.crss.shared.core.dataflow.repository.ExecutionParamRepository;
import com.pemc.crss.shared.core.dataflow.repository.ViewTxnOutputAddtlCompensationRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.supercsv.cellprocessor.ift.CellProcessor;

import javax.servlet.http.HttpServletResponse;
import javax.transaction.Transactional;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by jadona on 4/22/21.
 */
@Slf4j
@Service("finalizedAddtlCompensationExecutionService")
@Transactional
public class FinalizedAddtlCompensationExecutionServiceImpl implements FinalizedAddtlCompensationExecutionService {
    @Autowired
    @Qualifier("addtlCompensationExecutionService")
    private AddtlCompensationExecutionServiceImpl addtlCompensationExecutionService;

    @Autowired
    private ViewTxnOutputAddtlCompensationRepository viewTxnOutputAddtlCompensationRepository;

    @Autowired
    private ExecutionParamRepository executionParamRepository;

    @Override
    public void exportJobInstances(PageableRequest pageableRequest, HttpServletResponse response) throws IOException {
        log.info("exportJobInstances({}, {})", pageableRequest, response);

        Pageable pageable = pageableRequest.getPageable();
        Map<String, String> mapParams = pageableRequest.getMapParams();
        String filename = ExportFinalizedACUtil.getFilename();
        List<AddtlCompensationExecutionDto> addtlCompensationExecutionDtoList = addtlCompensationExecutionService.getAddtlCompensationExecutionDtoList(pageable, mapParams);
        List<FinalizedAC> finalizedACList = getFinalizedACList(addtlCompensationExecutionDtoList);
        String[] headers = ExportFinalizedACUtil.getHeaders();
        String[] fields = ExportFinalizedACUtil.getFields();
        CellProcessor[] processors = ExportFinalizedACUtil.getCellProcessors();

        WebCsvUtils.writeToCsv(filename, finalizedACList, response, headers, fields, processors);
    }

    private List<FinalizedAC> getFinalizedACList(List<AddtlCompensationExecutionDto> addtlCompensationExecutionDtoList) {
        log.info("getFinalizedACList({})", addtlCompensationExecutionDtoList);

        FinalizedAC.setViewTxnOutputAddtlCompensationRepository(viewTxnOutputAddtlCompensationRepository);
        FinalizedAC.setExecutionParamRepository(executionParamRepository);

        return addtlCompensationExecutionDtoList.stream().flatMap(FinalizedAC::getFinalizedACStream).collect(Collectors.toList());
    }
}
