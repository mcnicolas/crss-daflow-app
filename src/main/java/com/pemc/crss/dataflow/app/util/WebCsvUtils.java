package com.pemc.crss.dataflow.app.util;

import com.pemc.crss.shared.commons.util.AppTimeUtil;
import org.supercsv.cellprocessor.Optional;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvBeanWriter;
import org.supercsv.io.ICsvBeanWriter;
import org.supercsv.prefs.CsvPreference;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * Created by jadona on 2/26/21.
 */
public class WebCsvUtils {
    public static <T> void writeToCsv(String filename, List<T> rows, HttpServletResponse response, String[] headers, String[] fields,
                                      CellProcessor[] processors) throws IOException {
        String processedFilename = URLEncoder.encode(filename
                + convertToString(AppTimeUtil.now(), "yyyy-MM-dd_hhmmssa")
                + ".csv", "UTF-8");
        processedFilename = URLDecoder.decode(processedFilename, "ISO8859_1");
        response.setContentType("application/x-msdownload");
        response.setHeader("Content-disposition", "attachment; filename=" + processedFilename);
        response.setHeader("Access-Control-Expose-Headers", "Content-Disposition");

        try (ICsvBeanWriter beanWriter = new CsvBeanWriter(new OutputStreamWriter(response.getOutputStream()),
                CsvPreference.STANDARD_PREFERENCE)) {

            if (headers.length != fields.length) {
                throw new RuntimeException(
                        String.format("Headers and fields length does not match :: headers=%d, fields=%d",
                                headers.length, fields.length));
            }

            if (processors == null || processors.length == 0) {
                processors = getDefaultCellProcessors(fields.length);
            }

            beanWriter.writeHeader(headers);

            for (T row : rows) {
                beanWriter.write(row, fields, processors);
            }
        }
    }

    private static CellProcessor[] getDefaultCellProcessors(int length) {
        CellProcessor[] processors = new CellProcessor[length];

        for (int i = 0; i < processors.length; i++) {
            processors[i] = new Optional();
        }

        return processors;
    }

    private static String convertToString(LocalDateTime date, String pattern) {
        return date.format(DateTimeFormatter.ofPattern(pattern));
    }
}
