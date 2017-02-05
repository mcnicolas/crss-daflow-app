package com.pemc.crss.restresource.app.util;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.multipart.MultipartFile;

import java.io.InputStreamReader;
import java.util.List;

public class CsvParserUtil {

    private static final Logger LOG = LoggerFactory.getLogger(CsvParserUtil.class);

    public static List<CSVRecord> parseCsvFile(MultipartFile configFile) throws Exception {
        LOG.info("Parsing " + configFile.getOriginalFilename() + "...");
        CSVParser csvParser = CSVFormat.DEFAULT.withFirstRecordAsHeader().withTrim()
                .parse(new InputStreamReader(configFile.getInputStream()));

        List<CSVRecord> csvRecords = csvParser.getRecords();
        csvParser.close();

        if (csvRecords.isEmpty()) {
            throw new Exception("File has no records.");
        }

        return csvRecords;
    }
}
