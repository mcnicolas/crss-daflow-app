package com.pemc.crss.restresource.app.service.impl.importconfig;

import com.pemc.crss.restresource.app.dto.ImportConfigResponseDto;
import com.pemc.crss.restresource.app.exception.InvalidFileException;
import com.pemc.crss.restresource.app.exception.InvalidMtnException;
import com.pemc.crss.restresource.app.util.CsvParserUtil;
import com.pemc.crss.restresource.app.util.ValidationUtil;
import com.pemc.crss.meterprocess.core.main.entity.mtn.ConfigImport;
import com.pemc.crss.meterprocess.core.main.entity.mtn.MTNModelConfig;
import com.pemc.crss.meterprocess.core.main.repository.MTNModelConfigRepository;
import com.pemc.crss.shared.core.registration.entity.model.MarketTradingNode;
import com.pemc.crss.shared.core.registration.repository.MarketTradingNodeRepository;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.pemc.crss.restresource.app.util.Constants.*;

@Service
public class ImportMtnModelConfigServiceImpl extends AbstractImportConfigService {

    private static final Logger LOG = LoggerFactory.getLogger(ImportMtnModelConfigServiceImpl.class);

    @Autowired
    private MarketTradingNodeRepository marketTradingNodeRepository;

    @Autowired
    private MTNModelConfigRepository mtnModelConfigRepository;

    @Override
    public ImportConfigResponseDto importConfig(String configName, MultipartFile configFile, Date importDate) {
        ImportConfigResponseDto response = new ImportConfigResponseDto();
        String status = FAILED;

        try {
            ValidationUtil.validateConfigFile(configFile);

            LOG.info("Importing config file: " + configFile.getOriginalFilename()
                    + " to mtn config model..");

            List<CSVRecord> csvRecords = CsvParserUtil.parseCsvFile(configFile);

            Set<MTNModelConfig> mtnModelConfigs = new HashSet<>();

            for (int i = 0; i < csvRecords.size(); i++) {
                CSVRecord record = csvRecords.get(i);

                if (!record.isSet(MTN_CONFIG_NAME_FILE_HEADER) || !record.isSet(MTN_NAME_FILE_HEADER)) {
                    throw new InvalidFileException("Record " + (i + 1) + " has no mtn config name and/or" +
                            " mtn name.");
                }

                String mtnConfigName = record.get(MTN_CONFIG_NAME_FILE_HEADER);
                String mtnName = record.get(MTN_NAME_FILE_HEADER);

                if (StringUtils.isEmpty(mtnConfigName) || !mtnConfigName.matches(IMPORT_STRING_INPUT_REGEX)) {
                    throw new InvalidMtnException("Invalid MTN Config Name input.");
                } else if (StringUtils.isEmpty(mtnName) || !mtnName.matches(IMPORT_STRING_INPUT_REGEX)) {
                    throw new InvalidMtnException("Invalid MTN input.");
                }

                //check if the mtn exists in the database
                MarketTradingNode mtn = marketTradingNodeRepository.findByName(mtnName);
                if (mtn == null) {
                    throw new InvalidMtnException("Record " + (i + 1) + ": MTN name is not found in CRSS");
                }

                if (!mtn.isActive()) {
                    throw new InvalidMtnException("Record " + (i + 1) + ": MTN is not active.");
                }

                //check if the mtn model config exists in the database
                if (mtnModelConfigRepository.findByMtnConfigName(mtnConfigName) != null) {
                    throw new InvalidMtnException("Record " + (i + 1) + ": MTN Model Config already exists in" +
                            " the database.");
                }

                MTNModelConfig mtnModelConfig = new MTNModelConfig();
                mtnModelConfig.setMtnConfigName(mtnConfigName);
                mtnModelConfig.setMtnName(mtnName);

                if (!mtnModelConfigs.add(mtnModelConfig)) {
                    throw new InvalidMtnException("Record " + (i + 1) + ": MTN Model Config already " +
                            "exists in the import file.");
                }
            }

            mtnModelConfigRepository.save(mtnModelConfigs);
            status = COMPLETED;
            LOG.info(configFile.getOriginalFilename() + " is imported.");

        } catch (Exception e) {
            LOG.error("Exception : " + e.getMessage());
            response.setRemarks(e.getMessage());
        }

        response.setStatus(status);
        super.saveImport(new ConfigImport(importDate, configName, configFile.getOriginalFilename(), status, ConfigImport.METERING));
        return response;
    }
}
