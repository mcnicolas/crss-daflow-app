package com.pemc.crss.restresource.app.service.impl.importconfig;

import com.pemc.crss.meterprocess.core.main.entity.mtn.ChannelConfig;
import com.pemc.crss.meterprocess.core.main.entity.mtn.ConfigImport;
import com.pemc.crss.meterprocess.core.main.entity.mtn.SettlementMeteringPointConfig;
import com.pemc.crss.meterprocess.core.main.entity.mtn.VirtualMeteringPointConfig;
import com.pemc.crss.meterprocess.core.main.reference.MeterType;
import com.pemc.crss.meterprocess.core.main.repository.RcoaMeteringPointChannelConfigRepository;
import com.pemc.crss.meterprocess.core.main.repository.SettlementMeteringPointConfigDetailRepository;
import com.pemc.crss.meterprocess.core.main.repository.VirtualMeteringPointConfigRepository;
import com.pemc.crss.restresource.app.dto.ImportConfigResponseDto;
import com.pemc.crss.restresource.app.exception.InvalidChannelConfigException;
import com.pemc.crss.restresource.app.exception.InvalidFileException;
import com.pemc.crss.restresource.app.exception.InvalidSeinException;
import com.pemc.crss.restresource.app.util.CsvParserUtil;
import com.pemc.crss.restresource.app.util.ValidationUtil;
import com.pemc.crss.shared.core.registration.entity.model.MeteringRegistration;
import com.pemc.crss.shared.core.registration.repository.RegistrationRepository;
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
public class ImportSettlementMpConfigServiceImpl extends AbstractImportConfigService {

    private static final Logger LOG = LoggerFactory.getLogger(ImportSettlementMpConfigServiceImpl.class);

    @Autowired
    private RegistrationRepository registrationRepository;

    @Autowired
    private RcoaMeteringPointChannelConfigRepository rcoaMeteringPointChannelConfigRepository;

    @Autowired
    private SettlementMeteringPointConfigDetailRepository settlementMeteringPointConfigDetailRepository;

    @Autowired
    private VirtualMeteringPointConfigRepository virtualMeteringPointConfigRepository;

    @Override
    public ImportConfigResponseDto importConfig(String configName, MultipartFile configFile, Date importDate) {

        ImportConfigResponseDto response = new ImportConfigResponseDto();
        String status = FAILED;

        try {
            ValidationUtil.validateConfigFile(configFile);

            LOG.info("Importing config file: " + configFile.getOriginalFilename()
                    + " settlement mp config detail...");

            List<CSVRecord> csvRecords = CsvParserUtil.parseCsvFile(configFile);

            Set<SettlementMeteringPointConfig> configDetails = new HashSet<>();

            for (int i = 0; i < csvRecords.size(); i++) {
                CSVRecord record = csvRecords.get(i);

                if (!record.isSet(SEIN_FILE_HEADER) || !record.isSet(CHANNEL_CONFIG_FILE_HEADER)) {
                    throw new InvalidSeinException("Record " + (i + 1) + " has no sein and/or channel config.");
                }

                if (!ValidationUtil.validateRecordMaxValue(record.size(), 3)) {
                    throw new InvalidFileException("Record " + (i + 1) + " has more than the maximum values.");
                }

                String sein = record.get(SEIN_FILE_HEADER);
                String channelConfigStr = record.get(CHANNEL_CONFIG_FILE_HEADER);

                if (StringUtils.isEmpty(sein) || !sein.matches(IMPORT_STRING_INPUT_REGEX)) {
                    throw new InvalidSeinException("Record " + (i + 1) + ": Invalid SEIN input.");
                } else if (StringUtils.isEmpty(channelConfigStr)) {
                    throw new InvalidSeinException("Record " + (i + 1) + ": Invalid Channel Config input.");
                }

                //check if sein is found in MIRF
                MeteringRegistration meteringRegistration = registrationRepository.findFirstBySein(sein);
                if (meteringRegistration == null) {
                    throw new InvalidSeinException("Record " + (i + 1) + ": SEIN is not found in MIRF.");
                }

                String meterType = meteringRegistration.getMeterType();
                if (!MeterType.MIRF_MT_WESM.name().equalsIgnoreCase(meterType) & !MeterType.MIRF_MT_RCOA.name().equalsIgnoreCase(meterType)) {
                    throw new InvalidSeinException("Record " + (i + 1) + ": Meter type of metering point is not WESM/RCOA " +
                            "in MIRF.");
                }

                if (!ValidationUtil.isChannelConfigNameValid(channelConfigStr)) {
                    throw new InvalidChannelConfigException("Record " + (i + 1) + ": "
                            + channelConfigStr + " is invalid channel config name.");
                }

                ChannelConfig channelConfig = ChannelConfig.valueOf(channelConfigStr);

                if (rcoaMeteringPointChannelConfigRepository.findBySeinAndChannelConfig(sein, channelConfig) == null) {
                    throw new InvalidSeinException("Record " + (i + 1) + ": metering point channel config with sein: " +
                            sein + " and channel config: " + channelConfigStr + " already exists in the database.");
                }

                if (settlementMeteringPointConfigDetailRepository.findBySeinAndChannelConfig(sein, channelConfig) != null) {
                    throw new InvalidSeinException("Record " + (i + 1) + ": Settlement Metering Point Channel Config already " +
                            "exists in the  database.");
                }

                SettlementMeteringPointConfig stlMpConfig = new SettlementMeteringPointConfig();
                stlMpConfig.setSein(sein);
                stlMpConfig.setChannelConfig(channelConfig);

                if (configDetails.contains(stlMpConfig)) {
                    throw new InvalidSeinException("Record " + (i + 1) + ": Settlement Metering Point Config already " +
                            "exists in the import file.");
                }

                VirtualMeteringPointConfig virtualSein = null;

                if (record.isSet(VIRTUAL_SEIN_FILE_HEADER)) {
                    String virtualSeinStr = record.get(VIRTUAL_SEIN_FILE_HEADER);

                    if (StringUtils.isNotEmpty(virtualSeinStr)) {
                        if (!virtualSeinStr.matches(IMPORT_STRING_INPUT_REGEX)) {
                            throw new Exception("Record " + (i + 1) + ": Invalid virtual sein input.");
                        }

                        virtualSein = virtualMeteringPointConfigRepository
                                .findByVirtualSein(virtualSeinStr);

                        if (virtualSein == null) {
                            throw new Exception("Record " + (i + 1) + ": Invalid virtual sein.");
                        }
                    }
                }

                stlMpConfig.setVirtualSein(virtualSein);
                configDetails.add(stlMpConfig);
            }

            settlementMeteringPointConfigDetailRepository.save(configDetails);
            status = COMPLETED;
            LOG.info(configFile.getOriginalFilename() + " is imported.");

        } catch (Exception e) {
            LOG.error("Exception: " + e);
            response.setRemarks(e.getMessage());
        }

        response.setStatus(status);
        super.saveImport(new ConfigImport(importDate, configName, configFile.getOriginalFilename(), status, ConfigImport.STL_MPOINTS));
        return response;
    }
}
