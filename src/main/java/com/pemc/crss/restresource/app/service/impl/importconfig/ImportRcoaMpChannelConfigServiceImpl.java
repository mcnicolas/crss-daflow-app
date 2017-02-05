package com.pemc.crss.restresource.app.service.impl.importconfig;

import com.pemc.crss.restresource.app.dto.ImportConfigResponseDto;
import com.pemc.crss.restresource.app.exception.InvalidChannelConfigException;
import com.pemc.crss.restresource.app.exception.InvalidFileException;
import com.pemc.crss.restresource.app.exception.InvalidSeinException;
import com.pemc.crss.restresource.app.util.CsvParserUtil;
import com.pemc.crss.restresource.app.util.ValidationUtil;
import com.pemc.crss.meterprocess.core.main.entity.mtn.ChannelConfig;
import com.pemc.crss.meterprocess.core.main.entity.mtn.ConfigImport;
import com.pemc.crss.meterprocess.core.main.entity.mtn.RcoaMeteringPointChannelConfig;
import com.pemc.crss.meterprocess.core.main.reference.MeterType;
import com.pemc.crss.meterprocess.core.main.repository.RcoaMeteringPointChannelConfigRepository;
import com.pemc.crss.shared.core.registration.entity.model.MeteringRegistration;
import com.pemc.crss.shared.core.registration.entity.model.MeteringRegistrationChannel;
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
public class ImportRcoaMpChannelConfigServiceImpl extends AbstractImportConfigService {

    private static final Logger LOG = LoggerFactory.getLogger(ImportRcoaMpChannelConfigServiceImpl.class);

    @Autowired
    private RegistrationRepository registrationRepository;

    @Autowired
    private RcoaMeteringPointChannelConfigRepository rcoaMeteringPointChannelConfigRepository;

    @Override
    public ImportConfigResponseDto importConfig(String configName, MultipartFile configFile, Date importDate) {
        ImportConfigResponseDto response = new ImportConfigResponseDto();
        String status = FAILED;

        try {
            ValidationUtil.validateConfigFile(configFile);

            LOG.info("Importing config file: " + configFile.getOriginalFilename()
                    + " to metering point channel config...");

            List<CSVRecord> csvRecords = CsvParserUtil.parseCsvFile(configFile);

            Set<RcoaMeteringPointChannelConfig> mpChannelConfigs = new HashSet<>();

            for (int i = 0; i < csvRecords.size(); i++) {
                CSVRecord record = csvRecords.get(i);

                //check if the record has values for sein and channel config
                if (!record.isSet(SEIN_FILE_HEADER) || !record.isSet(CHANNEL_CONFIG_FILE_HEADER)) {
                    throw new InvalidChannelConfigException("Record " + (i + 1) + " has no sein and/or channel config.");
                }

                //check if the no. of record values is not more than maximum values
                if (!ValidationUtil.validateRecordMaxValue(record.size(), 2)) {
                    throw new InvalidFileException("Record " + (i + 1) + " has more than the maximum values.");
                }

                String sein = record.get(SEIN_FILE_HEADER);
                String channelConfigStr = record.get(CHANNEL_CONFIG_FILE_HEADER);

                if (StringUtils.isEmpty(sein) || !sein.matches(IMPORT_STRING_INPUT_REGEX)) {
                    throw new InvalidSeinException("Record " + (i + 1) + ": Invalid sein input.");
                }

                //check if sein is found in MIRF
                List<MeteringRegistration> meteringRegistration = registrationRepository.findBySeinWithChannelList(sein);
                if (meteringRegistration.isEmpty()) {
                    throw new InvalidSeinException("Record " + (i + 1) + ": Sein not found in MIRF.");
                }

                //check if the metering point's meter type is RCOA
                if (!MeterType.MIRF_MT_RCOA.name().equalsIgnoreCase(meteringRegistration.get(0).getMeterType())) {
                    throw new Exception("Record " + (i + 1) + ": Meter type is not RCOA.");
                }

                if (!ValidationUtil.isChannelConfigNameValid(channelConfigStr)) {
                    throw new InvalidChannelConfigException("Record " + (i + 1) + ": "
                            + channelConfigStr + " is invalid channel config name.");
                }

                ChannelConfig channelConfig = ChannelConfig.valueOf(channelConfigStr);

                //check if the rcoa mp channel config does not exists in the database
                if (rcoaMeteringPointChannelConfigRepository.findBySeinAndChannelConfig(sein, channelConfig) != null) {
                    throw new InvalidChannelConfigException("Record " + (i + 1)
                            + ": RCOA Metering Point Channel Config already exist in the database.");
                }

                if (!validateChannelConfigLoadChannel(channelConfig, meteringRegistration.get(0).getMeteringRegistrationChannelList())) {
                    throw new InvalidChannelConfigException("Record " + (i + 1) + ": Invalid channel config "
                            + channelConfigStr);
                }

                Set<RcoaMeteringPointChannelConfig> allRcoaMpChannelConfigs = new HashSet<>(mpChannelConfigs);
                allRcoaMpChannelConfigs.addAll(rcoaMeteringPointChannelConfigRepository.findAll());

                if (isSetToOtherChConfig(channelConfig, sein, allRcoaMpChannelConfigs)) {
                    throw new Exception("Record " + (i + 1) + ": RCOA Metering Point Channel Config is already set to other channel config.");
                }

                RcoaMeteringPointChannelConfig mpChannelConfig = new RcoaMeteringPointChannelConfig();
                mpChannelConfig.setSein(sein);
                mpChannelConfig.setChannelConfig(channelConfig);

                if (!mpChannelConfigs.add(mpChannelConfig)) {
                    throw new InvalidSeinException("Record " + (i + 1) + ": RCOA Metering Point Channel Config already " +
                            "exists in the import file.");
                }
            }

            rcoaMeteringPointChannelConfigRepository.save(mpChannelConfigs);
            status = COMPLETED;

            LOG.info(configFile.getOriginalFilename() + " is imported.");
        } catch (Exception e) {
            LOG.error("Exception: " + e);
            response.setRemarks(e.getMessage());
        }

        response.setStatus(status);

        super.saveImport(new ConfigImport(importDate, configName, configFile.getOriginalFilename(), status, ConfigImport.METERING));
        return response;
    }

    private boolean validateChannelConfigLoadChannel(ChannelConfig config, Set<MeteringRegistrationChannel> loadChannels) {

        if (loadChannels.isEmpty()) {
            return false;
        }

        switch (config) {
            case DEL:
                return isChannelExist("kwhd", loadChannels);
            case REC:
                return isChannelExist("kwhr", loadChannels);
            case NET:
                return isChannelExist("kwhr", loadChannels) &&
                        isChannelExist("kwhd", loadChannels);
        }

        return false;
    }

    private boolean isSetToOtherChConfig(ChannelConfig channelConfig, String sein, Set<RcoaMeteringPointChannelConfig> mpChannelConfigs) {
        for (RcoaMeteringPointChannelConfig mpChannelConfig : mpChannelConfigs) {
            if (mpChannelConfig.getSein().equalsIgnoreCase(sein) &&
                    !mpChannelConfig.getChannelConfig().equals(channelConfig)) {
                return true;
            }
        }
        return false;
    }

    private boolean isChannelExist(String channel, Set<MeteringRegistrationChannel> loadChannels) {
        for (MeteringRegistrationChannel loadChannel : loadChannels) {

            if (channel.equalsIgnoreCase(loadChannel.getName())) {
                return true;
            }
        }
        return false;
    }
}
