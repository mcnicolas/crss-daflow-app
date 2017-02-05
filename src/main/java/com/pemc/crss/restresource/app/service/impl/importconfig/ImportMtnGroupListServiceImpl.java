package com.pemc.crss.restresource.app.service.impl.importconfig;


import com.pemc.crss.meterprocess.core.main.entity.mtn.*;
import com.pemc.crss.meterprocess.core.main.repository.MTNModelConfigRepository;
import com.pemc.crss.meterprocess.core.main.repository.MTNModelGroupRepository;
import com.pemc.crss.restresource.app.dto.ImportConfigResponseDto;
import com.pemc.crss.restresource.app.util.CsvParserUtil;
import com.pemc.crss.restresource.app.util.ValidationUtil;
import com.pemc.crss.shared.core.registration.entity.model.constants.FacilityType;
import com.pemc.crss.shared.core.registration.repository.RegistrationRepository;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.multipart.MultipartFile;

import java.util.*;

import static com.pemc.crss.restresource.app.util.Constants.*;

@Service
@Transactional("mainTransactionManager")
public class ImportMtnGroupListServiceImpl extends AbstractImportConfigService {

    private static final Logger LOG = LoggerFactory.getLogger(ImportMtnGroupListServiceImpl.class);

    @Autowired
    private MTNModelConfigRepository mtnModelConfigRepository;

    @Autowired
    private MTNModelGroupRepository mtnModelGroupRepository;

    @Autowired
    private RegistrationRepository registrationRepository;

    @Override
    public ImportConfigResponseDto importConfig(String configName, MultipartFile configFile, Date importDate) {
        ImportConfigResponseDto response = new ImportConfigResponseDto();
        String status = FAILED;

        try {
            ValidationUtil.validateConfigFile(configFile);

            LOG.info("Importing config file: " + configFile.getOriginalFilename()
                    + " to mtn model group list..");

            List<CSVRecord> csvRecords = CsvParserUtil.parseCsvFile(configFile);

            Map<String, Set<MTNModelConfig>> mtnModelGroupMap = new HashMap<>();

            for (int i = 0; i < csvRecords.size(); i++) {
                CSVRecord record = csvRecords.get(i);

                if (!record.isSet(MTN_GROUP_NAME_FILE_HEADER) || !record.isSet(MTN_CONFIG_NAME_FILE_HEADER)) {
                    throw new Exception("Record [" + (i + 1) + "]: No mtn group name/mtn name/mtn config name.");
                }

                String newMtnGroupName = record.get(MTN_GROUP_NAME_FILE_HEADER);
                String mtnConfigName = record.get(MTN_CONFIG_NAME_FILE_HEADER);

                if (StringUtils.isEmpty(newMtnGroupName) || !newMtnGroupName.matches(IMPORT_STRING_INPUT_REGEX)) {
                    throw new Exception("Record [" + (i + 1) + "]: Invalid MTN Model Group Name input.");
                } else if (StringUtils.isEmpty(mtnConfigName) || !mtnConfigName.matches(IMPORT_STRING_INPUT_REGEX)) {
                    throw new Exception("Record [" + (i + 1) + "]: Invalid MTN Model Config Name input.");
                }

                //check if a mtn model config with record's mtn config name and mtn name exists in the database
                MTNModelConfig newMtnConfig = mtnModelConfigRepository.findByMtnConfigName(mtnConfigName);
                if (newMtnConfig == null) {
                    throw new Exception("Record [" + (i + 1) + "]: MTN Model Config does not exist in the database.");
                }

                Set<MTNModelConfig> mtnModelConfigs = mtnModelGroupMap.get(newMtnGroupName);
                if (mtnModelConfigs == null) {
                    mtnModelConfigs = new HashSet<>();

                    //check if the group name exists in the database
                    if (mtnModelGroupRepository.findByGroupName(newMtnGroupName) != null) {
                        throw new Exception("Record [" + (i + 1) + "]: MTN Model Group Name already exists" +
                                " in the database.");
                    }

                } else {

                    if (mtnModelConfigs.contains(newMtnConfig)) {
                        throw new Exception("Record [" + (i + 1) + "]: MTN Model Config : " + mtnConfigName +
                                " already exists in the MTN group name: " + newMtnGroupName);
                    }

                    //check if the MTN name of the new MTN config is same in other MTN configs of the same group
                    for (MTNModelConfig mtnModelConfig : mtnModelConfigs) {
                        if (newMtnConfig.getMtnName().equals(mtnModelConfig.getMtnName())) {
                            throw new Exception("Record [" + (i + 1) + "]: MTN Name " + newMtnConfig.getMtnName() + " is " +
                                    " the same in other MTN configs of the MTN Group");
                        }
                    }

                    /* check if the new mtn model config has metering points with same sein and channel config
                    in metering points of other mtn model configs of the same mtn group */
                    for (MTNModelMeteringPoint meteringPoint : newMtnConfig.getMeteringPoints()) {
                        MTNModelMeteringPoint mpWithSameSein = findMpWithSameSein(meteringPoint.getSein(), mtnModelConfigs);

                        if (mpWithSameSein != null) {
                            if (mpWithSameSein.getChannelConfig() != null && meteringPoint.getChannelConfig() != null &&
                                    mpWithSameSein.getChannelConfig().equals(meteringPoint.getChannelConfig())) {
                                throw new Exception("Record [" + (i + 1) + "]: MTN Model Config has duplicate actual sein with" +
                                        " same channel config in other mtn config's metering points.");
                            }

                            if (!validateMatchedMeteringPoints(meteringPoint, mpWithSameSein)) {
                                throw new Exception("Record [" + (i + 1) + "]: Invalid metering point " + meteringPoint.getSein() +
                                        " in mtn config " + newMtnConfig.getMtnConfigName());
                            }
                        }
                    }
                }

                mtnModelConfigs.add(newMtnConfig);
                mtnModelGroupMap.put(newMtnGroupName, mtnModelConfigs);
            }

            //check if the MTN names of an MTN group is same with other MTN group's when there is a common MTN name between the groups
            for (Map.Entry<String, Set<MTNModelConfig>> entry : mtnModelGroupMap.entrySet()) {
                for (Map.Entry<String, Set<MTNModelConfig>> entry2 : mtnModelGroupMap.entrySet()) {
                    if (!entry.getKey().equals(entry2.getKey()) &&
                            jointMtnNames(entry.getValue(), entry2.getValue()) &&
                            !getDistinctMtnNames(entry.getValue()).equals(getDistinctMtnNames(entry2.getValue()))) {
                        throw new Exception(entry.getKey() + " and " + entry2.getKey() + " has common MTN name but " +
                                "have different set of MTN names.");
                    }
                }
            }

            mtnModelGroupRepository.save(convertMtnModelGroupMapToList(mtnModelGroupMap));
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

    /**
     * returns true if the two metering points have Generator facility type and
     * if one has DEL channel config amd Exact MTN location and
     * the other has REC channel config and non-Exact MTN location
     */
    private boolean validateMatchedMeteringPoints(MTNModelMeteringPoint meteringPoint1, MTNModelMeteringPoint meteringPoint2) {

        if (!isGenMeteringPoint(meteringPoint1.getSein())) {
            return false;
        }

        if (meteringPoint1.getChannelConfig() == ChannelConfig.DEL
                && meteringPoint1.getMtnLocation() == MTNLocation.EXACT
                && meteringPoint2.getChannelConfig() == ChannelConfig.REC
                && meteringPoint2.getMtnLocation() != MTNLocation.EXACT) {
            return true;
        } else if (meteringPoint1.getChannelConfig() == ChannelConfig.REC
                && meteringPoint1.getMtnLocation() != MTNLocation.EXACT
                && meteringPoint2.getChannelConfig() == ChannelConfig.DEL
                && meteringPoint2.getMtnLocation() == MTNLocation.EXACT) {
            return true;
        }

        return false;
    }

    private boolean isGenMeteringPoint(String sein) {
        return FacilityType.GENERATOR.equals(registrationRepository.findFacilityTypeBySein(sein));
    }

    private MTNModelMeteringPoint findMpWithSameSein(String sein, Set<MTNModelConfig> mtnModelConfigs) {
        for (MTNModelConfig config : mtnModelConfigs) {
            for (MTNModelMeteringPoint meteringPoint : config.getMeteringPoints()) {
                if (meteringPoint.getSein().equals(sein)) {
                    return meteringPoint;
                }
            }
        }

        return null;
    }

    //returns true if the two set of mtn model configs has common mtn name
    private boolean jointMtnNames(Set<MTNModelConfig> mtnModelConfigs1, Set<MTNModelConfig> mtnModelConfigs2) {

        if (mtnModelConfigs1.isEmpty() || mtnModelConfigs2.isEmpty()) {
            return false;
        }

        for (MTNModelConfig cfg1 : mtnModelConfigs1) {
            for (MTNModelConfig cfg2 : mtnModelConfigs2) {
                if (cfg1.getMtnName().equals(cfg2.getMtnName())) {
                    return true;
                }
            }
        }

        return false;
    }

    private Set<String> getDistinctMtnNames(Set<MTNModelConfig> mtnModelConfigs) {
        Set<String> mtnNames = new HashSet<>();
        for (MTNModelConfig config : mtnModelConfigs) {
            mtnNames.add(config.getMtnName());
        }
        return mtnNames;
    }

    private List<MTNModelGroup> convertMtnModelGroupMapToList(Map<String, Set<MTNModelConfig>> mtnModelGroupMap) {
        List<MTNModelGroup> mtnModelGroups = new ArrayList<>();

        for (Map.Entry<String, Set<MTNModelConfig>> entry : mtnModelGroupMap.entrySet()) {
            MTNModelGroup mtnModelGroup = new MTNModelGroup();
            mtnModelGroup.setGroupName(entry.getKey());
            mtnModelGroup.setMtnModelConfigList(entry.getValue());
            mtnModelGroups.add(mtnModelGroup);
        }

        return mtnModelGroups;
    }
}
