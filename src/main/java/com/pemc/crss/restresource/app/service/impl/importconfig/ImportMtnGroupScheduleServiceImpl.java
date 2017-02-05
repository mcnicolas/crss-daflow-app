package com.pemc.crss.restresource.app.service.impl.importconfig;

import com.pemc.crss.meterprocess.core.main.entity.mtn.*;
import com.pemc.crss.meterprocess.core.main.repository.MTNModelGroupRepository;
import com.pemc.crss.meterprocess.core.main.repository.MTNModelGroupScheduleRepository;
import com.pemc.crss.restresource.app.dto.ImportConfigResponseDto;
import com.pemc.crss.restresource.app.util.CsvParserUtil;
import com.pemc.crss.restresource.app.util.ValidationUtil;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.multipart.MultipartFile;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.pemc.crss.restresource.app.util.Constants.*;

@Service
@Transactional("mainTransactionManager")
public class ImportMtnGroupScheduleServiceImpl extends AbstractImportConfigService {

    private static final Logger LOG = LoggerFactory.getLogger(ImportMtnGroupScheduleServiceImpl.class);

    @Autowired
    private MTNModelGroupScheduleRepository mtnModelGroupScheduleRepository;

    @Autowired
    private MTNModelGroupRepository mtnModelGroupRepository;

    @Override
    public ImportConfigResponseDto importConfig(String configName, MultipartFile configFile, Date importDate) {
        ImportConfigResponseDto response = new ImportConfigResponseDto();
        String status = FAILED;

        try {
            ValidationUtil.validateConfigFile(configFile);

            LOG.info("Importing config file: " + configFile.getOriginalFilename()
                    + " to mtn model group schedule..");

            List<CSVRecord> csvRecords = CsvParserUtil.parseCsvFile(configFile);

            Set<MTNModelGroupSchedule> mtnModelGroupSchedules = new HashSet<>();

            for (int i = 0; i < csvRecords.size(); i++) {
                CSVRecord record = csvRecords.get(i);

                if (!record.isSet(MTN_GROUP_NAME_FILE_HEADER) || !record.isSet(EFFECTIVE_START_DATE_FILE_HEADER)) {
                    throw new Exception("Record [" + (i + 1) + "]: No mtn group name/effective start date.");
                }

                String mtnGroupName = record.get(MTN_GROUP_NAME_FILE_HEADER);
                String effectiveStartDateStr = record.get(EFFECTIVE_START_DATE_FILE_HEADER);

                if (StringUtils.isEmpty(mtnGroupName) || !mtnGroupName.matches(IMPORT_STRING_INPUT_REGEX)) {
                    throw new Exception("Record [" + (i + 1) + "]: Invalid mtn group name input");
                }

                Date effectiveStartDate;
                try {
                    if (StringUtils.isEmpty(effectiveStartDateStr)) {
                        throw new Exception();
                    }
                    SimpleDateFormat sdf = new SimpleDateFormat("MM-dd-yyyy HH:mm");
                    effectiveStartDate = sdf.parse(effectiveStartDateStr);
                } catch (Exception e) {
                    throw new Exception("Record [" + (i + 1) + "]: Invalid effective start date input.");
                }

                //check if mtn group name exists in the database
                MTNModelGroup mtnModelGroup = mtnModelGroupRepository.findByGroupName(mtnGroupName);
                if (mtnModelGroup == null) {
                    throw new Exception("Record [" + (i + 1) + "]: MTN Model Group " + mtnGroupName + " does not exists in " +
                            "the database");
                }

                //check if mtn group schedule exists in the database
                if (mtnModelGroupScheduleRepository.findByMtnModelGroupAndEffectiveStartDate(mtnModelGroup,
                        effectiveStartDate) != null) {
                    throw new Exception("Record [" + (i + 1) + "]: MTN Model Group Schedule already exists in " +
                            "the database");
                }

                // check if one of metering points in mtn model group contains same sein and channel config in metering points
                // of existing mtn model group that has same effective start date
                for (MTNModelConfig modelConfig : mtnModelGroup.getMtnModelConfigList()) {
                    for (MTNModelMeteringPoint mp : modelConfig.getMeteringPoints()) {
                        if (hasSameSeinAndChConfigInExistingModelGroupsWithSameEffectiveDate(mp, effectiveStartDate,
                                mtnModelGroupSchedules)) {
                            throw new Exception("Record [" + (i + 1) + "]: Model group has same " +
                                    " metering point's sein and channel config in one of existed model groups with" +
                                    " the same effective start date.");
                        }
                    }
                }

                MTNModelGroupSchedule mtnModelGroupSchedule = new MTNModelGroupSchedule();
                mtnModelGroupSchedule.setMtnModelGroup(mtnModelGroup);
                mtnModelGroupSchedule.setEffectiveStartDate(effectiveStartDate);

                if (mtnModelGroupSchedules.contains(mtnModelGroupSchedule)) {
                    throw new Exception("Record [" + (i + 1) + "]: Model Group Schedule already exists in " +
                            "the import file");
                }

                mtnModelGroupSchedules.add(mtnModelGroupSchedule);
            }

            mtnModelGroupScheduleRepository.save(mtnModelGroupSchedules);
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

    private boolean hasSameSeinAndChConfigInExistingModelGroupsWithSameEffectiveDate(MTNModelMeteringPoint meteringPoint,
                                                                                     Date effectiveStartDate,
                                                                                     Set<MTNModelGroupSchedule> groupSchedules) {
        for (MTNModelGroupSchedule gs : groupSchedules) {
            if (gs.getEffectiveStartDate().equals(effectiveStartDate)) {
                Set<MTNModelConfig> configList = gs.getMtnModelGroup().getMtnModelConfigList();

                //add config list of mtn group with the same effective start date from the database
                configList.addAll(mtnModelGroupScheduleRepository.findMtnConfigsByEffectiveStartDate(effectiveStartDate));

                for (MTNModelConfig config : gs.getMtnModelGroup().getMtnModelConfigList()) {
                    for (MTNModelMeteringPoint mp : config.getMeteringPoints()) {
                        if (mp.getChannelConfig() != null && mp.getChannelConfig().equals(meteringPoint.getChannelConfig())
                                && mp.getSein().equals(meteringPoint.getSein())) {
                            return true;
                        }
                    }
                }

            }
        }

        return false;
    }
}
