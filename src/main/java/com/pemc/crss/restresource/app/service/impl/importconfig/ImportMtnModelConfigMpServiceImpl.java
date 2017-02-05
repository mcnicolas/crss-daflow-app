package com.pemc.crss.restresource.app.service.impl.importconfig;

import com.pemc.crss.restresource.app.dto.ImportConfigResponseDto;
import com.pemc.crss.restresource.app.exception.InvalidSeinException;
import com.pemc.crss.restresource.app.util.CsvParserUtil;
import com.pemc.crss.restresource.app.util.ValidationUtil;
import com.pemc.crss.meterprocess.core.main.entity.mtn.*;
import com.pemc.crss.meterprocess.core.main.reference.MeterType;
import com.pemc.crss.meterprocess.core.main.repository.MTNModelConfigRepository;
import com.pemc.crss.meterprocess.core.main.repository.MTNModelMeteringPointRepository;
import com.pemc.crss.shared.core.registration.entity.model.MeteringRegistration;
import com.pemc.crss.shared.core.registration.repository.RegistrationRepository;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.util.*;
import java.util.regex.Pattern;

import static com.pemc.crss.restresource.app.util.Constants.*;

@Service
public class ImportMtnModelConfigMpServiceImpl extends AbstractImportConfigService {

    private static final Logger LOG = LoggerFactory.getLogger(ImportMtnModelConfigMpServiceImpl.class);

    @Autowired
    private MTNModelConfigRepository mtnModelConfigRepository;

    @Autowired
    private MTNModelMeteringPointRepository mtnModelMeteringPointRepository;

    @Autowired
    private RegistrationRepository registrationRepository;

    @Override
    public ImportConfigResponseDto importConfig(String configName, MultipartFile configFile, Date importDate) {
        ImportConfigResponseDto response = new ImportConfigResponseDto();
        String status = FAILED;

        try {
            ValidationUtil.validateConfigFile(configFile);

            LOG.info("Importing config file: " + configFile.getOriginalFilename()
                    + " to mtn config model metering points..");

            List<CSVRecord> csvRecords = CsvParserUtil.parseCsvFile(configFile);

            Set<MTNModelMeteringPoint> mtnModelMeteringPoints = new TreeSet<>();

            for (int i = 0; i < csvRecords.size(); i++) {
                CSVRecord record = csvRecords.get(i);

                if (!record.isSet(MTN_CONFIG_NAME_FILE_HEADER) || !record.isSet(SEIN_FILE_HEADER)) {
                    throw new Exception("Record [" + (i + 1) + "]: No mtn config name/mtn name/sein.");
                }

                if (!ValidationUtil.validateRecordMaxValue(record.size(), 12)) {
                    throw new Exception("Record " + (i + 1) + " has more than maximum values.");
                }

                String mtnConfigName = record.get(MTN_CONFIG_NAME_FILE_HEADER);
                String sein = record.get(SEIN_FILE_HEADER);
                String mtnLocationStr = record.get(MTN_LOCATION_FILE_HEADER);
                String channelConfigStr = record.get(CHANNEL_CONFIG_FILE_HEADER);

                if (StringUtils.isEmpty(mtnConfigName) || !mtnConfigName.matches(IMPORT_STRING_INPUT_REGEX)) {
                    throw new Exception("Record [" + (i + 1) + "]: Invalid MTN config name input.");
                } else if (StringUtils.isEmpty(sein)) {
                    throw new Exception("Record [" + (i + 1) + "]: Invalid SEIN input.");
                }

                //check if the mtn config exists in the database
                MTNModelConfig mtnModelConfig = mtnModelConfigRepository.findByMtnConfigName(mtnConfigName);
                if (mtnModelConfig == null) {
                    throw new Exception("Record [" + (i + 1) + "]: MTN config name does not exist in the database.");
                }

                //check if the mtn model config has metering points in the database
                if (mtnModelMeteringPointRepository.findFirstByMtnModelConfig(mtnModelConfig) != null) {
                    throw new Exception("Record [" + (i + 1) + "]: MTN Model Config has already metering points in " +
                            "the database");
                }

                ChannelConfig channelConfig = null;
                MTNLocation mtnLocation = null;

                if (sein.contains("+")) {
                    if (!validateSeinCombination(sein, mtnModelMeteringPoints, mtnModelConfig)) {
                        throw new Exception("Record [" + (i + 1) + "]: Invalid SEIN mtn combination.");
                    }

                    if (!StringUtils.isEmpty(channelConfigStr)) {
                        throw new Exception("Record [" + (i + 1) + "]: Channel Config is not empty.");
                    }

                    if (!StringUtils.isEmpty(mtnLocationStr)) {
                        throw new Exception("Record [" + (i + 1) + "]: MTN Location is not empty.");
                    }

                } else {
                    if (!sein.matches(IMPORT_STRING_INPUT_REGEX)) {
                        throw new Exception("Record [" + (i + 1) + "]: Invalid actual SEIN");
                    }

                    //check if the sein is found in MIRF
                    MeteringRegistration meteringRegistration = registrationRepository.findFirstBySein(sein);
                    if (meteringRegistration == null) {
                        throw new Exception("Record [" + (i + 1) + "]: SEIN is not found in MIRF.");
                    }

                    if (!ValidationUtil.isChannelConfigNameValid(channelConfigStr)) {
                        throw new InvalidSeinException("Record [" + (i + 1) + "]: Invalid channel config name.");
                    }

                    channelConfig = ChannelConfig.valueOf(channelConfigStr);

                    if (!ValidationUtil.isMtnLocationValid(mtnLocationStr)) {
                        throw new Exception("Record [" + (i + 1) + "]: MTN Location name is not valid.");
                    }

                    mtnLocation = MTNLocation.valueOf(mtnLocationStr);

                    if (!MeterType.MIRF_MT_WESM.name().equalsIgnoreCase(meteringRegistration.getMeterType())) {
                        throw new Exception("Record [" + (i + 1) + "]: Sein's meter type is not WESM.");
                    }
                }

                Integer mtnOrder = null;
                if (record.isSet(MTN_ORDER_FILE_HEADER)) {
                    String mtnOrderStr = record.get(MTN_ORDER_FILE_HEADER);
                    if (StringUtils.isNotEmpty(mtnOrderStr)) {
                        try {
                            mtnOrder = parseAndValidatePositiveNonZeroInt(mtnOrderStr);
                        } catch (Exception e) {
                            throw new Exception("Record [" + (i + 1) + "]: Invalid mtn order input.");
                        }
                    }
                }

                if (mtnLocation == MTNLocation.EXACT && mtnOrder != null) {
                    throw new Exception("Record [" + (i + 1) + "]: MTN order of metering point with Exact MTN location " +
                            "is not null.");
                }

                if (mtnLocation != MTNLocation.EXACT && mtnOrder == null) {
                    throw new Exception("Record [" + (i + 1) + "]: MTN order of metering point with non-exact MTN location " +
                            "is null.");
                }

                MTNModelMeteringPoint lastMeteringPoint = getLastMpointOfMtnCfgByMtnOrder(mtnModelMeteringPoints, mtnModelConfig);

                if (!validateMtnOrder(lastMeteringPoint, mtnOrder)) {
                    throw new Exception("Record [" + (i + 1) + "]: Invalid MTN order.");
                }

                if (!validateMeteringPointOrder(lastMeteringPoint, mtnLocation, channelConfig)) {
                    throw new Exception("Record [" + (i + 1) + "]: Invalid metering point order.");
                }

                MTNModelMeteringPoint mtnModelMeteringPoint = new MTNModelMeteringPoint();
                mtnModelMeteringPoint.setMtnModelConfig(mtnModelConfig);
                mtnModelMeteringPoint.setSein(sein);
                mtnModelMeteringPoint.setChannelConfig(channelConfig);
                mtnModelMeteringPoint.setMtnLocation(mtnLocation);
                mtnModelMeteringPoint.setMtnOrder(mtnOrder);

                if (isMeteringPointExist(mtnModelMeteringPoint, mtnModelMeteringPoints)) {
                    throw new Exception("Record [" + (i + 1) + "]: Sein already exists in the MTN model's " +
                            "metering points in the import file.");
                }

                if (record.isSet(LINE_LENGTH_FILE_HEADER)) {
                    try {
                        mtnModelMeteringPoint.setLineLength(parseAndValidatePositiveDouble(record.get(LINE_LENGTH_FILE_HEADER)));
                    } catch (Exception e) {
                        throw new Exception("Record [" + (i + 1) + "]: Invalid line length input.");
                    }
                }

                if (record.isSet(KVLINE_FILE_HEADER)) {
                    try {
                        mtnModelMeteringPoint.setKvLine(parseAndValidatePositiveDouble(record.get(KVLINE_FILE_HEADER)));
                    } catch (Exception e) {
                        throw new Exception("Record [" + (i + 1) + "]: Invalid kVLine input.");
                    }
                }

                if (record.isSet(RA_FILE_HEADER)) {
                    try {
                        mtnModelMeteringPoint.setRa(parseAndValidatePositiveDouble(record.get(RA_FILE_HEADER)));
                    } catch (Exception e) {
                        throw new Exception("Record [" + (i + 1) + "]: Invalid RA input");
                    }
                }

                if (record.isSet(XI_FILE_HEADER)) {
                    try {
                        mtnModelMeteringPoint.setXi(parseAndValidatePositiveDouble(record.get(XI_FILE_HEADER)));
                    } catch (Exception e) {
                        throw new Exception("Record [" + (i + 1) + "]: Invalid XI input");
                    }
                }

                if (record.isSet(TKVA_RATING_FILE_HEADER)) {
                    try {
                        mtnModelMeteringPoint.setTkvaRating(parseAndValidatePositiveDouble(record.get(TKVA_RATING_FILE_HEADER)));
                    } catch (Exception e) {
                        throw new Exception("Record [" + (i + 1) + "]: Invalid Tkva rating input.");
                    }
                }

                if (record.isSet(TCORE_LOSS_FILE_HEADER)) {
                    try {
                        mtnModelMeteringPoint.settCoreLoss(parseAndValidatePositiveDouble(record.get(TCORE_LOSS_FILE_HEADER)));
                    } catch (Exception e) {
                        throw new Exception("Record [" + (i + 1) + "]: Invalid TCore loss input.");
                    }
                }

                if (record.isSet(TCOPPER_LOSS_FILE_HEADER)) {
                    try {
                        mtnModelMeteringPoint.settCopperLoss(parseAndValidatePositiveDouble(record.get(TCOPPER_LOSS_FILE_HEADER)));
                    } catch (Exception e) {
                        throw new Exception("Record [" + (i + 1) + "]: Invalid TCopper loss input.");
                    }
                }

                mtnModelMeteringPoints.add(mtnModelMeteringPoint);
            }

            mtnModelMeteringPointRepository.save(mtnModelMeteringPoints);
            status = COMPLETED;
            LOG.info(configFile.getOriginalFilename() + " is imported.");

        } catch (Exception e) {
            LOG.error(e + ": " + e.getMessage());
            response.setRemarks(e.getMessage());
        }

        response.setStatus(status);
        super.saveImport(new ConfigImport(importDate, configName, configFile.getOriginalFilename(), status, ConfigImport.METERING));

        return response;
    }

    private boolean isMeteringPointExist(MTNModelMeteringPoint mtnModelMeteringPoint, Set<MTNModelMeteringPoint> mtnModelMeteringPoints) {
        for (MTNModelMeteringPoint meteringPoint : mtnModelMeteringPoints) {
            if (mtnModelMeteringPoint.getMtnModelConfig().equals(meteringPoint.getMtnModelConfig()) &&
                    mtnModelMeteringPoint.getSein().equals(meteringPoint.getSein())) {
                return true;
            }
        }

        return false;
    }

    private Integer parseAndValidatePositiveNonZeroInt(String value) throws Exception {
        if (value.isEmpty()) {
            return null;
        }

        Integer field = Integer.parseInt(value.replaceAll(",", ""));

        if (field < 1) {
            throw new Exception();
        }

        return field;
    }

    private Double parseAndValidatePositiveDouble(String value) throws Exception {
        if (value.isEmpty()) {
            return null;
        }

        Double field = Double.parseDouble(value.replaceAll(",",""));

        if (field < 0) {
            throw new Exception();
        }

        return field;
    }

    private boolean validateSeinCombination(String sein, Set<MTNModelMeteringPoint> mtnModelMeteringPoints, MTNModelConfig mtnModelConfig) throws Exception {
        String[] tokens = sein.split(Pattern.quote("+"));

        int actualNonExactSeinCount = getNumberOfNonExactActualSeinOfMtnConfig(mtnModelMeteringPoints, mtnModelConfig);

        if (tokens.length > actualNonExactSeinCount) {
            return false;
        }

        Set<Integer> mtnOrderSet = new HashSet<>();
        Integer previousSeinMtn = null;

        for (String mtnOrder : tokens) {

            if (!StringUtils.isNumeric(mtnOrder.trim())) {
                return false;
            }

            Integer seinMtnOrderInt = Integer.parseInt(mtnOrder.trim());

            if (seinMtnOrderInt < 1) {
                return false;
            }

            if (seinMtnOrderInt > actualNonExactSeinCount) {
                return false;
            }

            if (!mtnOrderSet.add(seinMtnOrderInt)) {
                return false;
            }

            if (previousSeinMtn != null && seinMtnOrderInt < previousSeinMtn) {
                return false;
            }

            previousSeinMtn = seinMtnOrderInt;
        }

        return true;
    }

    private int getNumberOfNonExactActualSeinOfMtnConfig(Set<MTNModelMeteringPoint> mtnModelMeteringPoints, MTNModelConfig mtnModelConfig) {
        int i = 0;
        for (MTNModelMeteringPoint meteringPoint : mtnModelMeteringPoints) {
            if (meteringPoint.getMtnModelConfig().equals(mtnModelConfig) &&
                    meteringPoint.getMtnLocation() != MTNLocation.EXACT &&
                    meteringPoint.getChannelConfig() != null) {
                i++;
            }
        }
        return i;
    }

    private boolean validateMtnOrder(MTNModelMeteringPoint lastMeteringPoint, Integer mtnOrder) {

        if (mtnOrder != null) {

            if (mtnOrder >= 1 && lastMeteringPoint != null && mtnOrder.equals(lastMeteringPoint.getMtnOrder())) {
                return false;
            }

            if (mtnOrder > 1) {
                if (lastMeteringPoint == null) {
                    return false;
                } else if (lastMeteringPoint.getMtnOrder() == null) {
                    return false;
                } else if (lastMeteringPoint.getMtnOrder() != (mtnOrder - 1)) {
                    return false;
                }
            }
        }

        return true;
    }

    private boolean validateMeteringPointOrder(MTNModelMeteringPoint lastMeteringPoint, MTNLocation mtnLocation, ChannelConfig channelConfig) {
        if (lastMeteringPoint != null) {
            Integer lastMeteringPointMtnOrder = lastMeteringPoint.getMtnOrder();

            if (mtnLocation == MTNLocation.EXACT && lastMeteringPointMtnOrder != null) {
                return false;
            } else if (lastMeteringPointMtnOrder != null &&
                    lastMeteringPoint.getChannelConfig() == null && channelConfig != null) {
                return false;
            }
        }

        return true;
    }

    private MTNModelMeteringPoint getLastMpointOfMtnCfgByMtnOrder(Set<MTNModelMeteringPoint> mtnModelMeteringPoints, MTNModelConfig mtnModelConfig) {
        MTNModelMeteringPoint lastMeteringPoint = null;
        for (MTNModelMeteringPoint meteringPoint : mtnModelMeteringPoints) {
            if (meteringPoint.getMtnModelConfig().equals(mtnModelConfig)) {
                lastMeteringPoint = meteringPoint;
            }
        }

        return lastMeteringPoint;
    }

}
