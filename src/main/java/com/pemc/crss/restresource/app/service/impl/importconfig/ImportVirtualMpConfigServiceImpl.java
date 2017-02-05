package com.pemc.crss.restresource.app.service.impl.importconfig;

import com.pemc.crss.restresource.app.dto.ImportConfigResponseDto;
import com.pemc.crss.restresource.app.exception.InvalidMtnException;
import com.pemc.crss.restresource.app.exception.InvalidSeinException;
import com.pemc.crss.restresource.app.util.CsvParserUtil;
import com.pemc.crss.restresource.app.util.ValidationUtil;
import com.pemc.crss.meterprocess.core.main.entity.mtn.ConfigImport;
import com.pemc.crss.meterprocess.core.main.entity.mtn.VirtualMeteringPointConfig;
import com.pemc.crss.meterprocess.core.main.repository.VirtualMeteringPointConfigRepository;
import com.pemc.crss.shared.core.registration.entity.model.MarketTradingNode;
import com.pemc.crss.shared.core.registration.entity.model.MeteringRegistration;
import com.pemc.crss.shared.core.registration.repository.MarketTradingNodeRepository;
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
public class ImportVirtualMpConfigServiceImpl extends AbstractImportConfigService {

    private static final Logger LOG = LoggerFactory.getLogger(ImportVirtualMpConfigServiceImpl.class);

    @Autowired
    private VirtualMeteringPointConfigRepository virtualMeteringPointConfigRepository;

    @Autowired
    private RegistrationRepository registrationRepository;

    @Autowired
    private MarketTradingNodeRepository marketTradingNodeRepository;

    @Override
    public ImportConfigResponseDto importConfig(String configName, MultipartFile configFile, Date importDate) {

        ImportConfigResponseDto response = new ImportConfigResponseDto();
        String status = FAILED;

        try {
            ValidationUtil.validateConfigFile(configFile);

            LOG.info("Importing config file: " + configFile.getOriginalFilename()
                    + " settlement mp config header...");

            List<CSVRecord> csvRecords = CsvParserUtil.parseCsvFile(configFile);

            Set<VirtualMeteringPointConfig> virtualMeteringPoints = new HashSet<>();

            for (int i = 0; i < csvRecords.size(); i++) {
                CSVRecord record = csvRecords.get(i);

                if (!record.isSet(SEIN_FILE_HEADER) || !record.isSet(MTN_FILE_HEADER)) {
                    throw new InvalidSeinException("Record " + (i + 1) + " has no sein and/or market trading node.");
                }

                String sein = record.get(SEIN_FILE_HEADER);
                String mtn = record.get(MTN_FILE_HEADER);

                if (StringUtils.isEmpty(sein) || !sein.matches(IMPORT_STRING_INPUT_REGEX)) {
                    throw new InvalidSeinException("Record " + (i + 1) + ": Invalid SEIN input.");
                } else if (StringUtils.isEmpty(mtn) || !mtn.matches(IMPORT_STRING_INPUT_REGEX)) {
                    throw new InvalidMtnException("Record " + (i + 1) + ": Invalid MTN input.");
                }

                MeteringRegistration meteringRegistration = registrationRepository.findFirstBySein(sein);
                if (meteringRegistration == null) {
                    throw new InvalidSeinException("Record " + (i + 1) + ": SEIN not found in MIRF.");
                }

                if (!"Virtual".equalsIgnoreCase(meteringRegistration.getMeterType())) {
                    throw new InvalidSeinException("Record " + (i + 1) + ": Meter type is not virtual in MIRF.");
                }

                if (virtualMeteringPointConfigRepository.findByVirtualSein(sein) != null) {
                    throw new InvalidSeinException("Record " + (i + 1) + ": Virtual SEIN already exists in the " +
                            " database.");
                }

                MarketTradingNode marketTradingNode = marketTradingNodeRepository.findByName(mtn);
                if (marketTradingNode == null) {
                    throw new InvalidMtnException("Record " + (i + 1) + ": MTN is not found in the database.");
                }

                if (!marketTradingNode.isActive()) {
                    throw new InvalidMtnException("Record " + (i + 1) + ": MTN is not active.");
                }

                VirtualMeteringPointConfig virtualMeteringPoint = new VirtualMeteringPointConfig();
                virtualMeteringPoint.setVirtualSein(sein);
                virtualMeteringPoint.setMtn(mtn);

                if (!virtualMeteringPoints.add(virtualMeteringPoint)) {
                    throw new InvalidSeinException("Record " + (i + 1) + ": Virtual Sein already " +
                            "exists in the import file.");
                }
            }

            virtualMeteringPointConfigRepository.save(virtualMeteringPoints);
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
