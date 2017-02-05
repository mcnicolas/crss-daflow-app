package com.pemc.crss.restresource.app.service.impl.importconfig;

import com.pemc.crss.restresource.app.dto.ImportConfigResponseDto;
import com.pemc.crss.restresource.app.service.ImportConfigService;
import com.pemc.crss.meterprocess.core.main.entity.mtn.ConfigImport;
import com.pemc.crss.meterprocess.core.main.repository.ConfigImportRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.web.multipart.MultipartFile;

import java.util.Date;

public abstract class AbstractImportConfigService implements ImportConfigService {

    @Autowired
    private ConfigImportRepository configImportRepository;

    public abstract ImportConfigResponseDto importConfig(String configName, MultipartFile configFile, Date importDate);

    @Override
    public Page<ConfigImport> getConfigImports(String configType, Pageable pageable) {
        return configImportRepository.findByConfigType(configType, pageable);
    }

    void saveImport(ConfigImport configImport) {
        configImportRepository.save(configImport);
    }
}
