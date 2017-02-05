package com.pemc.crss.restresource.app.service;

import com.pemc.crss.meterprocess.core.main.entity.mtn.ConfigImport;
import com.pemc.crss.restresource.app.dto.ImportConfigResponseDto;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.web.multipart.MultipartFile;

import java.util.Date;

public interface ImportConfigService {

    ImportConfigResponseDto importConfig(String configName, MultipartFile configFile, Date importDate);

    Page<ConfigImport> getConfigImports(String configType, Pageable pageable);
}
