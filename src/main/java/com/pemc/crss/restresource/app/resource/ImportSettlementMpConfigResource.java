package com.pemc.crss.restresource.app.resource;


import com.pemc.crss.meterprocess.core.main.entity.mtn.ConfigImport;
import com.pemc.crss.restresource.app.dto.ImportConfigResponseDto;
import com.pemc.crss.restresource.app.service.impl.importconfig.ImportSettlementMpConfigServiceImpl;
import com.pemc.crss.restresource.app.service.impl.importconfig.ImportVirtualMpConfigServiceImpl;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.Resource;
import java.util.Date;

@RestController
@RequestMapping("/import-settlement-mpconfig")
public class ImportSettlementMpConfigResource {

    @Resource
    private ImportVirtualMpConfigServiceImpl importVirtualMpConfigService;

    @Resource
    private ImportSettlementMpConfigServiceImpl importSettlementMpConfigService;

    @RequestMapping(value = "/import-virtual-sein", method = RequestMethod.POST)
    public ResponseEntity<ImportConfigResponseDto> importVirtualSein(@RequestParam(value = "configFile") MultipartFile configFile,
                                                                     @RequestParam(value = "configName") String configName,
                                                                     @RequestParam(value = "importDate") Date importDate) {

        ImportConfigResponseDto response = importVirtualMpConfigService.importConfig(configName, configFile, importDate);
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @RequestMapping(value = "/import-stl-mpoints", method = RequestMethod.POST)
    public ResponseEntity<ImportConfigResponseDto> importStlMpChannelConfig(@RequestParam(value = "configFile") MultipartFile configFile,
                                                                            @RequestParam(value = "configName") String configName,
                                                                            @RequestParam(value = "importDate") Date importDate) {

        ImportConfigResponseDto response = importSettlementMpConfigService.importConfig(configName, configFile, importDate);
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @RequestMapping(value = "/display-imports", method = RequestMethod.GET)
    public ResponseEntity<Page<ConfigImport>> displayImports(Pageable pageable) {

        return new ResponseEntity<>(importSettlementMpConfigService.getConfigImports(ConfigImport.STL_MPOINTS, pageable), HttpStatus.OK);
    }
}
