package com.pemc.crss.restresource.app.resource;


import com.pemc.crss.meterprocess.core.main.entity.mtn.ConfigImport;
import com.pemc.crss.restresource.app.dto.ImportConfigResponseDto;
import com.pemc.crss.restresource.app.service.impl.importconfig.*;
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
@RequestMapping("/import-metering-config")
public class ImportMeteringConfigResource {

    @Resource
    private ImportRcoaMpChannelConfigServiceImpl importRcoaMpChannelConfigService;

    @Resource
    private ImportMtnGroupListServiceImpl importMtnGroupListService;

    @Resource
    private ImportMtnGroupScheduleServiceImpl importMtnGroupScheduleService;

    @Resource
    private ImportMtnModelConfigServiceImpl importMtnModelConfigService;

    @Resource
    private ImportMtnModelConfigMpServiceImpl importMtnModelConfigMpService;

    @RequestMapping(value = "/import-rcoa-mpchannel-config", method = RequestMethod.POST)
    public ResponseEntity<ImportConfigResponseDto> importRcoaMpChannelConfig(@RequestParam(value = "configFile") MultipartFile configFile,
                                                                             @RequestParam(value = "configName") String configName,
                                                                             @RequestParam(value = "importDate") Date importDate) {

        ImportConfigResponseDto response = importRcoaMpChannelConfigService.importConfig(configName, configFile, importDate);
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @RequestMapping(value = "/import-mtn-model-config", method = RequestMethod.POST)
    public ResponseEntity<ImportConfigResponseDto> importMtnModelConfig(@RequestParam(value = "configFile") MultipartFile configFile,
                                                                        @RequestParam(value = "configName") String configName,
                                                                        @RequestParam(value = "importDate") Date importDate) {

        ImportConfigResponseDto response = importMtnModelConfigService.importConfig(configName, configFile, importDate);
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @RequestMapping(value = "/import-mtn-model-config-mp", method = RequestMethod.POST)
    public ResponseEntity<ImportConfigResponseDto> importMtnModelConfigMp(@RequestParam(value = "configFile") MultipartFile configFile,
                                                                          @RequestParam(value = "configName") String configName,
                                                                          @RequestParam(value = "importDate") Date importDate) {

        ImportConfigResponseDto response = importMtnModelConfigMpService.importConfig(configName, configFile, importDate);
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @RequestMapping(value = "/import-mtn-group-list", method = RequestMethod.POST)
    public ResponseEntity<ImportConfigResponseDto> importMtnGroupList(@RequestParam(value = "configFile") MultipartFile configFile,
                                                                      @RequestParam(value = "configName") String configName,
                                                                      @RequestParam(value = "importDate") Date importDate) {

        ImportConfigResponseDto response = importMtnGroupListService.importConfig(configName, configFile, importDate);
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @RequestMapping(value = "/import-mtn-group-schedule", method = RequestMethod.POST)
    public ResponseEntity<ImportConfigResponseDto> importMtnGroupSchedule(@RequestParam(value = "configFile") MultipartFile configFile,
                                                                          @RequestParam(value = "configName") String configName,
                                                                          @RequestParam(value = "importDate") Date importDate) {

        ImportConfigResponseDto response = importMtnGroupScheduleService.importConfig(configName, configFile, importDate);
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @RequestMapping(value = "/display-imports", method = RequestMethod.GET)
    public ResponseEntity<Page<ConfigImport>> displayImports(Pageable pageable) {
        return new ResponseEntity<>(importRcoaMpChannelConfigService.getConfigImports(ConfigImport.METERING, pageable), HttpStatus.OK);
    }
}
