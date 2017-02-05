package com.pemc.crss.restresource.app.resource;

import com.pemc.crss.meterprocess.core.main.entity.mtn.MTNModelConfig;
import com.pemc.crss.meterprocess.core.main.repository.MTNModelConfigRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/retrieve-mtn-config")
public class RetrieveMtnModelConfigResource {

    @Autowired
    private MTNModelConfigRepository mtnModelConfigRepository;

    @RequestMapping(value = "/{configName}", method = RequestMethod.GET, produces = "application/json")
    public MTNModelConfig getModelConfig(@PathVariable("configName") String configName) {
        return mtnModelConfigRepository.findByMtnConfigName(configName);
    }
}
