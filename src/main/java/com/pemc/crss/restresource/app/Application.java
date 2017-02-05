package com.pemc.crss.restresource.app;

import com.pemc.crss.meterprocess.core.main.CrssMainDatasourceConfig;
import com.pemc.crss.shared.core.dataflow.CrssDataflowDatasourceConfig;
import com.pemc.crss.shared.core.nmms.CrssNmmsDatasourceConfig;
import com.pemc.crss.shared.core.registration.CrssRegistrationDatasourceConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@EnableDiscoveryClient
@ComponentScan(basePackageClasses = {CrssMainDatasourceConfig.class,
        CrssRegistrationDatasourceConfig.class,
        CrssDataflowDatasourceConfig.class,
        CrssNmmsDatasourceConfig.class,
        Application.class})

public class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}
