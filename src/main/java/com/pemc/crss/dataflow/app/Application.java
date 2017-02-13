package com.pemc.crss.dataflow.app;

import com.pemc.crss.meterprocess.core.main.CrssMainDatasourceConfig;
import com.pemc.crss.shared.core.dataflow.CrssDataflowDatasourceConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.context.annotation.ComponentScan;


@SpringBootApplication
//@EnableDiscoveryClient
@ComponentScan(basePackageClasses = {CrssMainDatasourceConfig.class,
        CrssDataflowDatasourceConfig.class,
        Application.class})

public class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}
