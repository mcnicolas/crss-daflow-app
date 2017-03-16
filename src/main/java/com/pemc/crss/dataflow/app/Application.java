package com.pemc.crss.dataflow.app;

import com.pemc.crss.shared.core.config.cache.CachingConfig;
import com.pemc.crss.shared.core.config.cache.redis.RedisConfig;
import com.pemc.crss.shared.core.dataflow.CrssDataflowDatasourceConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;


@SpringBootApplication
@ComponentScan(basePackageClasses = {
        CrssDataflowDatasourceConfig.class,
        CachingConfig.class,
        RedisConfig.class,
        Application.class})

public class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}
