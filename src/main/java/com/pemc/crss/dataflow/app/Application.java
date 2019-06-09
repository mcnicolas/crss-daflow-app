package com.pemc.crss.dataflow.app;

import com.pemc.crss.shared.core.config.cache.CachingConfig;
import com.pemc.crss.shared.core.config.cache.redis.RedisConfig;
import com.pemc.crss.shared.core.dataflow.CrssDataflowDatasourceConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableScheduling;


@SpringBootApplication
@EnableScheduling
@EnableCaching
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
