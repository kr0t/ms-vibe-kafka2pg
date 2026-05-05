package com.example.msloader;

import com.example.msloader.config.LoaderProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
@EnableConfigurationProperties(LoaderProperties.class)
public class MsSpringLoaderApplication {

    public static void main(String[] args) {
        SpringApplication.run(MsSpringLoaderApplication.class, args);
    }
}
