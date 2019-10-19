package com.prashanth.os.spring.batch.gs;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@EnableBatchProcessing
@SpringBootApplication
public class SpringBatchGsApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringBatchGsApplication.class, args);
    }

}
