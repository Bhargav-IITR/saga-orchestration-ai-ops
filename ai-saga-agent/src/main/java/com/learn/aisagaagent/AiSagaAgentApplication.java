package com.learn.aisagaagent;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;


@SpringBootApplication(scanBasePackages = {"com.learn.aisagaagent", "com.learn.sagacommons"})
@ConfigurationPropertiesScan
@EnableScheduling
public class AiSagaAgentApplication {

	public static void main(String[] args) {
		SpringApplication.run(AiSagaAgentApplication.class, args);
	}

}
