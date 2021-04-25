package com.assignment.reportservice.influx;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;

@Configuration
public class InfluxConfig {

	    // You can generate a Token from the "Tokens Tab" in the UI
	    String token = "L88-KZ8PcYfFOyWVoPZwBFw3JnuXvs26gbAFit_FR9AGAlemNHu_XBJF4D2GWVaQK8NaMrl3Nx0dxQliIdz1tA==";
	    
	    @Bean
	    public InfluxDBClient client() {
	    	
	    	return InfluxDBClientFactory
	    			.create("https://eu-central-1-1.aws.cloud2.influxdata.com", token.toCharArray());
	    }
}
