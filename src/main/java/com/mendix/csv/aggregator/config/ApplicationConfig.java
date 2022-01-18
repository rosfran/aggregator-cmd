package com.mendix.csv.aggregator.config;

import com.mendix.csv.aggregator.service.CSVAggregatorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.env.Environment;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;


@ConfigurationProperties(prefix = "application")
//@ConfigurationProperties
public class ApplicationConfig {


    //Logger logger = LoggerFactory.getLogger(ApplicationConfig.class);

    public static final String TESTING_MEMORY = "2147480000";
    public static final String DRIVER_MEMORY = "571859200";
    public static final String MAX_RESULT_SIZE = "2147480000";

    public static final String DEFAULT_FILE_PATTERN = ".*.dat";

    @Autowired
    private Environment env;

    @Value("${app.name:csvaggregator}")
    private String appName;

//    @Value("${spark.home}")
//    private String sparkHome;

    @Value("${master.uri:local}")
    private String masterUri;

    @Value("${base.dir.resources:src/main/resources/}")
    private String resourcesDir;

    @Value("${base.dir.small:src/main/resources/small_example/}")
    private String smallFilesDir;

    @Value("${base.dir.medium:src/main/resources/medium_example/}")
    private String mediumFilesDir;

    @Value("${base.dir.volume:vol/}")
    private String dirVolume;


    @PostConstruct
    public void init() {
        //log.info("Init spark");
    }

    @PreDestroy
    public void destroy() {
    }

    @Bean
    public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
        return new PropertySourcesPlaceholderConfigurer();
    }

    public String getSmallFilesDir()
    {
        return smallFilesDir;
    }

    public String getMediumFilesDir()
    {
        return mediumFilesDir;
    }

    public String getResourcesDir()
    {
        return resourcesDir;
    }

    public String getDirVolume()
    {
        return dirVolume;
    }

}