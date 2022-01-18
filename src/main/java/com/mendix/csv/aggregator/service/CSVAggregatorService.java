package com.mendix.csv.aggregator.service;


import com.mendix.csv.aggregator.config.ApplicationConfig;
import com.mendix.csv.aggregator.service.types.FileTypeEnum;
import com.mendix.csv.aggregator.util.FilesUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;


@Component
public class CSVAggregatorService
{

    //Logger logger = LoggerFactory.getLogger(CSVAggregatorService.class);

    Logger logger = LoggerFactory.getLogger(CSVAggregatorService.class);

    @Autowired
    private ApplicationConfig config;


}
