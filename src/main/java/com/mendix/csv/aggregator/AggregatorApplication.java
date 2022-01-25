package com.mendix.csv.aggregator;

import com.mendix.csv.aggregator.config.ApplicationConfig;
import com.mendix.csv.aggregator.parallel.ParallelProcessingStrategy;
import com.mendix.csv.aggregator.parallel.ParallelStrategyType;
import com.mendix.csv.aggregator.parallel.impl.ParallelProcessingExternalSortImpl;
import com.mendix.csv.aggregator.parallel.impl.ParallelProcessingForkJoinImpl;
import com.mendix.csv.aggregator.parallel.impl.ParallelProcessingNoParallelImpl;
import com.mendix.csv.aggregator.service.CSVAggregatorService;
import com.mendix.csv.aggregator.util.FilesUtil;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Stream;

@SpringBootApplication
@EnableConfigurationProperties({ApplicationConfig.class})
@ComponentScan(basePackages = { "com.mendix.csv.aggregator" })
public class AggregatorApplication implements ApplicationRunner
{

	private static final Logger logger = LoggerFactory.getLogger(AggregatorApplication.class);

	@Autowired
	private CSVAggregatorService csvAggregatorService;

	public static void main(String[] args) {

		SpringApplication.run(AggregatorApplication.class, args);

	}

	@Override
	public void run(ApplicationArguments args) throws Exception
	{
		logger.info("Application started with command-line arguments: {}", Arrays.toString(args.getSourceArgs()));
		logger.info("NonOptionArgs: {}", args.getNonOptionArgs());
		logger.info("OptionNames: {}", args.getOptionNames());

		String fromDir = null;
		String toFile = null;
		ParallelStrategyType strategy = null;

		for (String name : args.getNonOptionArgs()){
			logger.info("arg-" + name );

			if (name.startsWith("fromDir"))
			{
				fromDir = name.substring(name.indexOf('=')+1);
				logger.info("Contains fromDir: " + fromDir);
			}

			if (name.startsWith("toFile"))
			{
				toFile =  name.substring(name.indexOf('=')+1);
				logger.info("Contains toFile: " + toFile);
			}

			if (name.startsWith("strategy"))
			{
				String st =  name.substring(name.indexOf('=')+1);

				strategy = ParallelStrategyType.fromArgument(st);
				logger.info("Contains strategy: " + strategy.getDescription());
			}
		}

		fromDir = Strings.isNotEmpty(fromDir) ? fromDir : "src/main/resources/medium_example/";

		toFile = Strings.isNotEmpty(toFile) ? toFile : "src/main/resources/merged_file.dat";

		ParallelProcessingStrategy concurrency = null;

		Set<String> result = null;

		if ( strategy == ParallelStrategyType.EXTERNAL_SORT)
		{
			result = csvAggregatorService.processFileUsingExternalSortStrategy( fromDir );

		} else if ( strategy == ParallelStrategyType.PARALLEL_STREAM)
		{
			result = csvAggregatorService.processFileUsingParallelStreamStrategy( fromDir );
		} else if ( strategy == ParallelStrategyType.SEQUENTIAL_STREAM)
		{
			result = csvAggregatorService.processFileUsingSequentialStreamStrategy( fromDir );
		} else {
			result = csvAggregatorService.processFileUsingForkJoinStrategy(fromDir);
		}

		logger.info("size = {}", result.size());

		csvAggregatorService.writeProcessedResultToFile(result, toFile);

	}

}
