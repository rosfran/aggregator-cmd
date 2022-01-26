package org.bendable.csv.aggregator;

import org.bendable.csv.aggregator.service.CSVAggregatorService;
import org.bendable.csv.aggregator.util.FilesUtil;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.Set;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertTrue;


@ExtendWith(SpringExtension.class)
@SpringBootTest
class AggregatorApplicationTests
{

	public static final int SMALL_FILES_RECORDS_SIZE = 551;

	public static final int MEDIUM_FILES_RECORDS_SIZE = 37652;
	public static final String SRC_MAIN_RESOURCES_MEDIUM_EXAMPLE = "src/main/resources/medium_example/";
	public static final String SRC_MAIN_RESOURCES_MEDIUM_EXTERNAL_SORT_DAT = "src/main/resources/medium_external_sort.dat";
	public static final String SRC_MAIN_RESOURCES_MEDIUM_PARALLEL_STREAM_DAT = "src/main/resources/medium_parallel_stream.dat";
	public static final String SRC_MAIN_RESOURCES_SMALL_EXAMPLE = "src/main/resources/small_example/";

	private static Logger logger = Logger.getLogger(AggregatorApplicationTests.class.getName());

	@Autowired
	private CSVAggregatorService csvAggregatorService;

	@Test
	@DisplayName("Runs the External Sorting parallel algorithm over the Medium Files")
	void testMediumFilesProcessingExternalSorting(TestInfo info)
	{
		logger.info(info.getDisplayName());
		Set<String> result = csvAggregatorService.processFileUsingExternalSortStrategy(
				SRC_MAIN_RESOURCES_MEDIUM_EXAMPLE);

		logger.info("size = "+result.size());

		csvAggregatorService.writeProcessedResultToFile(result, SRC_MAIN_RESOURCES_MEDIUM_EXTERNAL_SORT_DAT);

		assertTrue( result.size() == MEDIUM_FILES_RECORDS_SIZE,
				"Total result" );

	}

	@Test
	@DisplayName("Runs the Java Parallel Streams on the Medium Files")
	void testMediumFilesProcessingParallelStream( TestInfo info )
	{
		logger.info(info.getDisplayName());

		Set<String> result = csvAggregatorService.processFileUsingParallelStreamStrategy(SRC_MAIN_RESOURCES_MEDIUM_EXAMPLE);

		logger.info("size = "+result.size());

		csvAggregatorService.writeProcessedResultToFile(result, SRC_MAIN_RESOURCES_MEDIUM_PARALLEL_STREAM_DAT);

		assertTrue( result.size() == MEDIUM_FILES_RECORDS_SIZE,
				"Total result" );

	}

	@Test
	@DisplayName("Runs the Java Sequential Streams on the Medium Files")
	void testMediumFilesProcessingSequentialStream(TestInfo info )
	{
		logger.info(info.getDisplayName());
		Set<String> result = csvAggregatorService.processFileUsingSequentialStreamStrategy(SRC_MAIN_RESOURCES_MEDIUM_EXAMPLE);

		logger.info("size = "+result.size());

		FilesUtil.writeAllLinesSortedToFile(result, "src/main/resources/medium_noparallel.dat");

		assertTrue( result.size() == MEDIUM_FILES_RECORDS_SIZE,
				"Total result" );

	}

	@RepeatedTest(3)
	@DisplayName("Runs the ForkJoin model on the Medium Files")
	void testMediumFilesProcessingForkJoin(TestInfo info )
	{
		logger.info(info.getDisplayName());
		Set<String> result = csvAggregatorService.processFileUsingForkJoinStrategy(SRC_MAIN_RESOURCES_MEDIUM_EXAMPLE);

		logger.info("size = "+result.size());

		csvAggregatorService.writeProcessedResultToFile(result, "src/main/resources/medium_fork_join.dat");

		assertTrue( result.size() == MEDIUM_FILES_RECORDS_SIZE,
				"Total result" );

	}


	@Test
	@DisplayName("Runs the External Sorting parallel algorithm over the Small Files")
	void testSmallFilesProcessingExternalSorting(TestInfo info )
	{
		logger.info(info.getDisplayName());
		Set<String> result = csvAggregatorService.processFileUsingExternalSortStrategy(SRC_MAIN_RESOURCES_SMALL_EXAMPLE);

		logger.info("size = "+result.size());

		csvAggregatorService.writeProcessedResultToFile(result, "src/main/resources/small_external_sorting.dat");

		assertTrue( result.size() == SMALL_FILES_RECORDS_SIZE,
				"Total result" );

	}

	@Test
	@DisplayName("Runs the Java Parallel Streams on the Small Files")
	void testSmallFilesProcessingParallelStream(TestInfo info )
	{
		logger.info(info.getDisplayName());
		Set<String> result = csvAggregatorService.processFileUsingParallelStreamStrategy(SRC_MAIN_RESOURCES_SMALL_EXAMPLE);

		logger.info("size = "+result.size());

		csvAggregatorService.writeProcessedResultToFile(result, "src/main/resources/small_parallel_stream.dat");

		assertTrue( result.size() == SMALL_FILES_RECORDS_SIZE,
				"Total result" );

	}

	@Test
	@DisplayName("Runs the Java Sequential Streams on the Small Files")
	void testSmallFilesProcessingSequentialStream(TestInfo info )
	{
		logger.info(info.getDisplayName());
		Set<String> result = csvAggregatorService.processFileUsingSequentialStreamStrategy(SRC_MAIN_RESOURCES_SMALL_EXAMPLE);

		logger.info("size = "+result.size());

		csvAggregatorService.writeProcessedResultToFile(result, "src/main/resources/small_noparallel.dat");

		assertTrue( result.size() == SMALL_FILES_RECORDS_SIZE,
				"Total result" );

	}

	@RepeatedTest(3)
	@DisplayName("Runs the ForkJoin model on the Small Files")
	void testSmallFilesProcessingForkJoin(TestInfo info )
	{
		logger.info(info.getDisplayName());
		Set<String> result = csvAggregatorService.processFileUsingForkJoinStrategy(SRC_MAIN_RESOURCES_SMALL_EXAMPLE);

		logger.info("size = "+result.size());

		csvAggregatorService.writeProcessedResultToFile(result, "src/main/resources/small_fork_join.dat");

		assertTrue( result.size() == SMALL_FILES_RECORDS_SIZE,
				"Total result" );

	}

}
