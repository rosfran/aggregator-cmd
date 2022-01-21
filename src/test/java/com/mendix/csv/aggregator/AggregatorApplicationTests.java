package com.mendix.csv.aggregator;

import com.mendix.csv.aggregator.parallel.ParallelProcessingStrategy;
import com.mendix.csv.aggregator.parallel.impl.ParallelProcessingExternalSortImpl;
import com.mendix.csv.aggregator.parallel.impl.ParallelProcessingForkJoinImpl;
import com.mendix.csv.aggregator.parallel.impl.ParallelProcessingNoParallelImpl;
import com.mendix.csv.aggregator.parallel.impl.ParallelProcessingStreamImpl;
import com.mendix.csv.aggregator.util.FilesUtil;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.io.IOException;
import java.util.Set;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertTrue;


@ExtendWith(SpringExtension.class)
@SpringBootTest
class AggregatorApplicationTests
{

	public static final int SMALL_FILES_RECORDS_SIZE = 551;

	public static final int MEDIUM_FILES_RECORDS_SIZE = 37652;

	private static Logger logger = Logger.getLogger(AggregatorApplicationTests.class.getName());

	@Test
	@DisplayName("Runs the External Sorting parallel algorithm over the Medium Files")
	void testMediumFilesProcessingExternalSorting(TestInfo info)
	{
		logger.info(info.getDisplayName());
		ParallelProcessingStrategy concurrency = new ParallelProcessingExternalSortImpl();
		Set<String> result = null;
		try
		{
			result = concurrency.processAllFiles("src/main/resources/medium_example/");
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		//Stream.of(result).forEach(System.out::println);

		logger.info("size = "+result.size());

		FilesUtil.writeAllLinesSortedToFile(result, "src/main/resources/medium_external_sort.dat");

		assertTrue( result.size() == MEDIUM_FILES_RECORDS_SIZE,
				"Total result" );

	}

	@Test
	@DisplayName("Runs the Java Parallel Streams on the Medium Files")
	void testMediumFilesProcessingParallelStream( TestInfo info )
	{
		logger.info(info.getDisplayName());
		ParallelProcessingStrategy concurrency = new ParallelProcessingStreamImpl();
		Set<String> result = null;
		try
		{
			result = concurrency.processAllFiles("src/main/resources/medium_example/");
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		//Stream.of(result).forEach(System.out::println);

		logger.info("size = "+result.size());

		FilesUtil.writeAllLinesSortedToFile(result, "src/main/resources/medium_parallel_stream.dat");

		assertTrue( result.size() == MEDIUM_FILES_RECORDS_SIZE,
				"Total result" );

	}

	@Test
	@DisplayName("Runs the Java Sequential Streams on the Medium Files")
	void testMediumFilesProcessingSequentialStream(TestInfo info )
	{
		logger.info(info.getDisplayName());
		ParallelProcessingStrategy concurrency = new ParallelProcessingNoParallelImpl();
		Set<String> result = null;
		try
		{
			result = concurrency.processAllFiles("src/main/resources/medium_example/");
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		//Stream.of(result).forEach(System.out::println);

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
		ParallelProcessingStrategy concurrency = new ParallelProcessingForkJoinImpl();
		Set<String> result = null;
		try
		{
			result = concurrency.processAllFiles("src/main/resources/medium_example/");
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		//Stream.of(result).forEach(System.out::println);

		logger.info("size = "+result.size());

		FilesUtil.writeAllLinesSortedToFile(result, "src/main/resources/medium_fork_join.dat");

		assertTrue( result.size() == MEDIUM_FILES_RECORDS_SIZE,
				"Total result" );

	}


	@Test
	@DisplayName("Runs the External Sorting parallel algorithm over the Small Files")
	void testSmallFilesProcessingExternalSorting(TestInfo info )
	{
		logger.info(info.getDisplayName());
		ParallelProcessingStrategy concurrency = new ParallelProcessingExternalSortImpl();
		Set<String> result = null;
		try
		{
			result = concurrency.processAllFiles("src/main/resources/small_example/");
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		//Stream.of(result).forEach(System.out::println);

		logger.info("size = "+result.size());

		FilesUtil.writeAllLinesSortedToFile(result, "src/main/resources/small_external_sorting.dat");

		assertTrue( result.size() == SMALL_FILES_RECORDS_SIZE,
				"Total result" );

	}

	@Test
	@DisplayName("Runs the Java Parallel Streams on the Small Files")
	void testSmallFilesProcessingParallelStream(TestInfo info )
	{
		logger.info(info.getDisplayName());
		ParallelProcessingStrategy concurrency = new ParallelProcessingStreamImpl();
		Set<String> result = null;
		try
		{
			result = concurrency.processAllFiles("src/main/resources/small_example/");
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		//Stream.of(result).forEach(System.out::println);

		logger.info("size = "+result.size());

		FilesUtil.writeAllLinesSortedToFile(result, "src/main/resources/small_parallel_stream.dat");

		assertTrue( result.size() == SMALL_FILES_RECORDS_SIZE,
				"Total result" );

	}

	@Test
	@DisplayName("Runs the Java Sequential Streams on the Small Files")
	void testSmallFilesProcessingSequentialStream(TestInfo info )
	{
		logger.info(info.getDisplayName());
		ParallelProcessingStrategy concurrency = new ParallelProcessingNoParallelImpl();
		Set<String> result = null;
		try
		{
			result = concurrency.processAllFiles("src/main/resources/small_example/");
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		//Stream.of(result).forEach(System.out::println);

		logger.info("size = "+result.size());

		FilesUtil.writeAllLinesSortedToFile(result, "src/main/resources/small_noparallel.dat");

		assertTrue( result.size() == SMALL_FILES_RECORDS_SIZE,
				"Total result" );

	}

	@RepeatedTest(3)
	@DisplayName("Runs the ForkJoin model on the Small Files")
	void testSmallFilesProcessingForkJoin(TestInfo info )
	{
		logger.info(info.getDisplayName());
		ParallelProcessingStrategy concurrency = new ParallelProcessingForkJoinImpl();
		Set<String> result = null;
		try
		{
			result = concurrency.processAllFiles("src/main/resources/small_example/");
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		//Stream.of(result).forEach(System.out::println);

		logger.info("size = "+result.size());

		FilesUtil.writeAllLinesSortedToFile(result, "src/main/resources/small_fork_join.dat");

		assertTrue( result.size() == SMALL_FILES_RECORDS_SIZE,
				"Total result" );

	}

}
