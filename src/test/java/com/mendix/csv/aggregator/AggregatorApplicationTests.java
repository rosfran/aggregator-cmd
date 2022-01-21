package com.mendix.csv.aggregator;

import com.mendix.csv.aggregator.parallel.ParallelProcessingStrategy;
import com.mendix.csv.aggregator.parallel.impl.ParallelProcessingExternalSortImpl;
import com.mendix.csv.aggregator.parallel.impl.ParallelProcessingForkJoinImpl;
import com.mendix.csv.aggregator.parallel.impl.ParallelProcessingStreamImpl;
import com.mendix.csv.aggregator.util.FilesUtil;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.io.IOException;
import java.util.Set;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertTrue;


@ExtendWith(SpringExtension.class)
@SpringBootTest()
class AggregatorApplicationTests
{

	private static Logger logger = Logger.getLogger(AggregatorApplicationTests.class.getName());

	@Test
	@DisplayName("Runs the External Sorting parallel algorithm over the Medium Files")
	void testMediumFilesProcessingExternalSorting()
	{
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

		System.out.println("size = "+result.size());

		FilesUtil.writeAllLinesSortedToFile(result, "src/main/resources/t.dat");

		assertTrue( result.size() == 37652,
				"Total result" );

	}

	@Test
	@DisplayName("Runs the Java Parallel Streams on the Medium Files")
	void testMediumFilesProcessingParallelStream()
	{
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

		System.out.println("size = "+result.size());

		FilesUtil.writeAllLinesSortedToFile(result, "src/main/resources/t.dat");

		assertTrue( result.size() == 37652,
				"Total result" );

	}


	@RepeatedTest(3)
	@DisplayName("Runs the ForkJoin model on the Medium Files")
	void testMediumFilesProcessingForkJoin()
	{
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

		System.out.println("size = "+result.size());

		FilesUtil.writeAllLinesSortedToFile(result, "src/main/resources/t.dat");

		assertTrue( result.size() == 37652,
				"Total result" );

	}


	@Test
	@DisplayName("Runs the External Sorting parallel algorithm over the Small Files")
	void testSmallFilesProcessingExternalSorting()
	{
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

		System.out.println("size = "+result.size());

		FilesUtil.writeAllLinesSortedToFile(result, "src/main/resources/t.dat");

		assertTrue( result.size() == 551,
				"Total result" );

	}

	@Test
	@DisplayName("Runs the Java Parallel Streams on the Small Files")
	void testSmallFilesProcessingParallelStream()
	{
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

		System.out.println("size = "+result.size());

		FilesUtil.writeAllLinesSortedToFile(result, "src/main/resources/t.dat");

		assertTrue( result.size() == 551,
				"Total result" );

	}


	@RepeatedTest(3)
	@DisplayName("Runs the ForkJoin model on the Small Files")
	void testSmallFilesProcessingForkJoin()
	{
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

		System.out.println("size = "+result.size());

		FilesUtil.writeAllLinesSortedToFile(result, "src/main/resources/t.dat");

		assertTrue( result.size() == 551,
				"Total result" );

	}

}
