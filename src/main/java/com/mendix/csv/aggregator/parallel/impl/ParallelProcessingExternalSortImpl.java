package com.mendix.csv.aggregator.parallel.impl;

import com.mendix.csv.aggregator.config.ApplicationConfig;
import com.mendix.csv.aggregator.parallel.ParallelProcessingStrategy;
import com.mendix.csv.aggregator.parallel.ParallelStrategyType;
import com.mendix.csv.aggregator.tasks.CSVReaderChunkTask;
import com.mendix.csv.aggregator.tasks.util.TasksUtil;
import org.apache.logging.log4j.util.Strings;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.mendix.csv.aggregator.tasks.util.TasksUtil.splitArrays;

/**
 * Reads, merges and sorts all entries from CSV files, using
 * a kind of "external sort" strategy, using Fork Join pool
 */
public class ParallelProcessingExternalSortImpl extends ParallelProcessingStrategy
{

    @Override
    public ParallelStrategyType getStrategyType()
    {
        return ParallelStrategyType.EXTERNAL_SORT;
    }

    @Override
    public Set<String> process(String dir) throws IOException
    {

        Path directory = Paths.get(dir);

        Set<String> tSet =  new TreeSet<String>();

        final ForkJoinPool forkJoinPool = new ForkJoinPool(numberOfProcessors );

        /* check if the source directory exists */
        if ( Files.exists(directory) && Files.isDirectory(directory))
        {
            try
            {
                List<Path> allFilesFromDir = Files.find(directory,
                        1,
                        (path, basicFileAttributes) -> path.toFile().getName().matches(ApplicationConfig.DEFAULT_FILE_PATTERN)
                ).parallel().collect( Collectors.toList());

                List<CSVReaderChunkTask> lsTasks = Collections.synchronizedList(new ArrayList<CSVReaderChunkTask>());

                List<List<Path>> chunks = splitArrays(new ArrayList(allFilesFromDir), 4);

                //int s = 0;
                for ( List<Path> col : chunks )
                {
                    CSVReaderChunkTask t =  new CSVReaderChunkTask( col );

                    lsTasks.add( t );

                    forkJoinPool.execute( t );

                    //s+= col.size();

                }
                //getLogger().info("total size after chunks = {} - quant. chunks = {} ", s, chunks.size());

                List<ForkJoinTask> lsTasksConv = lsTasks.parallelStream()
                        .map(object -> (ForkJoinTask)object)
                        .parallel()
                        .collect(Collectors.toList());
                do
                {
//                    System.out.printf("Parallelism: %d\n", forkJoinPool.getParallelism());
//                    System.out.printf("Active Threads: %d\n", forkJoinPool.getActiveThreadCount());
//                    System.out.printf("Task Count: %d\n", forkJoinPool.getQueuedTaskCount());
//                    System.out.printf("Steal Count: %d\n", forkJoinPool.getStealCount());

                } while ( !TasksUtil.isEveryTaskFinished(lsTasksConv) );

                forkJoinPool.shutdown();

                try {
                    if (!forkJoinPool.awaitTermination( 40, TimeUnit.SECONDS )) {
                        logger.warn("Failed orderly shutdown");
                    }
                } catch (InterruptedException ex) {
                    logger.warn("Failed orderly shutdown", ex);
                }

                List<String> partialListResult = new ArrayList<String>();

                for ( int i = 0 ; i < lsTasks.size(); i++  )
                {

                    List<String> partialList1 = lsTasks.get(i).join();

                    //getLogger().info("partialList1.size() = "+partialList1.size());
                    partialListResult = Collections.synchronizedList( Stream.concat(
                                    partialListResult.parallelStream(), partialList1.parallelStream())
                            .sorted().collect(Collectors.toList()) );

                }

                tSet = new TreeSet<String>(partialListResult);


            }
            catch (IOException e)
            {
                e.printStackTrace();
            }


        }

        return tSet;
    }
}
