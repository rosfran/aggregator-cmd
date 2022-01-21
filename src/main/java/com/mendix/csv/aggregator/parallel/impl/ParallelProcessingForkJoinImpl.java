package com.mendix.csv.aggregator.parallel.impl;

import com.mendix.csv.aggregator.config.ApplicationConfig;
import com.mendix.csv.aggregator.parallel.ParallelProcessingStrategy;
import com.mendix.csv.aggregator.parallel.ParallelStrategyType;
import com.mendix.csv.aggregator.tasks.CSVReaderChunkTask;
import com.mendix.csv.aggregator.tasks.CSVReaderTask;
import com.mendix.csv.aggregator.tasks.util.TasksUtil;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


/**
 * Reads, sorts and merges all CSV entries, running a Task (Worker Thread) by each file entry
 */
public class ParallelProcessingForkJoinImpl extends ParallelProcessingStrategy
{

    @Override
    public ParallelStrategyType getStrategyType()
    {
        return ParallelStrategyType.FORK_JOIN;
    }

    @Override
    public Set<String> process(String dir) throws IOException
    {
        Path directory = Paths.get(dir);

        Set<String> tSet = Collections.synchronizedSet(new TreeSet<String>());

        final ForkJoinPool forkJoinPool = new ForkJoinPool(numberOfProcessors);

        /* check if the source directory exists */
        if ( Files.exists(directory) && Files.isDirectory(directory))
        {
            try
            {
                List<CSVReaderTask> lsTasks = Collections.synchronizedList(new ArrayList<CSVReaderTask>());

                Files.find(directory,
                        1,
                        (path, basicFileAttributes) -> path.toFile().getName().matches(ApplicationConfig.DEFAULT_FILE_PATTERN)
                ).parallel().forEach( f -> {

                    CSVReaderTask t =  new CSVReaderTask( f );

                    lsTasks.add( t );

                    forkJoinPool.submit( t );

                } );

                List<ForkJoinTask> lsTasksConv = lsTasks.stream()
                        .map(object -> (ForkJoinTask)object)
                        .collect(Collectors.toList());
                do
                {
//                    System.out.printf("Parallelism: %d\n", forkJoinPool.getParallelism());
//                    System.out.printf("Active Threads: %d\n", forkJoinPool.getActiveThreadCount());
//                    System.out.printf("Task Count: %d\n", forkJoinPool.getQueuedTaskCount());
//                    System.out.printf("Steal Count: %d\n", forkJoinPool.getStealCount());

                   // sleep();
                } while ( !TasksUtil.isEveryTaskFinished(lsTasksConv) );

                forkJoinPool.shutdown();

                try {
                    if (!forkJoinPool.awaitTermination( 40, TimeUnit.SECONDS )) {
                        logger.warn("Failed orderly shutdown");
                    }
                } catch (InterruptedException ex) {
                    logger.warn("Failed orderly shutdown", ex);
                }

                for ( CSVReaderTask task : lsTasks )
                {
                    List<String> partialList = task.join();
                    tSet.addAll( partialList );
                }

            }
            catch (IOException e)
            {
                e.printStackTrace();
            }

        }

        return tSet;

    }



}
