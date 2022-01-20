package com.mendix.csv.aggregator.parallel.impl;

import com.mendix.csv.aggregator.config.ApplicationConfig;
import com.mendix.csv.aggregator.parallel.ParallelProcessingStrategy;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.mendix.csv.aggregator.tasks.util.TasksUtil.sleep;
import static com.mendix.csv.aggregator.tasks.util.TasksUtil.splitArrays;

/**
 * Reads, sorts and merges all CSV entries, running a Task (Worker Thread) by each file entry
 */
public class ParallelProcessingForkJoinImpl extends ParallelProcessingStrategy
{

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
                Stream<Path> allFilesFromDir = Files.find(directory,
                        1,
                        (path, basicFileAttributes) -> path.toFile().getName().matches(ApplicationConfig.DEFAULT_FILE_PATTERN)
                );

                List<CSVReaderTask> lsTasks = new ArrayList<CSVReaderTask>();
                allFilesFromDir.parallel().forEach( f -> {

                    CSVReaderTask t =  new CSVReaderTask( f );

                    lsTasks.add( t );

                    forkJoinPool.execute( t );

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

                    sleep();
                } while ( !TasksUtil.isEveryTaskFinished(lsTasksConv) );

                //forkJoinPool.awaitQuiescence( 40, TimeUnit.SECONDS );


                for ( CSVReaderTask task : lsTasks )
                {
                    List<String> partialList = task.join();
                    tSet.addAll( partialList );
                }

                forkJoinPool.shutdown();

            }
            catch (IOException e)
            {
                e.printStackTrace();
            }


        }

        return tSet;

    }
}
