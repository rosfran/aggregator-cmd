package org.bendable.csv.aggregator.parallel.impl;

import org.bendable.csv.aggregator.config.ApplicationConfig;
import org.bendable.csv.aggregator.parallel.ParallelProcessingStrategy;
import org.bendable.csv.aggregator.parallel.ParallelStrategyType;
import org.bendable.csv.aggregator.tasks.CSVReaderTask;
import org.bendable.csv.aggregator.tasks.util.TasksUtil;

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
 *
 * It will 4 some steps:
 *
 * 1. Creates in parallel a list of file names;
 * 2. For each file name, starts a Task (using the execute method from the ForkJoin thread pool). This Task
 *    will read and sort each file;
 * 3. Signalize the ThreadPool to stop (by the way, this command shutdown() here forks all Tasks);
 * 4. Since the parallel processing stopped, we JOIN each individual Task, and sort everything, merging the results.
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

                /*
                 Obtains a list of files, which are obtained from a given directory
                 */
                Files.find(directory,
                        1,
                        (path, basicFileAttributes) -> path.toFile().getName().matches(ApplicationConfig.DEFAULT_FILE_PATTERN)
                ).parallel().forEach( f -> {

                    CSVReaderTask t =  new CSVReaderTask( f );

                    lsTasks.add( t );

                    /* this actually forks this task, running it like a Thread */
                    forkJoinPool.execute( t );

                } );

                 /* This code is only to convert the type of the elements of this list, for convenience,
                   because we don't need to create 2 versions of the function isEveryTaskFinished

                   Java has a problem - it doesn't consider as a polymorphic implementation if you
                   have 2 functions with the same name, but as parameters only a List with diverse
                   type of elements. For example, if I have 2 methods on the same class, one as
                   write(List<String>) and another write(List<StringBuffer>) it will result on
                   an exception.
                 */
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

                /* this is a common step when using these pools - it only signalize the thread pool,
                 * doesn't really interrupt all the threads immediately
                 */
                forkJoinPool.shutdown();

                try {
                    if (!forkJoinPool.awaitTermination( 40, TimeUnit.SECONDS )) {
                        logger.warn("Failed orderly shutdown");
                    }
                } catch (InterruptedException ex) {
                    logger.warn("Failed orderly shutdown", ex);
                }

               /* enters the REDUCE (JOIN) phase of parallel computing - by the way,
                  we only need to get the results of each individual Task, and join
                  these results, taking care of sorting all of them (this is done by
                  the TreeSet, which implements sorting for each added element
                  ( this is O(log n) time complexity )
                 */
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
