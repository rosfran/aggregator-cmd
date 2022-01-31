package org.bendable.csv.aggregator.parallel.impl;

import org.bendable.csv.aggregator.config.ApplicationConfig;
import org.bendable.csv.aggregator.parallel.ParallelProcessingStrategy;
import org.bendable.csv.aggregator.parallel.ParallelStrategyType;
import org.bendable.csv.aggregator.tasks.CSVReaderChunkTask;
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
import java.util.stream.Stream;

import static org.bendable.csv.aggregator.tasks.util.TasksUtil.splitArrays;

/**
 * Reads, merges and sorts all entries from CSV files, using
 * a kind of "external sort" strategy, using Fork Join pool
 *
 * It will take 5 steps:
 *
 * 1. Creates in parallel a list of file names;
 * 2. Partition the files list, in that case, we create 4 lists;
 * 3. For each file name List, starts a Task (using the execute method from the ForkJoin thread pool). Each Task
 *    will read and sort all files listed in it;
 * 4. Signalize the ThreadPool to stop processing;
 * 5. Since the parallel processing stopped, we JOIN each individual Task, and sort everything, merging the results.
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
                /*
                 Obtains a list of files, which are obtained from a given directory
                 */
                List<Path> allFilesFromDir = Files.find(directory,
                        1,
                        (path, basicFileAttributes) -> path.toFile().getName().matches(ApplicationConfig.DEFAULT_FILE_PATTERN)
                ).parallel().collect( Collectors.toList());

                List<CSVReaderChunkTask> lsTasks = Collections.synchronizedList(new ArrayList<CSVReaderChunkTask>());

                /* This strategy will partition the file list, creating 4 partitions, or sets, of files */
                List<List<Path>> chunks = splitArrays(new ArrayList(allFilesFromDir), 4);

                /*
                  For each partition, execute an asynchronous Task that will read each of this
                  partitions in parallel. Each partition a number of files.
                 */
                for ( List<Path> col : chunks )
                {
                    CSVReaderChunkTask t =  new CSVReaderChunkTask( col );

                    lsTasks.add( t );

                    /* this actually forks this task, running it like a Thread */
                    forkJoinPool.execute( t );
                }
                //getLogger().info("total size after chunks = {} - quant. chunks = {} ", s, chunks.size());

                /* This code is only to convert the type of the elements of this list, for convenience,
                   because we don't need to create 2 versions of the function isEveryTaskFinished

                   Java has a problem - it doesn't consider as a polymorphic implementation if you
                   have 2 functions with the same name, but as parameters only a List with diverse
                   type of elements. For example, if I have 2 methods on the same class, one as
                   write(List<String>) and another write(List<StringBuffer>) it will result on
                   an exception.
                 */
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

                List<String> partialListResult = new ArrayList<String>();

                /* enters the REDUCE (JOIN) phase of parallel computing - by the way,
                  we only need to get the results of each individual Task, and join
                  these results, taking care of sorting all of them (this is done by
                  the TreeSet, which implements sorting for each added element
                  ( this is O(log n) time complexity )
                 */
                for ( int i = 0 ; i < lsTasks.size(); i++  )
                {

                    List<String> partialList1 = lsTasks.get(i).join();
                    tSet.addAll(partialList1);

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
