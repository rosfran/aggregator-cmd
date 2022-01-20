package com.mendix.csv.aggregator.parallel.impl;

import com.mendix.csv.aggregator.config.ApplicationConfig;
import com.mendix.csv.aggregator.parallel.ParallelProcessingStrategy;
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
import java.util.stream.Collectors;

import static com.mendix.csv.aggregator.tasks.util.TasksUtil.splitArrays;

public class ParallelProcessingExternalSortImpl extends ParallelProcessingStrategy
{

    @Override
    public Set<String> process(String dir) throws IOException
    {

        Path caminho = Paths.get(dir);

        Set<String> tSet =  new TreeSet<String>();

        final int numberOfProcessors = Runtime.getRuntime().availableProcessors();

        final ForkJoinPool forkJoinPool = new ForkJoinPool(numberOfProcessors);

        /* check if the source directory exists */
        if ( Files.exists(caminho) && Files.isDirectory(caminho))
        {
            try
            {
                List<Path> s = Files.find(caminho,
                        1,
                        (path, basicFileAttributes) -> path.toFile().getName().matches(ApplicationConfig.DEFAULT_FILE_PATTERN)
                ).collect( Collectors.toList());

                List<CSVReaderChunkTask> lsTasks = new ArrayList<CSVReaderChunkTask>();

                List<Path>[] chunks = splitArrays(new ArrayList(s), 6);

                for ( List<Path> col : chunks )
                {
                    CSVReaderChunkTask t =  new CSVReaderChunkTask( col );

                    lsTasks.add( t );

                    forkJoinPool.execute( t );
                }

                do
                {

                    System.out.printf("Parallelism: %d\n", forkJoinPool.getParallelism());
                    System.out.printf("Active Threads: %d\n", forkJoinPool.getActiveThreadCount());
                    System.out.printf("Task Count: %d\n", forkJoinPool.getQueuedTaskCount());
                    System.out.printf("Steal Count: %d\n", forkJoinPool.getStealCount());

                } while ( !TasksUtil.isEveryTaskFinished(lsTasks.stream()
                       .map(object -> (ForkJoinTask)object)
                        .collect(Collectors.toList())) );

                //forkJoinPool.awaitQuiescence( 40, TimeUnit.SECONDS );
                forkJoinPool.shutdown();

                for ( CSVReaderChunkTask task : lsTasks )
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
