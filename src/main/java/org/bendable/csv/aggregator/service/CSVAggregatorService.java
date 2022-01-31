package org.bendable.csv.aggregator.service;


import org.bendable.csv.aggregator.config.ApplicationConfig;


import org.bendable.csv.aggregator.parallel.ParallelProcessingStrategy;
import org.bendable.csv.aggregator.parallel.impl.ParallelProcessingExternalSortImpl;
import org.bendable.csv.aggregator.parallel.impl.ParallelProcessingForkJoinImpl;
import org.bendable.csv.aggregator.parallel.impl.ParallelProcessingNoParallelImpl;
import org.bendable.csv.aggregator.parallel.impl.ParallelProcessingStreamImpl;
import org.bendable.csv.aggregator.util.FilesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Set;

/**
 * This is a wrapper for 4 strategies of reading and sorting the DAT files. For example, the method
 * processFileUsingExternalSortStrategy uses the External Sort strategy, which consists on reading a
 * list of CSV files asynchronously, sort all this files in increasing order (based on the only column value),
 * write all these ordered values intoa unique CSV file.
 */
@Component
public class CSVAggregatorService
{

     Logger logger = LoggerFactory.getLogger(CSVAggregatorService.class);

    @Autowired
    private ApplicationConfig config;

    public ParallelProcessingStrategy createForkJoinStrategy()
    {
        return new ParallelProcessingForkJoinImpl();
    }

    public ParallelProcessingStrategy createExternalSortingStrategy()
    {
        return new ParallelProcessingExternalSortImpl();
    }

    public ParallelProcessingStrategy createParallelStreamStrategy()
    {
        return new ParallelProcessingStreamImpl();
    }

    public ParallelProcessingStrategy createSequentialStreamStrategy()
    {
        return new ParallelProcessingNoParallelImpl();
    }

    private final Set<String> processFileUsingStrategy( ParallelProcessingStrategy concurrency,
                                                  final String inputDir)
    {

        Set<String> result = null;
        try
        {
            result = concurrency.processAllFiles(inputDir );
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        return result;

    }

    public Set<String> processFileUsingForkJoinStrategy( final String inputDir )
    {
        return processFileUsingStrategy( createForkJoinStrategy(), inputDir );
    }

    public Set<String> processFileUsingExternalSortStrategy( final String inputDir )
    {
        return processFileUsingStrategy( createExternalSortingStrategy(), inputDir );
    }

    public Set<String> processFileUsingParallelStreamStrategy( final String inputDir )
    {
        return processFileUsingStrategy( createParallelStreamStrategy(), inputDir );
    }

    public Set<String> processFileUsingSequentialStreamStrategy( final String inputDir )
    {
        return processFileUsingStrategy( createSequentialStreamStrategy(), inputDir );
    }

    public void writeProcessedResultToFile( Set<String> result, final String outputDir )
    {
        FilesUtil.writeAllLinesSortedToFile(result, outputDir );
    }


}
