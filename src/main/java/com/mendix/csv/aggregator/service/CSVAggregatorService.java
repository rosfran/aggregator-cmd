package com.mendix.csv.aggregator.service;


import com.mendix.csv.aggregator.config.ApplicationConfig;


import com.mendix.csv.aggregator.parallel.ParallelProcessingStrategy;
import com.mendix.csv.aggregator.parallel.impl.ParallelProcessingExternalSortImpl;
import com.mendix.csv.aggregator.parallel.impl.ParallelProcessingForkJoinImpl;
import com.mendix.csv.aggregator.parallel.impl.ParallelProcessingNoParallelImpl;
import com.mendix.csv.aggregator.parallel.impl.ParallelProcessingStreamImpl;
import com.mendix.csv.aggregator.util.FilesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Set;


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
