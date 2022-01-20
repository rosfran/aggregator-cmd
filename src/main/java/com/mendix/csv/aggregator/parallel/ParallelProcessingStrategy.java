package com.mendix.csv.aggregator.parallel;

import com.mendix.csv.aggregator.util.FilesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.Set;

public abstract class ParallelProcessingStrategy implements ParallelProcessingStrategyInterface
{

    Logger logger = LoggerFactory.getLogger(ParallelProcessingStrategy.class);

    long start = 0L;

    long end = 0L;

    @Override
    public void preProcessing()
    {
        start = Instant.now().toEpochMilli();
        logger.info(String.format("\tStarted task in %d milliseconds", start));
    }

    @Override
    public void postProcessing()
    {
        end = Instant.now().toEpochMilli();
        logger.info(String.format("\tCompleted task in %d milliseconds", (end - start)));
    }

    public Set<String> process(String dir) throws IOException
    {

        return null;

    }

    public Set<String> processAllFiles(String dir) throws IOException
    {
        preProcessing();

        Set<String> s =  process(dir);
        postProcessing();

        return s;

    }
}
