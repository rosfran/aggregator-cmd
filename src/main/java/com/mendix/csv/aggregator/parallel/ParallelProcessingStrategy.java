package com.mendix.csv.aggregator.parallel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.Set;

/**
 * Abstract class implementing some basic methods which are needed by the child classes
 */
public abstract class ParallelProcessingStrategy implements ParallelProcessingStrategyInterface
{

    public Logger logger = LoggerFactory.getLogger(ParallelProcessingStrategy.class);

    public static final int numberOfProcessors = Runtime.getRuntime().availableProcessors();

    long start = 0L;

    long end = 0L;

    @Override
    public void preProcessing()
    {
        start = Instant.now().toEpochMilli();
        logger.info(String.format("\tStarted task."));
    }

    @Override
    public void postProcessing()
    {
        end = Instant.now().toEpochMilli();
        logger.info(String.format("\tCompleted task in %d milliseconds", (end - start)));
    }

    /**
     * This method will be implemented on the child classes
     * This method aims to be overloaded by child classes - this is why because it returns null here
     *
     * @param dir The directory to extract the DAT files (dictionaries)
     * @return A TreeSet with all records sorted
     * @throws IOException
     */
    public Set<String> process(String dir) throws IOException
    {

        return null;

    }

    /**
     * Method called by the client which runs the processment over all files
     *
     * @param dir The directory to extract the DAT files (dictionaries)
     * @return A TreeSet with all records sorted
     * @throws IOException
     */
    public Set<String> processAllFiles(String dir) throws IOException
    {
        preProcessing();

        Set<String> s =  process(dir);
        postProcessing();

        return s;

    }
}
