package org.bendable.csv.aggregator.parallel;

import java.io.IOException;
import java.util.Set;

/**
 * This is an interface implementing the Strategy Pattern
 *
 * Since we have 4 different algorithms for sorting the DAT files, it is a
 * nice decision to use this design pattern
 */
public interface ParallelProcessingStrategyInterface
{

    public Set<String> processAllFiles(String dir) throws IOException;

    Set<String> process(String dir) throws IOException;

    void preProcessing();

    void postProcessing();

}
