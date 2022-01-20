package com.mendix.csv.aggregator.parallel;

import java.io.IOException;
import java.util.Set;

public interface ParallelProcessingStrategyInterface
{

    public Set<String> processAllFiles(String dir) throws IOException;

    Set<String> process(String dir) throws IOException;

    void preProcessing();

    void postProcessing();

}
