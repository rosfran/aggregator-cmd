package org.bendable.csv.aggregator.parallel.impl;

import org.bendable.csv.aggregator.config.ApplicationConfig;
import org.bendable.csv.aggregator.parallel.ParallelProcessingStrategy;
import org.bendable.csv.aggregator.parallel.ParallelStrategyType;
import org.apache.logging.log4j.util.Strings;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

/**
 * Merge and sort all files using Java Parallel Streams
 */
public class ParallelProcessingStreamImpl extends ParallelProcessingStrategy
{

    @Override
    public ParallelStrategyType getStrategyType()
    {
        return ParallelStrategyType.PARALLEL_STREAM;
    }

    @Override
    public Set<String> process(String dir) throws IOException
    {

        Path caminho = Paths.get(dir);

        Set<String> tSet = Collections.synchronizedSet( new TreeSet<String>() );

        /* check if the source directory exists */
        if ( Files.exists(caminho)  && Files.isDirectory(caminho))
        {
            try
            {
                Files.find(caminho,
                        1,
                        (path, basicFileAttributes) -> path.toFile().getName().matches(ApplicationConfig.DEFAULT_FILE_PATTERN)
                ).parallel().forEach( f -> {

                    try
                    {
                        Files.lines(f).parallel().filter( l -> Strings.isNotBlank(l) ).forEach(l -> tSet.add(l) );
                    }
                    catch (IOException e)
                    {
                        e.printStackTrace();
                    }

                } );

            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
        }

        return tSet;

    }
}
