package com.mendix.csv.aggregator.parallel.impl;

import com.mendix.csv.aggregator.config.ApplicationConfig;
import com.mendix.csv.aggregator.parallel.ParallelProcessingStrategy;
import org.apache.logging.log4j.util.Strings;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

/**
 * Reads, merges and sorts all files using Java Streams with no parallelization
 */
public class ParallelProcessingNoParallelImpl extends ParallelProcessingStrategy
{

    @Override
    public Set<String> process(String dir) throws IOException
    {

        Path caminho = Paths.get(dir);

        Set<String> tSet =  new TreeSet<String>();

        /* check if the source directory exists */
        if ( Files.exists(caminho) && Files.isDirectory(caminho))
        {
            try
            {
                Stream<Path> allFilesFromDir = Files.find(caminho,
                        1,
                        (path, basicFileAttributes) -> path.toFile().getName().matches(ApplicationConfig.DEFAULT_FILE_PATTERN)
                );

                allFilesFromDir.forEach( f -> {

                    try
                    {
                        Files.lines(f).sequential().filter( l -> Strings.isNotBlank(l) ).forEach( l -> tSet.add(l) );
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
