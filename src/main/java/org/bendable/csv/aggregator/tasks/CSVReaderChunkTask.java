package org.bendable.csv.aggregator.tasks;


import org.apache.logging.log4j.util.Strings;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.RecursiveTask;

/**
 * Implements a Task (a kind of Thread), using the Fork Join strategy.
 * A Task is a higher level abstraction for a Thread.
 * A Task offers some concept that allows a Thread to deal with controlling the execution flow like, for
 * example, returning results after finishing processing this unit of execution.
 *
 *  Inside each task, there is a call to a Parallized Java Stream. This stream reads all lines of a
 *  given file, filters only the non-empty lines, and fills up a List with all lines from file
 *  (in parallel)
 *
 *  The difference from @CSVReaderTask class is that this class implements another strategy on parallelizing
 *  the work of sorting files - instead of just reading the file in parallel, here we antecipate the work
 *  of sorting the file. But we do this sorting each file chunk in parallel, and at the final phase, we
 *  join all the results.
 *
 * A return for this unit of execution is a List of Strings, so this means that, after processing all
 * the steps of this Tasks, the result will be a sequence of Strings.
 */
public class CSVReaderChunkTask extends RecursiveTask<List<String>>
{

    private final List<Path> paths;

    public CSVReaderChunkTask(List<Path> paths)
    {
        this.paths = paths;
    }

    @Override
    public List<String> compute()
    {
        List<String> list = Collections.synchronizedList( new ArrayList<String>());

        for ( Path path : this.paths )
        {

            try
            {
                 /* Code below does the following:
                1. Reads all lines of a DAT file;
                2. Creates a parallel Stream;
                3. Strips out all empty lines;
                3. Sorts all lines;
                4. Adds all lines to a synchronized List
                */
                Files.lines(path).parallel().filter(l -> Strings.isNotBlank(l)).sorted().forEach(l -> list.add(l));
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }

        }

        return list;
    }


}

