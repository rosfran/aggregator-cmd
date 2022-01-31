package org.bendable.csv.aggregator.tasks;


import org.apache.logging.log4j.util.Strings;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
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
 * A return for this unit of execution is a List of Strings, so this means that, after processing all
 * the steps of this Tasks, the result will be a sequence of Strings.
 */
public class CSVReaderTask extends RecursiveTask<List<String>>
{

    private final Path path;

    public CSVReaderTask(Path path)
    {
        this.path = path;
    }

    @Override
    protected List<String> compute()
    {
        /* using here a synchronized List, because we will update it from a Parallel Stream (below) */
        List<String> list = Collections.synchronizedList(new ArrayList<String>());

        List<CSVReaderTask> tasks = Collections.synchronizedList(new ArrayList<CSVReaderTask>());

        if ( Files.isRegularFile( this.path ))
        {
            try
            {
                /* Code below does the following:
                1. Reads all lines of a DAT file;
                2. Strips out all empty lines;
                3. Creates a parallel Stream;
                4. Adds all lines to a synchronized List
                */
                Files.lines(this.path).filter(l -> Strings.isNotBlank(l)).parallel().forEach(l -> list.add(l));
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
        } else if ( Files.isDirectory( this.path ))
        {
            /*
            * this is the code responsible for creating subtasks, whenever it reachs a directory
            * This is the essence of using the ForkJoin strategy - to be able to divide the work into
            * multiple tasks, and hence being able to get the benefits of Work Stealing
            */
            CSVReaderTask task = new CSVReaderTask(this.path);
            task.fork();
            tasks.add(task);
        }

        /* this piece of code only will be useful if we were processing directories
        * For the purpose of this simulation, we won't use  */
        addResultsFromTasks(list, tasks);

        return list;
    }

    public void addResultsFromTasks(List<String> list, List<CSVReaderTask> tasks)
    {
        for (CSVReaderTask item : tasks)
        {
            list.addAll(item.join());
        }
    }

}

