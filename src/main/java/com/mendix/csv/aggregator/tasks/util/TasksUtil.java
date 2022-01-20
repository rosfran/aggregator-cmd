package com.mendix.csv.aggregator.tasks.util;

import com.mendix.csv.aggregator.tasks.CSVReaderChunkTask;
import com.mendix.csv.aggregator.tasks.CSVReaderTask;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;
import java.util.concurrent.TimeUnit;

/**
 * Utilities needed when running Tasks (RecursiveTasks, ForkJoin tasks, etc.)
 *
 * This is the place for all functions used to give support for concurrency.
 */
public class TasksUtil
{


    /**
     * Check if all Tasks (asynchronous) are finished (see if all tasks completed its cumputation unit).
     *
     * @param lsTasks List of tasks, mainly when using on a common pool from a ForkJoin pool
     *
     * @return <code>True</code>, if all tasks are completed.
     */
    static public boolean isEveryTaskFinished(List<ForkJoinTask> lsTasks)
    {
        for (ForkJoinTask task : lsTasks)
        {
            if ( !task.isCompletedNormally() )
            {
                return false;
            }
        }

        return true;
    }


    static public List<Path>[] splitArrays(ArrayList<Path> list, int chunkSize)
    {

        int numChunks = (list.size() / chunkSize) + 1;

        final List<Path>[] chunks = new ArrayList[numChunks];

        for(int i = 0; i < numChunks; i++) {
            int offset = i + 1;
            int from = Math.max(((offset - 1) * chunkSize), 0);
            int to = Math.min((offset * chunkSize), list.size());
            chunks[i] = new ArrayList(list.subList(from, to));
        }

        return chunks;
    }


    static public void sleep()
    {
        try
        {
            TimeUnit.MILLISECONDS.sleep(100);
        } catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }



}
