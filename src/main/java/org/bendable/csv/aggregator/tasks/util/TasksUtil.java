package org.bendable.csv.aggregator.tasks.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinTask;
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
            if ( !task.isDone() )
            {
                return false;
            }
        }

        return true;
    }

    /**
     * Splits up an array in some smaller chunks.
     *
     * @param list
     * @param numChunks
     * @param <T>
     * @return
     */
    public static <T>List<List<T>> splitArrays( final List<T> list, final int numChunks )
    {
        final List<List<T>> parts = new ArrayList<List<T>>();
        final int chunkSize = list.size() / numChunks;
        int iLeft = list.size() % numChunks;
        int iRight = chunkSize;

        for( int i = 0, iT = list.size(); i < iT; i += iRight ) {
            if( iLeft > 0 ) {
                iLeft--;

                iRight = chunkSize + 1;
            }
            else
            {
                iRight = chunkSize;
            }

            parts.add( new ArrayList<T>( list.subList( i, Math.min( iT, i + iRight ) ) ) );
        }

        return parts;
    }

    /**
     * Put the current thread to sleep for 100 miliseconds
     */
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
