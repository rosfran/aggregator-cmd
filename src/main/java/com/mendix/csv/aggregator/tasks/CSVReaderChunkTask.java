package com.mendix.csv.aggregator.tasks;


import org.apache.logging.log4j.util.Strings;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RecursiveTask;

public class CSVReaderChunkTask extends RecursiveTask<List<String>>
{

    private final List<Path> paths;

    public CSVReaderChunkTask(List<Path> paths)
    {
        this.paths = paths;
    }

    @Override
    protected List<String> compute()
    {
        List<String> list = new ArrayList<String>();

        List<CSVReaderChunkTask> tasks = new ArrayList<CSVReaderChunkTask>();

        for ( Path path : this.paths )
        {

            try
            {

                Files.lines(path).filter(l -> Strings.isNotBlank(l)).parallel().forEach(l -> list.add(l));
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }

        }

        addResultsFromTasks(list, tasks);

        return list;
    }

    private void addResultsFromTasks(List<String> list, List<CSVReaderChunkTask> tasks)
    {
        for (CSVReaderChunkTask item : tasks)
        {
            list.addAll(item.join());
        }
    }

}

