package com.mendix.csv.aggregator.tasks;


import com.mendix.csv.aggregator.config.ApplicationConfig;
import org.apache.logging.log4j.util.Strings;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.RecursiveTask;

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
        List<String> list = Collections.synchronizedList(new ArrayList<String>());

        List<CSVReaderTask> tasks = Collections.synchronizedList(new ArrayList<CSVReaderTask>());

        if ( Files.isRegularFile( this.path ))
        {
            try
            {
                Files.lines(this.path).parallel().filter(l -> Strings.isNotBlank(l)).parallel().forEach(l -> list.add(l));
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
        } else if ( Files.isDirectory( this.path ))
        {
            CSVReaderTask task = new CSVReaderTask(this.path);
            task.fork();
            tasks.add(task);
        }

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

