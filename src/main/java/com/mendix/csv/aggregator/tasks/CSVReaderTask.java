package com.mendix.csv.aggregator.tasks;


import com.mendix.csv.aggregator.config.ApplicationConfig;
import org.apache.logging.log4j.util.Strings;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.RecursiveTask;
import java.util.stream.Stream;

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
        List<String> list = new ArrayList<String>();

        List<CSVReaderTask> tasks = new ArrayList<CSVReaderTask>();

        if ( Files.isRegularFile( this.path ))
        {
            try
            {
                Files.lines(this.path).filter(l -> Strings.isNotBlank(l)).parallel().forEach(l -> list.add(l));
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

        if (tasks.size() > 50)
        {
            System.out.printf("%s: %d tasks ran.\n", this.path.toAbsolutePath(), tasks.size());
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

