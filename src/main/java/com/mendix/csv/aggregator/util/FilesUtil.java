package com.mendix.csv.aggregator.util;


import com.mendix.csv.aggregator.config.ApplicationConfig;
import com.mendix.csv.aggregator.tasks.CSVReaderChunkTask;
import com.mendix.csv.aggregator.tasks.CSVReaderTask;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
public class FilesUtil
{

    @Autowired
    private static ApplicationConfig config;

    Logger logger = LoggerFactory.getLogger(FilesUtil.class);


    static public void deleteIfExists( String fileName )
    {

        Path caminho = Paths.get(fileName);

        try
        {
            Files.deleteIfExists(caminho);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

    }

    static public void copyToBaseDir( String dir, String to )
    {

        Path caminho = Paths.get(dir);

        Path dest = Paths.get(to);

        Path file = null;

        if ( Files.exists(caminho)  && Files.isDirectory(caminho))
        {
            try
            {
                Stream<Path> s = Files.find(caminho,
                        1,
                        (path, basicFileAttributes) -> path.toFile().getName().matches(".*.csv")
                );

                Optional<Path> p = s.findFirst();

                if ( p.isPresent())
                {
                    file = p.get();
                }

                Files.move(file, dest, StandardCopyOption.REPLACE_EXISTING );
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
        }

    }

    public static Path createFile(String arqOut, String header)
    {
        Path caminho = Paths.get(arqOut);
        if (Files.notExists(caminho))
        {

            Set<PosixFilePermission> perms = PosixFilePermissions.fromString("rwxrwxrwx");
            FileAttribute attr = PosixFilePermissions.asFileAttribute(perms);
            try
            {
                Files.createFile(caminho, attr);
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }

            try
            {
                if ( header != null )
                {
                    Files.write(caminho, (header + "\n").getBytes(),
                            StandardOpenOption.CREATE,
                            StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
                }
            }
            catch (IOException ioException)
            {
                ioException.printStackTrace();
            }
        } else {
            try
            {

                    Files.write(caminho, ( "").getBytes(),
                            StandardOpenOption.CREATE,
                            StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);

            }
            catch (IOException ioException)
            {
                ioException.printStackTrace();
            }
        }


        return caminho;
    }


    static Set<String> readAllLinesFromDir(String dir) throws IOException
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
                ).parallel().forEach(f -> {

                    try
                    {
                        Files.lines(f).filter( l -> Strings.isNotBlank(l) ).forEach( l -> tSet.add(l) );

                        //tSet.addAll(cont.collect(Collectors.toList()));
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

    static Set<String> readAllLinesFromDirSequential(String dir) throws IOException
    {

        Path caminho = Paths.get(dir);

        List<String> content = Collections.synchronizedList( new ArrayList<String>() );

        Set<String> tSet =  new TreeSet<String>();


        /* check if the source directory exists */
        if ( Files.exists(caminho) && Files.isDirectory(caminho))
        {
            try
            {
                Stream<Path> s = Files.find(caminho,
                        1,
                        (path, basicFileAttributes) -> path.toFile().getName().matches(ApplicationConfig.DEFAULT_FILE_PATTERN)
                );

                s.forEach( f -> {

                    try
                    {
                        Files.lines(f).filter( l -> Strings.isNotBlank(l) ).forEach( l -> tSet.add(l) );
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

    static void writeAllLinesSortedToFile( Set<String> lines, String destFile )
    {
        Path dest = FilesUtil.createFile(destFile, null);

        writeToFile(dest, lines.stream().collect( Collectors.joining( "\n" ) ) );


    }

    static public void writeToFile( Path file, String cont )
    {
        try
        {
            Files.write(file, (cont).getBytes(),
                    StandardOpenOption.APPEND,
                    StandardOpenOption.WRITE);
        }
        catch (IOException ioException)
        {
            ioException.printStackTrace();
        }
    }


    static class CSVReaderTask1 extends ForkJoinTask<List<String>>
    {

        private final Path file;

        private List<String> st = null;

        public CSVReaderTask1(Path f) {
            this.file = f;
        }

        @Override
        public List<String> getRawResult()
        {
            return st;
        }

        @Override
        protected void setRawResult(List<String> value)
        {

        }

        @Override
        protected boolean exec()
        {
            try (final Stream<String> lines = Files.lines(file)) {
                st = lines.parallel() // use default parallel ExecutorService
                        .filter( l -> Strings.isNotBlank(l) ).collect(Collectors.toUnmodifiableList());
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }

            return true;
        }
    }

    static private void sleep()
    {
        try
        {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }

    static Set<String> readAllLinesFromDirForkJoin(String dir) throws IOException
    {

        Path caminho = Paths.get(dir);

        Set<String> tSet =  new TreeSet<String>();

        final int numberOfProcessors = Runtime.getRuntime().availableProcessors();

        final ForkJoinPool forkJoinPool = new ForkJoinPool(numberOfProcessors);

        /* check if the source directory exists */
        if ( Files.exists(caminho) && Files.isDirectory(caminho))
        {
            try
            {
                Stream<Path> s = Files.find(caminho,
                        1,
                        (path, basicFileAttributes) -> path.toFile().getName().matches(ApplicationConfig.DEFAULT_FILE_PATTERN)
                );


                List<CSVReaderTask> lsTasks = new ArrayList<CSVReaderTask>();
                s.forEach( f -> {

                        CSVReaderTask t =  new CSVReaderTask( f );

                        lsTasks.add( t );

                        forkJoinPool.execute( t );

                } );

                do
                {
                    System.out.printf("Parallelism: %d\n", forkJoinPool.getParallelism());
                    System.out.printf("Active Threads: %d\n", forkJoinPool.getActiveThreadCount());
                    System.out.printf("Task Count: %d\n", forkJoinPool.getQueuedTaskCount());
                    System.out.printf("Steal Count: %d\n", forkJoinPool.getStealCount());

                } while ( !isEveryTaskFinished(lsTasks) );

                //forkJoinPool.awaitQuiescence( 40, TimeUnit.SECONDS );
                forkJoinPool.shutdown();

                for ( CSVReaderTask task : lsTasks )
                {
                    List<String> partialList = task.join();
                    tSet.addAll( partialList );
                }

            }
            catch (IOException e)
            {
                e.printStackTrace();
            }


        }

        return tSet;

    }


    static Set<String> readAllLinesFromDirForkJoinOptimized(String dir) throws IOException
    {

        Path caminho = Paths.get(dir);

        Set<String> tSet =  new TreeSet<String>();

        final int numberOfProcessors = Runtime.getRuntime().availableProcessors();

        final ForkJoinPool forkJoinPool = new ForkJoinPool(numberOfProcessors);

        /* check if the source directory exists */
        if ( Files.exists(caminho) && Files.isDirectory(caminho))
        {
            try
            {
                List<Path> s = Files.find(caminho,
                        1,
                        (path, basicFileAttributes) -> path.toFile().getName().matches(ApplicationConfig.DEFAULT_FILE_PATTERN)
                ).collect( Collectors.toUnmodifiableList());

                List<CSVReaderChunkTask> lsTasks = new ArrayList<CSVReaderChunkTask>();

                CSVReaderChunkTask t1 =  new CSVReaderChunkTask( s.subList(0, s.size()/4 ) );

                lsTasks.add( t1 );

                forkJoinPool.execute( t1 );

                CSVReaderChunkTask t2 =  new CSVReaderChunkTask( s.subList(s.size()/4, s.size()/2  ) );

                lsTasks.add( t2 );

                forkJoinPool.execute( t2 );

                CSVReaderChunkTask t3 =  new CSVReaderChunkTask( s.subList(s.size()/2, (3*s.size())/4  ) );

                lsTasks.add( t3 );

                forkJoinPool.execute( t3 );

                CSVReaderChunkTask t4 =  new CSVReaderChunkTask( s.subList((3*s.size())/4, s.size() ) );

                lsTasks.add( t4 );

                forkJoinPool.execute( t4 );

                do
                {
                    System.out.printf("******************************************\n");
                    System.out.printf("Main: Parallelism: %d\n", forkJoinPool.getParallelism());
                    System.out.printf("Main: Active Threads: %d\n", forkJoinPool.getActiveThreadCount());
                    System.out.printf("Main: Task Count: %d\n", forkJoinPool.getQueuedTaskCount());
                    System.out.printf("Main: Steal Count: %d\n", forkJoinPool.getStealCount());
                    System.out.printf("******************************************\n");

                } while ( !isEveryTaskFinishedOpt(lsTasks) );

                //forkJoinPool.awaitQuiescence( 40, TimeUnit.SECONDS );
                forkJoinPool.shutdown();

                for ( CSVReaderChunkTask task : lsTasks )
                {
                    List<String> partialList = task.join();
                    tSet.addAll( partialList );
                }

            }
            catch (IOException e)
            {
                e.printStackTrace();
            }


        }

        return tSet;

    }

    private static boolean isEveryTaskFinished(List<CSVReaderTask> lsTasks)
    {
        for (CSVReaderTask task : lsTasks)
        {
            if ( !task.isCompletedNormally() )
            {
                return false;
            }
        }

        return true;
    }

    private static boolean isEveryTaskFinishedOpt(List<CSVReaderChunkTask> lsTasks)
    {
        for (CSVReaderChunkTask task : lsTasks)
        {
            if ( !task.isCompletedNormally() )
            {
                return false;
            }
        }

        return true;
    }

    static OptionalInt parseInt(final String s) {
        try {
            return OptionalInt.of(Integer.parseInt(s));
        } catch (final NumberFormatException e) {
            return OptionalInt.empty();
        }
    }


    static public void main( String[] args )
    {
        try
        {
            long start = Instant.now().toEpochMilli();
            //Set<String> l = FilesUtil.readAllLinesFromDir("src/main/resources/medium_example/");
            Set<String> l = FilesUtil.readAllLinesFromDirForkJoinOptimized("src/main/resources/medium_example/");
            //Set<String> l = FilesUtil.readAllLinesFromDirForkJoin("src/main/resources/medium_example/");

            Stream.of(l).forEach(System.out::println);

            System.out.println("size = "+l.size());

            FilesUtil.writeAllLinesSortedToFile(l, "src/main/resources/t.dat");
            long end = Instant.now().toEpochMilli();
            System.out.println(String.format("\tCompleted in %d milliseconds", (end - start)));
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

}
