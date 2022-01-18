package com.mendix.csv.aggregator.util;


import com.mendix.csv.aggregator.config.ApplicationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Instant;
import java.util.*;
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
                Stream<Path> sFiles = Files.find(caminho,
                        1,
                        (path, basicFileAttributes) -> path.toFile().getName().matches(ApplicationConfig.DEFAULT_FILE_PATTERN)
                );

                sFiles.parallel().forEach( f -> {

                    try
                    {
                        Files.lines(f).forEach( l -> tSet.add(l) );

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
                        Stream<String> cont = Files.lines(f);

                        tSet.addAll(cont.collect(Collectors.toList()));
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

    static public void main( String[] args )
    {
        try
        {
            long start = Instant.now().toEpochMilli();
            Set<String> l = FilesUtil.readAllLinesFromDir("src/main/resources/medium_example/");

            Stream.of(l).forEach(System.out::println);

            System.out.println("size = "+l.size());

            FilesUtil.writeAllLinesSortedToFile(l, "src/main/resources/medium_example/t.dat");
            long end = Instant.now().toEpochMilli();
            System.out.println(String.format("\tCompleted in %d milliseconds", (end-start)));
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

}
