package org.bendable.csv.aggregator.service;


import org.bendable.csv.aggregator.config.ApplicationConfig;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;


@Component
public class FilesService
{

    @Autowired
    private ApplicationConfig config;

    //Logger logger = LoggerFactory.getLogger(FilesService.class);

    public List<File> getAllFiles()
    {
        List<File> files = getAllSmallFiles();

        files.addAll(getAllMediumFiles());

        return files;
    }

    public List<File> getAllSmallFiles()
    {
        List<File> files = new ArrayList<>();

        try (Stream<Path> paths = Files.walk(Paths.get(config.getSmallFilesDir()))) {
            paths.forEach( e -> files.add(e.toFile()));
        } catch (IOException e) {
            e.printStackTrace();
        }

        return files;
    }

    public List<File> getAllMediumFiles()
    {
        List<File> files = new ArrayList<>();

        try (Stream<Path> paths = Files.walk(Paths.get(config.getMediumFilesDir())))
        {
            //paths.forEach( e -> files.add(e.toFile()));
            files = paths.map( e -> e.toFile() ).collect(Collectors.toList());
        } catch (IOException e) {
            e.printStackTrace();
        }

        return files;
    }

    public static Path criarArquivo(String arqOut, String cabecalho)
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
                Files.write(caminho, (cabecalho + "\n").getBytes(),
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

}
