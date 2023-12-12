package eps.scp;

import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class Indexing
{
    public static final boolean Verbose = false;

    public static void main(String[] args)
    {
        InvertedIndex hash;

        if (args.length <2 || args.length>2)
            System.err.println("Erro in Parameters. Usage: Indexing <SourceDirectory> [<Index_Directory>]");
        if (args.length < 2)
            hash = new InvertedIndex(args[0]);
        else
            hash = new InvertedIndex(args[0], args[1]);

        Instant start = Instant.now();

        hash.buidIndex();
        if (false) hash.printIndex();
        hash.saveIndex();
        Map<String, HashSet<Location>> old_hash = new TreeMap<String, HashSet <Location>>(hash.getHash());
        Map<Location, String> old_indexFilesLines = hash.getIndexFilesLines();
        Map<Integer, String> old_files = hash.getFiles();
        hash.loadIndex();
        if (false) hash.printIndex();

        // Comprobar que el Indice Invertido cargado sea igual al salvado.
        try {
            assertEquals(old_hash, hash.getHash());
            assertEquals(old_indexFilesLines, hash.getIndexFilesLines());
            assertEquals(old_files, hash.getFiles());
        }catch (AssertionError e){
            System.out.println(hash.ANSI_RED+ e.getMessage() + " "+ hash.ANSI_RESET);
        }

        Instant finish = Instant.now();
        long timeElapsed = Duration.between(start, finish).toMillis();  //in millis
        System.out.printf("[All Stages] Total execution time: %.3f secs.\n", timeElapsed/1000.0);
    }
}
