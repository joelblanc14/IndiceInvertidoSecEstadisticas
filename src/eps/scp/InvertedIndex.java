    /* ---------------------------------------------------------------
Práctica 3.
Grau Enginyeria Informàtica
Joel Blanc Iniesta
Miquel Jordan Rodriguez
--------------------------------------------------------------- */

package eps.scp;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.jupiter.api.Assertions.*;
import org.apache.commons.io.FileUtils;

public class InvertedIndex
{
    // Constantes
    public final String ANSI_RED = "\u001B[31m";
    public final String ANSI_GREEN = "\u001B[32m";
    public final String ANSI_BLUE = "\u001B[34m";
    public final String ANSI_GREEN_YELLOW_UNDER = "\u001B[32;40;4m";
    public final String ANSI_RESET = "\u001B[0m";
    private final int DIndexMaxNumberOfFiles = 200;   // Número máximo de ficheros para salvar el índice invertido.
    private final int DIndexMinNumberOfFiles = 2;     // Número mínimo de ficheros para salvar el índice invertido.
    private final int DKeysByFileIndex = 1000;
    private final String DIndexFilePrefix = "IndexFile";   // Prefijo de los ficheros de Índice Invertido.
    private final String DFileLinesName = "FilesLinesContent";  // Nombre fichero donde guardar las lineas de los ficheros indexados
    private final String DFilesIdsName = "FilesIds";  // Nombre fichero donde guardar las identificadores de los ficheros indexados
    private final String DDefaultIndexDir = "./Index/";   // Directorio por defecto donde se guarda el indice invertido.

    private final float DMatchingPercentage = 0.80f;  // Porcentaje mínimo de matching entre el texto original y la consulta (80%)
    private final float DNearlyMatchingPercentage = 0.60f;  // Porcentaje mínimo de matching entre el texto original y la consulta (80%)

    // Members
    private String InputDirPath = null;       // Contiene la ruta del directorio que contiene los ficheros a Indexar.
    private String IndexDirPath = null;       // Contiene la ruta del directorio donde guardar el indice.
    private RandomAccessFile randomInputFile;  // Fichero random para acceder al texto original con mayor porcentaje de matching.

    // Lista ne donde se guardas los ficheros a procesar
    private List<File> FilesList = new ArrayList<>();

    // Hash Map convertir de ids ficheros a su ruta
    private Map<Integer,String> Files = new HashMap<Integer,String>();

    // Hash Map para acceder a las líneas de todos los ficheros del indice.
    private Map<Location, String> IndexFilesLines = new TreeMap<Location, String>();

    // Hash Map que implementa el Índice Invertido: key=word, value=Locations(Listof(file,line)).
    private Map<String, HashSet <Location>> Hash =  new TreeMap<String, HashSet <Location>>();

    // Estadisticas para verificar la correcta contrucción del indice invertido.
    private Statistics GlobalStatistics = new Statistics("=");
    private long TotalLocations = 0;
    private long TotalWords = 0;
    private long TotalLines = 0;
    private long TotalKeysFound = 0;
    private int TotalProcessedFiles = 0;


    /*----------------------------------------Nuevos Parametros-------------------------------------------------------*/
    //Uso del Phaser en la espera de los hilos secundarios en la fase de Build
    private final Phaser phaser = new Phaser(1); // Inicializado con 1 por el hilo principal

    //Uso de CountDownLatch en la espera de los hilos secundarios de la fase Load
    private CountDownLatch latch;

    private final Lock lock = new ReentrantLock(); //Cambiar por otro metodo de sincronización

    private final Condition condition = lock.newCondition();

    private int numThreads = 3;

    //Uso del Semaphore en la espera de los hilos secundarios en la fase de Save
    private final Semaphore semaphore = new Semaphore(0);
    private int M = 1000;

    CyclicBarrier cyclicBarrier; // Se deberà inicializar cuando sepamos el numero de hilos total

    private int threadsFinishedCount = 0;

    // Getters
    public Map<Integer, String> getFiles() { return Files; }
    public Map<Location, String> getIndexFilesLines() { return IndexFilesLines; }
    public Map<String, HashSet<Location>> getHash() { return Hash; }
    public void setIndexDirPath(String indexDirPath) {
        IndexDirPath = indexDirPath;
    }
    public long getTotalWords(Map hash){ return(hash.size()); }
    public long getTotalLocations() { return TotalLocations; }
    public long getTotalWords() { return TotalWords; }
    public long getTotalLines() { return TotalLines; }
    public int getTotalProcessedFiles() { return TotalProcessedFiles; }

    public long getTotalKeysFound() {
        return TotalKeysFound;
    }

    public Statistics getGlobalStatistics() {return GlobalStatistics;}

    public void setTotalKeysFound(long totalKeys) {
        TotalKeysFound = totalKeys;
    }


    // Constructores
    public InvertedIndex() {
    }
    public InvertedIndex(String InputPath, int M) {
        this.InputDirPath = InputPath;
        this.IndexDirPath = DDefaultIndexDir;
        this.M = M;
    }

    public InvertedIndex(String inputDir, String indexDir, int M) {
        this.InputDirPath = inputDir;
        this.IndexDirPath = indexDir;
        this.M = M;
    }



    // Método para la construcción del indice invertido.
    //  1. Busca los ficheros de texto recursivamente en el directorio de entrada.
    //  2. Construye el indice procesando las palabras del fichero.
    public void buidIndex() {
        Instant start = Instant.now();
        initializeCounters();
        searchDirectoryFiles(InputDirPath);

        processFilesWithThreads();

        Instant finish = Instant.now();
        long timeElapsed = Duration.between(start, finish).toMillis();  // in millis
        System.out.printf("[Build Index with %d files] Total execution time: %.3f secs.\n", FilesList.size(), timeElapsed/1000.0);

        // Comprobar que el resultado sea correcto.
        try {
            assertEquals(getTotalWords(Hash), getTotalKeysFound());
            assertEquals(getTotalLocations(Hash), getTotalLocations());
            assertEquals(getTotalFiles(Files), getTotalProcessedFiles());
            assertEquals(getTotalLines(IndexFilesLines), getTotalLines());
        } catch (AssertionError e) {
            System.out.println(ANSI_RED + e.getMessage() + " " + ANSI_RESET);
        }
    }

    private void initializeCounters() {
        TotalProcessedFiles = 0;
        TotalLocations = 0;
        TotalLines = 0;
        TotalKeysFound = 0;
        TotalWords = 0;
    }

    private void processFilesWithThreads() {
        int fileId = 0;
        List<Thread> threads = new ArrayList<>();

        cyclicBarrier = new CyclicBarrier(FilesList.size(), () -> {
            GlobalStatistics.print("GlobalStatistics", ANSI_GREEN); // Cuando termina imprime
        });

        for(File file: FilesList) {
            Files.put(fileId, file.getAbsolutePath());
            int finalfileid= fileId;
            threads.add(Thread.startVirtualThread(() ->
                    addFileWords2Index(finalfileid, file)
            ));
            phaser.register();
            fileId++;
        }

        phaser.arriveAndAwaitAdvance();
    }

    // Calcula el número de ubicaciones diferentes de las palabras en los ficheros.
    // Si una palabra aparece varias veces en un linea de texto, solo se cuenta una vez.
    public long getTotalLocations(Map hash)
    {
        long locations=0;
        Set<String> keySet = hash.keySet();

        Iterator keyIterator = keySet.iterator();
        while (keyIterator.hasNext() ) {
            String word = (String) keyIterator.next();
            locations += Hash.get(word).size();
        }
        return(locations);
    }
    public long getTotalFiles(Map files){
        return(files.size());
    }
    public long getTotalLines(Map filesLines){
        return(filesLines.size());
    }

    // Procesamiento recursivo del directorio para buscar los ficheros de texto.
    // Cada fichero encontrado se guarda en la lista fileList
    public void searchDirectoryFiles(String dirpath)
    {
        File file=new File(dirpath);
        File content[] = file.listFiles();
        if (content != null) {
            for (int i = 0; i < content.length; i++) {
                if (content[i].isDirectory()) {
                    // Si es un directorio, procesarlo recursivamente.
                    searchDirectoryFiles(content[i].getAbsolutePath());
                }
                else {
                    // Si es un fichero de texto, añadirlo a la lista para su posterior procesamiento.
                    if (checkFile(content[i].getName())){
                        FilesList.add(content[i]);
                    }
                }
            }
        }
        else
            System.err.printf("Directorio %s no existe.\n",file.getAbsolutePath());
    }

    // Método para incorporar las palabras de un fichero de texto al indice invertido todos los ficheros
    // de la lista.
    //  1. Se lee cada un de las líneas de texto del fichero, se eliminan los caracteres especiales y se
    //  divide el contenido de la linea en palabras.
    //  2. Para cada palabra, se accede a su entrada en la tabla de hash del indice invertido y se comprueba
    //  si existe la clave (palabra). Si no existe la clave, se crea una nueva lista, añadiendo la localización
    //  de la palabra (id_fichero + línea), y está se añade al hashmap del indice asociandao con la palabra
    //  encontrada como clave. Si existe la clave, se añade una nueva entrada en su lista de localizaciones.
    // También se genera una hasp con todas las líneas de los ficheros indexados mediante su localización
    // (idFile,Linea)
    public void addFileWords2Index(int fileId, File file) {

        Statistics FileStatistics = new Statistics("-");
        synchronized (this) {
            System.out.printf("Processing %3dth file %s (Path: %s)\n", fileId, file.getName(), file.getAbsolutePath());
            TotalProcessedFiles++;
            FileStatistics.incProcessingFiles();
        }

        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            int lineNumber = 0;

            while ((line = br.readLine()) != null) {
                lineNumber++;
                synchronized (this) {
                    TotalLines++;
                }
                FileStatistics.incProcessedLines();
                Location newLocation = new Location(fileId, lineNumber);
                addIndexFilesLine(newLocation, line);

                line = Normalizer.normalize(line, Normalizer.Form.NFD);
                line = line.replaceAll("[\\p{InCombiningDiacriticalMarks}]", "");
                String filter_line = line.replaceAll("[^a-zA-Z0-9áÁéÉíÍóÓúÚäÄëËïÏöÖüÜñÑ ]", "");
                String[] words = filter_line.split("\\W+");

                for (String word : words) {

                    if (FileStatistics.getProcessedWords() == M) {
                        synchronized (this) {
                            setMostPopularWord(FileStatistics);
                            FileStatistics.print(file.getName(), ANSI_BLUE); // Imprime estadisticas parciales
                        }
                        synchronized (GlobalStatistics) {
                            FileStatistics.decProcessingFiles();
                            GlobalStatistics.addStatistics(FileStatistics); // Actualiza estadisticas globales
                        }

                        synchronized (this) {
                            FileStatistics = new Statistics("_");
                            FileStatistics.incProcessingFiles();     // Resetea estadísticas parciales
                        }

                        cyclicBarrier.await();
                    }

                    word = word.toLowerCase();
                    synchronized (this) {
                        HashSet<Location> locations = Hash.get(word);
                        if (locations == null) {
                            locations = new HashSet<>();
                            if (!Hash.containsKey(word)){
                                FileStatistics.incKeysFound();
                                TotalKeysFound++;
                            }
                            Hash.put(word, locations);
                        }
                        TotalWords++;   // Modificado!!
                        FileStatistics.incProcessedWords();
                        int oldLocSize = locations.size();
                        locations.add(newLocation);
                        if (locations.size() > oldLocSize) {
                            TotalLocations++;
                            FileStatistics.incProcessedLocations();
                        }
                    }
                }
            }

            synchronized (this) {
                FileStatistics.incProcessedFiles();
                FileStatistics.decProcessingFiles();
                setMostPopularWord(FileStatistics);
                FileStatistics.print(file.getName(), ANSI_RED);
            }
            synchronized (GlobalStatistics) {
                GlobalStatistics.addStatistics(FileStatistics);
            }

            threadsFinishedCount++;
            cyclicBarrier.await(); // Para que el último hilo se pare en la barrera y así esta finalice

            while (threadsFinishedCount < FilesList.size()) {
                cyclicBarrier.await();
            }

        } catch (FileNotFoundException e) {
            System.err.printf("Fichero %s no encontrado.\n", file.getAbsolutePath());
            e.printStackTrace();
        } catch (IOException e) {
            System.err.printf("Error lectura fichero %s.\n", file.getAbsolutePath());
            e.printStackTrace();
        } catch (BrokenBarrierException | InterruptedException e) {
            throw new RuntimeException(e);
        }

        phaser.arriveAndDeregister();
    }

    public synchronized void setMostPopularWord(Statistics stats) {
        String maxWord = Collections.max(Hash.entrySet(), (entry1, entry2) -> entry1.getValue().size() - entry2.getValue().size()).getKey();
        stats.setMostPopularWord(maxWord);
        stats.setMostPopularWordLocations(Hash.get(maxWord).size());
    }

    // Verificar si la extensión del fichero coincide con la extensiones buscadas (txt)
    private boolean checkFile (String name)
    {
        if (name.endsWith("txt")) {
            return true;
        }
        return false;
    }

    // Método para imprimir por pantalla el índice invertido.
    public void printIndex()
    {
        Set<String> keySet = Hash.keySet();
        Iterator keyIterator = keySet.iterator();
        while (keyIterator.hasNext() ) {
            String word = (String) keyIterator.next();
            System.out.print(word + "\t");
            HashSet<Location> locations = Hash.get(word);
            for(Location loc: locations){
                System.out.printf("(%d,%d) ", loc.getFileId(), loc.getLine());
            }
            System.out.println();
        }
    }


    public void saveIndex()
    {
        saveIndex(IndexDirPath);
    }

    // Método para salvar el indice invertido en el directorio pasado como parámetro.
    // Se salva:
    //  + Indice Invertido (Hash)
    //  + Hash map de conversión de idFichero->RutaNombreFichero (Files)
    //  + Hash de acceso indexado a las lineas de los ficheros (IndexFilesLines)
    public void saveIndex(String indexDirectory) {
        Instant start = Instant.now();

        resetDirectory(indexDirectory);

        List<Thread> threads = new ArrayList<>();

        threads.add(Thread.startVirtualThread(() -> {
            saveInvertedIndex(indexDirectory);
            signalThreadFinished();
        }));

        threads.add(Thread.startVirtualThread(() -> {
            saveFilesIds(indexDirectory);
            signalThreadFinished();
        }));

        threads.add(Thread.startVirtualThread(() -> {
            saveFilesLines(indexDirectory);
            signalThreadFinished();
        }));

        waitForThreadsToFinishWithCondition();

        Instant finish = Instant.now();
        long timeElapsed = Duration.between(start, finish).toMillis();  // in millis
        System.out.printf("[Save Index with %d keys] Total execution time: %.3f secs.\n", Hash.size(), timeElapsed / 1000.0);
    }

    private void waitForThreadsToFinishWithCondition() {
        lock.lock();
        try {
            while (numThreads > 0) {
                condition.await();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            lock.unlock();
        }
    }

    private void signalThreadFinished() {
        lock.lock();
        try {
            numThreads--;
            if (numThreads == 0) {
                condition.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    public void resetDirectory(String outputDirectory)
    {
        File path = new File(outputDirectory);
        if (!path.exists())
            path.mkdir();
        else if (path.isDirectory()) {
            try {
                FileUtils.cleanDirectory(path);
            } catch (IOException e) {
                System.err.printf("Error borrando contenido directorio indice %s.\n",path.getAbsolutePath());
                e.printStackTrace();
            }
        }
    }

    // Método para salvar en disco el índice invertido.
    // Recibe la ruta del directorio en donde se van a guardar los ficheros del indice.
    public void saveInvertedIndex(String outputDirectory) {
        int numberOfFiles;
        Set<String> keySet = new HashSet<>(Hash.keySet());

        numberOfFiles = keySet.size() / DKeysByFileIndex;
        if (numberOfFiles > DIndexMaxNumberOfFiles) {
            numberOfFiles = DIndexMaxNumberOfFiles;
        }
        if (numberOfFiles < DIndexMinNumberOfFiles) {
            numberOfFiles = DIndexMinNumberOfFiles;
        }

        int fileId = 1;
        List<Thread> threads = new ArrayList<>();
        Iterator<String> keyIterator = keySet.iterator();
        int keysPerFile = keySet.size() / numberOfFiles;


        while (keyIterator.hasNext()) {
            Set<String> keysToSave = new HashSet<>();
            for (int i = 0; i < keysPerFile && keyIterator.hasNext(); i++) {
                keysToSave.add(keyIterator.next());
            }

            saveInvertedIndexTask task = new saveInvertedIndexTask(keysToSave, outputDirectory, fileId);
            threads.add(Thread.startVirtualThread(task));

            fileId++;
        }

        try {
            semaphore.acquire(numberOfFiles);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    // Método para salvar una clave y sus ubicaciones en un fichero.
    public void saveIndexKey(String key, BufferedWriter bw)
    {
        try {
            HashSet<Location> locations = Hash.get(key);
            // Creamos un string con todos los offsets separados por una coma.
            //String joined1 = StringUtils.join(locations, ";");
            String joined = String.join(";",locations.toString());
            bw.write(key+"\t");
            bw.write(joined.substring(1,joined.length()-1)+"\n");
        } catch (IOException e) {
            System.err.println("Error writing Index file");
            e.printStackTrace();
            System.exit(-1);
        }
    }


    public void saveFilesIds(String outputDirectory)
    {
        try {
            //File IdsFile = new File(outputDirectory +"/"+ DFilesIdsName);
            FileWriter fw = new FileWriter(outputDirectory + "/" + DFilesIdsName);
            BufferedWriter bw = new BufferedWriter(fw);
            Set<Entry<Integer,String>> keySet = Files.entrySet();
            Iterator keyIterator = keySet.iterator();

            while (keyIterator.hasNext() )
            {
                Entry<Integer,String> entry = (Entry<Integer,String>) keyIterator.next();
                bw.write(entry.getKey() + "\t" + entry.getValue() + "\n");
            }
            bw.close(); // Cerramos el fichero.

        } catch (IOException e) {
            System.err.println("Error creating FilesIds file: " + outputDirectory + DFilesIdsName + "\n");
            e.printStackTrace();
            System.exit(-1);
        }
    }

    public void saveFilesLines(String outputDirectory)
    {
        try {
            File KeyFile = new File(outputDirectory + "/" + DFileLinesName);
            FileWriter fw = new FileWriter(KeyFile);
            BufferedWriter bw = new BufferedWriter(fw);
            Set<Entry<Location, String>> keySet = IndexFilesLines.entrySet();
            Iterator keyIterator = keySet.iterator();

            while (keyIterator.hasNext() )
            {
                Entry<Location, String> entry = (Entry<Location, String>) keyIterator.next();
                bw.write(entry.getKey() + "\t" + entry.getValue() + "\n");
            }
            bw.close(); // Cerramos el fichero.
        } catch (IOException e) {
            System.err.println("Error creating FilesLines contents file: " + outputDirectory + DFileLinesName + "\n");
            e.printStackTrace();
            System.exit(-1);
        }
    }

    public void loadIndex()
    {
        loadIndex(IndexDirPath);
    }

    // Método para carga el indice invertido del directorio pasado como parámetro.
    // Se carga:
    //  + Indice Invertido (Hash)
    //  + Hash map de conversión de idFichero->RutaNombreFichero (Files)
    //  + Hash de acceso indexado a las lineas de los ficheros (IndexFilesLines)
    public void loadIndex(String indexDirectory) {
        Instant start = Instant.now();

        resetIndex();
        latch = new CountDownLatch(3);
        List<Thread> threads = new ArrayList<>();

        threads.add(Thread.startVirtualThread(() -> {
            loadInvertedIndex(indexDirectory);
            latch.countDown();
        }));

        threads.add(Thread.startVirtualThread(() -> {
            loadFilesIds(indexDirectory);
            latch.countDown();
        }));

        threads.add(Thread.startVirtualThread(() -> {
            loadFilesLines(indexDirectory);
            latch.countDown();
        }));

        // Espera a que todos los hilos alcancen la barrera final

        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        Instant finish = Instant.now();
        long timeElapsed = Duration.between(start, finish).toMillis();  // en milisegundos
        System.out.printf("[Load Index with %d keys] Total execution time: %.3f secs.\n", Hash.size(), timeElapsed / 1000.0);
    }

    public void resetIndex()
    {
        Hash.clear();
        Files.clear();
        IndexFilesLines.clear();
    }

    // Método para cargar en memoria el índice invertido desde su copia en disco.
    public void loadInvertedIndex(String inputDirectory)
    {
        File folder = new File(inputDirectory);
        File[] listOfFiles = folder.listFiles((d, name) -> name.startsWith(DIndexFilePrefix));

        List<Thread> threads = new ArrayList<>();
        Phaser phaser = new Phaser(1);

        // Recorremos todos los ficheros del directorio de Indice y los procesamos.
        for (File file : listOfFiles) {
            if (file.isFile()) {
                phaser.register(); // Registrar el hilo en el Phaser
                threads.add(Thread.startVirtualThread(() -> {
                    new loadIndexTask(file).run();
                    phaser.arriveAndDeregister(); // Indica que este hilo ha terminado
                }));
            }
        }

        phaser.arriveAndAwaitAdvance(); // Esperar a que todos los hilos lleguen al punto de la barrera
    }

    public void loadFilesIds(String inputDirectory)
    {
        try {
            FileReader input = new FileReader(inputDirectory + "/" + DFilesIdsName);
            BufferedReader bufRead = new BufferedReader(input);
            String keyLine = null;
            try {

                // Leemos fichero línea a linea (clave a clave)
                while ( (keyLine = bufRead.readLine()) != null)
                {
                    // Descomponemos la línea leída en su clave (File Id) y la ruta del fichero.
                    String[] fields = keyLine.split("\t");
                    int fileId = Integer.parseInt(fields[0]);
                    fields[0]="";
                    String filePath = String.join("", fields);
                    Files.put(fileId, filePath);
                }
                bufRead.close();

            } catch (IOException e) {
                System.err.println("Error reading Files Ids");
                e.printStackTrace();
            }
        } catch (FileNotFoundException e) {
            System.err.println("Error opening Files Ids file");
            e.printStackTrace();
        }
    }

    public void loadFilesLines(String inputDirectory)
    {
        try {
            FileReader input = new FileReader(inputDirectory + "/" + DFileLinesName);
            BufferedReader bufRead = new BufferedReader(input);
            String keyLine = null;
            try
            {
                // Leemos fichero línea a linea (clave a clave)
                while ( (keyLine = bufRead.readLine()) != null)
                {
                    // Descomponemos la línea leída en su clave (Location) y la linea de texto correspondiente
                    String[] fields = keyLine.split("\t");
                    String[] location = fields[0].substring(1, fields[0].length()-1).split(",");
                    int fileId = Integer.parseInt(location[0]);
                    int line = Integer.parseInt(location[1]);
                    fields[0]="";
                    String textLine = String.join("", fields);
                    IndexFilesLines.put(new Location(fileId,line),textLine);
                }
                bufRead.close();

            } catch (IOException e) {
                System.err.println("Error reading Files Ids");
                e.printStackTrace();
            }
        } catch (FileNotFoundException e) {
            System.err.println("Error opening Files Ids file");
            e.printStackTrace();
        }
    }


    // Implentar una consulta sobre el indice invertido:
    //  1. Descompone consulta en palabras.
    //  2. Optiene las localizaciones de cada palabra en el indice invertido.
    //  3. Agrupa palabras segun su localizacion en una hash de coincidencias.
    //  4. Recorremos la tabla de coincidencia y mostramos las coincidencias en función del porcentaje de matching.
    public void query(String queryString)
    {
        String queryResult=null;
        Map<Location, Integer> queryMatchings = new TreeMap<Location, Integer>();
        Instant start = Instant.now();

        System.out.println ("Searching for query: "+queryString);

        // Pre-procesamiento query
        queryString = Normalizer.normalize(queryString, Normalizer.Form.NFD);
        queryString = queryString.replaceAll("[\\p{InCombiningDiacriticalMarks}]", "");
        String filter_line = queryString.replaceAll("[^a-zA-Z0-9áÁéÉíÍóÓúÚäÄëËïÏöÖüÜñÑ ]","");
        // Dividimos la línea en palabras.
        String[] words = filter_line.split("\\W+");
        int querySize = words.length;

        // Procesar cada palabra de la query
        for(String word: words)
        {
            word = word.toLowerCase();
            if (Indexing.Verbose) System.out.printf("Word %s matching: ",word);
            // Procesar las distintas localizaciones de esta palabra
            if (Hash.get(word)==null)
                continue;
            for(Location loc: Hash.get(word))
            {
                // Si no existe esta localización en la tabla de coincidencias, entonces la añadimos con valor inicial a 1.
                Integer value = queryMatchings.putIfAbsent(loc, 1);
                if (value != null) {
                    // Si existe, incrementamos el número de coincidencias para esta localización.
                    queryMatchings.put(loc, value+1);
                }
                if (Indexing.Verbose) System.out.printf("%s,",loc);
            }
            if (Indexing.Verbose) System.out.println(".");
        }

        if (queryMatchings.size()==0)
            System.out.printf(ANSI_RED+"Not matchings found.\n"+ANSI_RESET);

        // Recorremos la tabla de coincidencia y mostramos las líneas en donde aparezca más de un % de las palabras de la query.
        for(Map.Entry<Location, Integer> matching : queryMatchings.entrySet())
        {
            Location location = matching.getKey();
            if ((matching.getValue()/(float)querySize)==1.0)
                System.out.printf(ANSI_GREEN_YELLOW_UNDER+"%.2f%% Full Matching found in line %d of file %s: %s.\n"+ANSI_RESET,(matching.getValue()/(float)querySize)*100.0,location.getLine(), location.getFileId(), getIndexFilesLine(location));
            else if ((matching.getValue()/(float)querySize)>=DMatchingPercentage)
                System.out.printf(ANSI_GREEN+"%.2f%% Matching found in line %d of file %s: %s.\n"+ANSI_RESET,(matching.getValue()/(float)querySize)*100.0,location.getLine(), location.getFileId(), getIndexFilesLine(location));
            else if ((matching.getValue()/(float)querySize)>=DNearlyMatchingPercentage)
                System.out.printf(ANSI_RED+"%.2f%% Weak Matching found in line %d of file %s: %s.\n"+ANSI_RESET,(matching.getValue()/(float)querySize)*100.0,location.getLine(), location.getFileId(), getIndexFilesLine(location));
        }

        Instant finish = Instant.now();
        long timeElapsed = Duration.between(start, finish).toMillis();  //in millis
        System.out.printf("[Query with %d words] Total execution time: %.3f secs.\n", querySize, timeElapsed/1000.0);
    }

    private String getIndexFilesLine(Location loc){
        return(IndexFilesLines.get(loc));
    }

    private synchronized void addIndexFilesLine(Location loc, String line){
        IndexFilesLines.put(loc, line);
    }


    /*----------------------------------------------Codigo nuevo------------------------------------------------------*/

    class loadIndexTask implements Runnable {

        private File file;

        public loadIndexTask(File file) {
            this.file = file;
        }

        @Override
        public void run() {
            //System.out.println("Processing file " + folder.getPath() + "/" + file.getName()+" -> ");
            try {
                FileReader input = new FileReader(file);
                BufferedReader bufRead = new BufferedReader(input);
                String keyLine = null;
                try {
                    // Leemos fichero línea a linea (clave a clave)
                    while ( (keyLine = bufRead.readLine()) != null)
                    {
                        HashSet<Location> locationsList = new HashSet<Location>();
                        // Descomponemos la línea leída en su clave (word) y las ubicaciones
                        String[] fields = keyLine.split("\t");
                        String word = fields[0];
                        String[] locations = fields[1].split(", ");
                        // Recorremos los offsets para esta clave y los añadimos al HashMap
                        for (int i = 0; i < locations.length; i++)
                        {
                            String[] location = locations[i].substring(1, locations[i].length()-1).split(",");
                            int fileId = Integer.parseInt(location[0]);
                            int line = Integer.parseInt(location[1]);
                            locationsList.add(new Location(fileId,line));
                        }
                        synchronized (Hash) {
                            Hash.put(word, locationsList);
                        }
                    }
                } catch (IOException e) {
                    System.err.println("Error reading Index file");
                    e.printStackTrace();
                }
            } catch (FileNotFoundException e) {
                System.err.println("Error opening Index file");
                e.printStackTrace();
            }
            //System.out.println("")
        }
    }

    class saveInvertedIndexTask implements Runnable {
        private final Set<String> keys;
        private final String outputDirectory;
        private final int fileId;

        public saveInvertedIndexTask(Set<String> keys, String outputDirectory, int fileId) {
            this.keys = keys;
            this.outputDirectory = outputDirectory;
            this.fileId = fileId;
        }

        @Override
        public void run() {
            try {
                File keyFile = new File(outputDirectory + "/" + DIndexFilePrefix + String.format("%03d", fileId));
                FileWriter fw = new FileWriter(keyFile);
                BufferedWriter bw = new BufferedWriter(fw);

                for (String key : keys) {
                    saveIndexKey(key, bw);
                }

                bw.close();
            } catch (IOException e) {
                System.err.println("Error creating Index file " + outputDirectory + "/IndexFile" + fileId);
                e.printStackTrace();
                System.exit(-1);
            } finally {
                semaphore.release();
            }
        }
    }
}