package eps.scp;

import java.io.*;
import java.text.Normalizer;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.jupiter.api.Assertions.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

public class InvertedIndex
{
    private static final int M = 1000;
    private int contador = 0;

    private int numThreads = 3;
    // Definimos el semáforo
    private final Semaphore semaphore = new Semaphore(1);

    // Creamos un lock y una Condition
    private final Lock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();

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
    private AtomicLong TotalLocations = new AtomicLong(0);
    private AtomicLong TotalWords = new AtomicLong(0);
    private AtomicLong TotalLines = new AtomicLong(0);
    private AtomicInteger TotalProcessedFiles = new AtomicInteger(0);

    final int CLineSize = 129;
    final int CLineHeaderSize = 2;
    final int CFieldSize = 10;

    // Getters
    public Map<Integer, String> getFiles() { return Files; }
    public Map<Location, String> getIndexFilesLines() { return IndexFilesLines; }
    public Map<String, HashSet<Location>> getHash() { return Hash; }
    public void setIndexDirPath(String indexDirPath) {
        IndexDirPath = indexDirPath;
    }
    public long getTotalWords(Map hash){ return(hash.size()); }
    public long getTotalLocations() { return TotalLocations.get(); }
    public long getTotalWords() { return TotalWords.get(); }
    public long getTotalLines() { return TotalLines.get(); }
    public int getTotalProcessedFiles() { return TotalProcessedFiles.get(); }


    // Constructores
    public InvertedIndex() {
    }
    public InvertedIndex(String InputPath) {
        this.InputDirPath = InputPath;
        this.IndexDirPath = DDefaultIndexDir;
    }

    public InvertedIndex(String inputDir, String indexDir) {
        this.InputDirPath = inputDir;
        this.IndexDirPath = indexDir;
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
            assertEquals(getTotalWords(Hash), getTotalWords());
            assertEquals(getTotalLocations(Hash), getTotalLocations());
            assertEquals(getTotalFiles(Files), getTotalProcessedFiles());
            assertEquals(getTotalLines(IndexFilesLines), getTotalLines());
        } catch (AssertionError e) {
            System.out.println(ANSI_RED + e.getMessage() + " " + ANSI_RESET);
        }
    }

    private void initializeCounters() {
        TotalProcessedFiles = new AtomicInteger(0);
        TotalLocations = new AtomicLong(0);
        TotalLines = new AtomicLong(0);
        TotalWords = new AtomicLong(0);
    }

    private void processFilesWithThreads() {
        int fileId = 0;
        List<Thread> threads = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(FilesList.size()); // Inicializado con el número de hilos que se van a ejecutar

        for (File file : FilesList) {
            fileId++;
            Files.put(fileId, file.getAbsolutePath());
            int finalFileId = fileId;
            threads.add(Thread.startVirtualThread(() -> {
                addFileWords2Index(finalFileId, file);
                latch.countDown(); // Señala que un hilo ha completado su ejecución
            }));
        }

        waitForThreadsToFinish(latch);
    }

    private void waitForThreadsToFinish(CountDownLatch latch) {
        // Espera a que todos los hilos hayan completado su ejecución
        while (latch.getCount() > 0) {
            try {
                latch.await();
            } catch (InterruptedException e) {
                System.err.println("Error al esperar a que todos los hilos finalicen: " + e.getMessage());
            }
        }
        // En este punto, todos los hilos han llamado a countDown()
        // y el CountDownLatch ha alcanzado cero.
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
            //HashSet<Location> locs = Hash.get(word);
            //String joined = String.join(";",locs.toString());
            //System.out.printf(ANSI_BLUE+"[%d-%d] %s --> %s\n"+ANSI_RESET,locations-locs.size(),locations, word, joined);
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

    /*public void setBarrierSize(int size) {
        cyclicBarrier = new CyclicBarrier(size);
    }*/

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
        Statistics FileStatistics = new Statistics("_");
        System.out.printf("Processing %3dth file %s (Path: %s)\n", fileId, file.getName(), file.getAbsolutePath());
        TotalProcessedFiles.incrementAndGet();
        FileStatistics.incProcessingFiles();

        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            int lineNumber = 0;
            while ((line = br.readLine()) != null) {
                lineNumber++;
                TotalLines.incrementAndGet();
                FileStatistics.incProcessedLines();
                if (Indexing.Verbose) System.out.printf("Procesando linea %d fichero %d: ", lineNumber, fileId);
                Location newLocation = new Location(fileId, lineNumber);
                addIndexFilesLine(newLocation, line);

                line = Normalizer.normalize(line, Normalizer.Form.NFD);
                line = line.replaceAll("[\\p{InCombiningDiacriticalMarks}]", "");
                String filter_line = line.replaceAll("[^a-zA-Z0-9áÁéÉíÍóÓúÚäÄëËïÏöÖüÜñÑ ]", "");
                String[] words = filter_line.split("\\W+");

                for (String word : words) {
                    if (Indexing.Verbose) System.out.printf("%s ", word);
                    word = word.toLowerCase();

                    // Adquiere el semáforo
                    try {
                        semaphore.acquire();

                        HashSet<Location> locations = Hash.get(word);
                        if (locations == null) {
                            locations = new HashSet<>();
                            if (!Hash.containsKey(word))
                                FileStatistics.incKeysFound();
                            Hash.put(word, locations);
                            TotalWords.incrementAndGet();
                            FileStatistics.incProcessedWords();
                        }

                        int oldLocSize = locations.size();
                        locations.add(newLocation);
                        if (locations.size() > oldLocSize) {
                            TotalLocations.incrementAndGet();
                            FileStatistics.incProcessedLocations();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        // Libera el semáforo en un bloque finally para asegurar que se libere
                        // incluso si ocurre una excepción.
                        semaphore.release();
                    }
                    // Actualiza contadores y muestra resultados parciales y totales cada M palabras
                    contador++;
                    if (contador % M == 0) {
                        showPartialAndTotalStatistics(FileStatistics, file.getName(), 130);

                    }
                }
                if (Indexing.Verbose) System.out.println();
            }
        } catch (FileNotFoundException e) {
            System.err.printf("Fichero %s no encontrado.\n", file.getAbsolutePath());
            e.printStackTrace();
        } catch (IOException e) {
            System.err.printf("Error lectura fichero %s.\n", file.getAbsolutePath());
            e.printStackTrace();
        }

        try {
            semaphore.acquire();
            FileStatistics.incProcessedFiles();
            FileStatistics.decProcessingFiles();
            setMostPopularWord(FileStatistics);
            FileStatistics.print(file.getName());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            semaphore.release();
        }

        GlobalStatistics.addStatistics(FileStatistics);
    }
    private void showPartialAndTotalStatistics(Statistics fileStatistics, String fileName, int CLineSize) {
        System.out.println(StringUtils.repeat("_", CLineSize));
        System.out.printf("__ %s %s\n", StringUtils.center(fileName, CLineSize - CLineHeaderSize), StringUtils.repeat("_", CLineHeaderSize));
        System.out.println(StringUtils.repeat("_", CLineSize));

        // Muestra estadísticas parciales en azul
        System.out.printf("%s__ Thread %d - Partial Statistics:\n", ANSI_BLUE, Thread.currentThread().getId());
        System.out.printf("__ Processed Files:     %-" + CFieldSize + "d", fileStatistics.getProcessedFiles());
        System.out.printf("__ Processing Files: %-" + CFieldSize + "d", fileStatistics.getProcessingFiles());
        System.out.printf("__ Processed Lines:   %-" + CFieldSize + "d", fileStatistics.getProcessedLines());
        System.out.printf("__ Processed Words: %-" + CFieldSize + "d\n", fileStatistics.getProcessedWords());
        System.out.printf("__ Processed Locations: %-" + CFieldSize + "d", fileStatistics.getProcessedLocations());
        System.out.printf("__ Found Keys:       %-" + CFieldSize + "d", fileStatistics.getKeysFound());
        System.out.printf("__ Most Popular word: %-" + CFieldSize + "s", fileStatistics.getMostPopularWord());
        System.out.printf("__ Locations:       %-" + CFieldSize + "d\n", fileStatistics.getMostPopularWordLocations());
        System.out.println(StringUtils.repeat("_", CLineSize));
        System.out.print("\033[00m"); // Restaura el color normal

        // Muestra estadísticas totales en verde
        System.out.printf("%s__ Total Statistics:\n", ANSI_GREEN);
        System.out.printf("__ Ficheros procesados: %-" + CFieldSize + "d\n", getTotalProcessedFiles());
        System.out.printf("__ Líneas procesadas:   %-" + CFieldSize + "d\n", getTotalLines());
        System.out.printf("__ Palabras procesadas: %-" + CFieldSize + "d\n", getTotalWords());
        System.out.printf("__ Ubicaciones encontradas: %-" + CFieldSize + "d\n", getTotalLocations());
        System.out.println(StringUtils.repeat("_", CLineSize));
        System.out.print("\033[00m"); // Restaura el color normal
    }


    public synchronized void setMostPopularWord(Statistics stats) {
        String maxWord = Collections.max(Hash.entrySet(), Comparator.comparingInt(entry -> entry.getValue().size())).getKey();
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

        waitForThreadsToFinish();

        Instant finish = Instant.now();
        long timeElapsed = Duration.between(start, finish).toMillis();  // in millis
        System.out.printf("[Save Index with %d keys] Total execution time: %.3f secs.\n", Hash.size(), timeElapsed / 1000.0);
    }

    private void waitForThreadsToFinish() {
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

        Phaser phaser = new Phaser(numberOfFiles);

        while (keyIterator.hasNext()) {
            Set<String> keysToSave = new HashSet<>();
            for (int i = 0; i < keysPerFile && keyIterator.hasNext(); i++) {
                keysToSave.add(keyIterator.next());
            }

            saveInvertedIndexTask task = new saveInvertedIndexTask(keysToSave, outputDirectory, fileId, phaser);
            threads.add(Thread.startVirtualThread(task));

            fileId++;
        }
        phaser.arriveAndAwaitAdvance(); // Espera a que todos los hilos lleguen a este punto
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
        CyclicBarrier barrier = new CyclicBarrier(4); // 3 + 1 para el hilo principal
        List<Thread> threads = new ArrayList<>();

        threads.add(Thread.startVirtualThread(() -> {
            loadInvertedIndex(indexDirectory);
            awaitBarrier(barrier);
        }));

        threads.add(Thread.startVirtualThread(() -> {
            loadFilesIds(indexDirectory);
            awaitBarrier(barrier);
        }));

        threads.add(Thread.startVirtualThread(() -> {
            loadFilesLines(indexDirectory);
            awaitBarrier(barrier);
        }));

        // Espera a que todos los hilos alcancen la barrera final
        awaitBarrier(barrier);

        Instant finish = Instant.now();
        long timeElapsed = Duration.between(start, finish).toMillis();  // en milisegundos
        System.out.printf("[Load Index with %d keys] Total execution time: %.3f secs.\n", Hash.size(), timeElapsed / 1000.0);
    }

    private void awaitBarrier(CyclicBarrier barrier) {
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
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

    //*********************************************NUEVO CÓDIGO************************************************************
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
        private final Phaser phaser;

        public saveInvertedIndexTask(Set<String> keys, String outputDirectory, int fileId, Phaser phaser) {
            this.keys = keys;
            this.outputDirectory = outputDirectory;
            this.fileId = fileId;
            this.phaser = phaser;
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
                phaser.arriveAndDeregister();
            }
        }
    }
}