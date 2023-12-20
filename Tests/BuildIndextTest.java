    import eps.scp.InvertedIndex;
    import org.junit.jupiter.api.Test;
    import org.junit.jupiter.api.BeforeEach;
    import static org.junit.jupiter.api.Assertions.assertEquals;
    public class BuildIndextTest {

        InvertedIndex invertedIndex;

        @BeforeEach
        public void setUp() {
            invertedIndex = new InvertedIndex("./test", 100);
            invertedIndex.buidIndex();
        }

        @Test
        public void testGetTotalLines() {
            assertEquals(invertedIndex.getTotalLines(), 16);
        }

        @Test
        public void testGetTotalWords() {
            assertEquals(invertedIndex.getTotalWords(), 30);
        }

        @Test
        public void testGetTotalKeysFound() {
            assertEquals(invertedIndex.getTotalKeysFound(), 23);
        }

        @Test
        public void testGetTotalProcessedFiles() {
            assertEquals(invertedIndex.getTotalProcessedFiles(), 3);
        }

        @Test
        public void testGetTotalLocations() {
            // En nuestro caso la letra a
            assertEquals(invertedIndex.getTotalLocations(), 27); // El número máximo de apariciones de un carácter
        }

        @Test
        public void testGlobalStatistics() {
            assertEquals(invertedIndex.getGlobalStatistics().getProcessedFiles(), invertedIndex.getTotalProcessedFiles());
            assertEquals(invertedIndex.getGlobalStatistics().getProcessedWords(), invertedIndex.getTotalWords());
            assertEquals(invertedIndex.getGlobalStatistics().getProcessedLocations(), invertedIndex.getTotalLocations());
        }
    }
