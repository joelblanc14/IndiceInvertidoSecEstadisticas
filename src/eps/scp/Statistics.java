package eps.scp;

import org.apache.commons.lang3.StringUtils;

public class Statistics {
    static String color_red = "\33[01;31m";
    static String color_green = "\033[01;32m";
    static final String color_blue = "\033[01;34m";
    static final String end_color = "\033[00m";

    final int CLineSize = 129;
    final int CLineHeaderSize = 2;
    final int CFieldSize = 10;

    private String c;

    // Statistics:
    public int ProcessingFiles = 0;
    public int ProcessedFiles = 0;
    public long ProcessedLines = 0;
    public long ProcessedWords = 0;
    public long ProcessedLocations = 0;
    public int KeysFound = 0;
    public String MostPopularWord;
    public int MostPopularWordLocations = 0;

    public int getProcessingFiles() { return ProcessingFiles; }
    public void setProcessingFiles(int processingFiles) { ProcessingFiles = processingFiles; }
    public void incProcessingFiles() { ProcessingFiles++; }
    public void addProcessingFiles(int processingFiles) { ProcessingFiles += processingFiles; }
    public void decProcessingFiles() { ProcessingFiles--; }
    public int getProcessedFiles() { return ProcessedFiles; }
    public void setProcessedFiles(int processedFiles) { ProcessedFiles = processedFiles; }
    public void addProcessedFiles(int processedFiles) { ProcessedFiles += processedFiles; }
    public void incProcessedFiles() { ProcessedFiles++; }
    public long getProcessedLines() { return ProcessedLines; }
    public void setProcessedLines(long processedLines) { ProcessedLines = processedLines; }
    public void incProcessedLines() { ProcessedLines++; }
    public void addProcessedLines(long processedLines) { ProcessedLines += processedLines; }
    public long getProcessedWords() { return ProcessedWords; }
    public void setProcessedWords(long processedWords) { ProcessedWords = processedWords; }
    public void incProcessedWords() { ProcessedWords++; }
    public void addProcessedWords(long processedWords) { ProcessedWords += processedWords; }
    public long getProcessedLocations() { return ProcessedLocations; }
    public void setProcessedLocations(long processedLocations) { ProcessedLocations = processedLocations;}
    public void incProcessedLocations() { ProcessedLocations++;}
    public void addProcessedLocations(long processedLocations) { ProcessedLocations += processedLocations;}
    public int getKeysFound() { return KeysFound; }
    public void setKeysFound(int keysFound) { KeysFound = keysFound; }
    public void incKeysFound() { KeysFound++; }
    public void addtKeysFound(int keysFound) { KeysFound += keysFound; }
    public String getMostPopularWord() { return MostPopularWord; }
    public void setMostPopularWord(String mostPopularWord) { MostPopularWord = mostPopularWord; }
    public int getMostPopularWordLocations() { return MostPopularWordLocations; }
    public void setMostPopularWordLocations(int mostPopularWordLocations) { MostPopularWordLocations = mostPopularWordLocations; }

    public Statistics (String c){
        this.c = c;
    }

    public void addStatistics(Statistics stats){
        addProcessedFiles(stats.getProcessedFiles());
        addProcessingFiles(stats.getProcessingFiles());
        addProcessedLines(stats.getProcessedLines());
        addProcessedWords(stats.getProcessedWords());
        addtKeysFound(stats.getKeysFound());
        addProcessedLocations(stats.getProcessedLocations());
        if (getMostPopularWordLocations()<stats.getMostPopularWordLocations())
        {
            setMostPopularWord(stats.getMostPopularWord());
            setMostPopularWordLocations(stats.getMostPopularWordLocations());
        }
    }

    public void print(String name)
    {
        System.out.print(color_blue);
        System.out.println(StringUtils.repeat(c,CLineHeaderSize)+" "+name+" "+StringUtils.repeat(c,CLineSize-(CLineHeaderSize+name.length()+2)));
        print();
        System.out.println(StringUtils.repeat(c,CLineSize));
        System.out.print(end_color);

    }

    public void print()
    {
        System.out.printf(StringUtils.repeat(c,CLineHeaderSize)+" Processed Files:     %-"+CFieldSize+"d",getProcessedFiles());
        System.out.printf(StringUtils.repeat(c,CLineHeaderSize)+" Processing Files: %-"+CFieldSize+"d",getProcessingFiles());
        System.out.printf(StringUtils.repeat(c,CLineHeaderSize)+" Processed Lines:   %-"+CFieldSize+"d",getProcessedLines());
        System.out.printf(StringUtils.repeat(c,CLineHeaderSize)+" Processed Words: %-"+CFieldSize+"d",getProcessedWords());
        System.out.println(StringUtils.repeat(c,CLineHeaderSize));
        System.out.printf(StringUtils.repeat(c,CLineHeaderSize)+" Processed Locations: %-"+CFieldSize+"d",getProcessedLocations());
        System.out.printf(StringUtils.repeat(c,CLineHeaderSize)+" Found Keys:       %-"+CFieldSize+"d",getKeysFound());
        System.out.printf(StringUtils.repeat(c,CLineHeaderSize)+" Most Popular word: %-"+CFieldSize+"s",getMostPopularWord());
        System.out.printf(StringUtils.repeat(c,CLineHeaderSize)+" Locations:       %-"+CFieldSize+"d",getMostPopularWordLocations());
        System.out.println(StringUtils.repeat(c,CLineHeaderSize));
    }
}
