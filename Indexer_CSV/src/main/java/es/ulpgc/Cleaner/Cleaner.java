package es.ulpgc.Cleaner;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;

public class Cleaner {

    private final TextCleaner textCleaner;

    public Cleaner() {
        Set<String> stopwords = StopwordsLoader.loadStopwords();
        this.textCleaner = new TextCleaner(stopwords);
    }

    public Book processBook(File file) throws IOException {
        String content = new String(Files.readAllBytes(file.toPath()));

        Map<String, String> metadata = MetadataExtractor.extractMetadata(content);

        int startIdx = content.indexOf("*** START OF THIS PROJECT GUTENBERG EBOOK");
        if (startIdx != -1) {
            content = content.substring(startIdx);
        }
        String fullContent = content;
        List<String> words = textCleaner.cleanText(content);
        String ebookNumber = file.getName().replaceFirst("[.][^.]+$", "");  // Eliminar la extensión
        return new Book(
                metadata.get("title"),
                metadata.get("author"),
                metadata.get("date"),
                metadata.get("language"),
                metadata.get("credits"),
                ebookNumber,
                words,
                fullContent
        );
    }

}
