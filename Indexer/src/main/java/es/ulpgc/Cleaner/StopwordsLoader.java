package es.ulpgc.Cleaner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

public class StopwordsLoader {
    private static final String STOPWORDS_RESOURCE_PATH = "stopwords-en.txt"; // Correct path to the file

    public static Set<String> loadStopwords() {
        Set<String> stopwords = new HashSet<>();
        try (InputStream inputStream = StopwordsLoader.class.getClassLoader().getResourceAsStream(STOPWORDS_RESOURCE_PATH)) {
            if (inputStream == null) {
                System.err.println("Error: Resource not found: " + STOPWORDS_RESOURCE_PATH);
                return stopwords; // Return empty set if resource is not found
            }
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    stopwords.add(line.trim().toLowerCase()); // Normalize to lowercase
                }
            }
        } catch (IOException e) {
            System.err.println("Error loading stopwords from resource: " + STOPWORDS_RESOURCE_PATH);
        }
        return stopwords;
    }
}


