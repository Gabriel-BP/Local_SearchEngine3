package es.ulpgc.data;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class CSVDataSource implements DataSource {
    private final String contentFilePath;
    private final String metadataFilePath;

    public CSVDataSource(String contentFilePath, String metadataFilePath) {
        this.contentFilePath = contentFilePath;
        this.metadataFilePath = metadataFilePath;
    }

    @Override
    public Map<String, Set<String>> loadIndex() {
        Map<String, Set<String>> index = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(contentFilePath))) {
            String line;
            br.readLine(); // Skip header
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                String word = parts[0];
                Set<String> ebookNumbers = new HashSet<>(Arrays.asList(parts).subList(1, parts.length));
                index.put(word, ebookNumbers);
            }
        } catch (IOException e) {
            System.err.println("Error reading the CSV file: " + e.getMessage());
        }
        return index;
    }

    public Map<String, Map<String, String>> loadMetadata(Set<String> ebookNumbers) {
        Map<String, Map<String, String>> metadata = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(metadataFilePath))) {
            String line = br.readLine(); // Read header
            String[] headers = line.split(",");
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                String ebookNumber = parts[0];
                if (ebookNumbers.contains(ebookNumber)) {
                    Map<String, String> ebookMetadata = new HashMap<>();
                    for (int i = 1; i < headers.length; i++) {
                        ebookMetadata.put(headers[i], parts[i]);
                    }
                    metadata.put(ebookNumber, ebookMetadata);
                }
            }
        } catch (IOException e) {
            System.err.println("Error reading metadata CSV: " + e.getMessage());
        }
        return metadata;
    }
}
