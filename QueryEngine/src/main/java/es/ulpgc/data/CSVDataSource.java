package es.ulpgc.data;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class CSVDataSource implements DataSource {
    private final String contentFilePath = "index_content.csv";
    private final String metadataFilePath = "index_metadata.csv";

    @Override
    public Map<String, Set<String>> loadIndex() {
        downloadIndexFile("index/index_content.csv", contentFilePath);
        Map<String, Set<String>> index = new HashMap<>();

        try (BufferedReader br = new BufferedReader(new FileReader(contentFilePath))) {
            String line;
            br.readLine(); // Skip header
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length < 2) continue;
                String word = parts[0];
                Set<String> ebookNumbers = new HashSet<>(Arrays.asList(parts).subList(1, parts.length));
                index.put(word, ebookNumbers);
            }
        } catch (IOException e) {
            System.err.println("Error reading the CSV file: " + e.getMessage());
        }

        return index;
    }

    @Override
    public Map<String, Map<String, String>> loadMetadata(Set<String> ebookNumbers) {
        downloadIndexFile("index/index_metadata.csv", metadataFilePath);
        Map<String, Map<String, String>> metadata = new HashMap<>();

        try (BufferedReader br = new BufferedReader(new FileReader(metadataFilePath))) {
            String headerLine = br.readLine();
            if (headerLine == null) return metadata;

            String[] headers = splitCsvLine(headerLine);
            String line;

            while ((line = br.readLine()) != null) {
                String[] parts = splitCsvLine(line);
                if (parts.length != headers.length) continue;

                String ebookNumber = clean(parts[0]);
                if (!ebookNumbers.contains(ebookNumber)) continue;

                Map<String, String> ebookMetadata = new HashMap<>();
                for (int i = 1; i < headers.length; i++) {
                    String key = headers[i].trim().toLowerCase();
                    String value = clean(parts[i]);
                    ebookMetadata.put(key, value);
                }
                metadata.put(ebookNumber, ebookMetadata);
            }
        } catch (IOException e) {
            System.err.println("Error reading metadata CSV: " + e.getMessage());
        }

        return metadata;
    }

    private String clean(String value) {
        return value == null ? "" : value.trim().replaceAll("^\"|\"$", "");
    }

    private String[] splitCsvLine(String line) {
        List<String> result = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);

            if (c == '\"') {
                inQuotes = !inQuotes;
            } else if (c == ',' && !inQuotes) {
                result.add(current.toString());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        result.add(current.toString());
        return result.toArray(new String[0]);
    }


    private void downloadIndexFile(String remote, String local) {
        try {
            MinioClientHelper.downloadFile(remote, local);
        } catch (Exception e) {
            System.err.println("[WARN] Could not download " + remote + ": " + e.getMessage());
        }
    }
}
