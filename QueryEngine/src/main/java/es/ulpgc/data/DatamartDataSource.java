package es.ulpgc.data;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DatamartDataSource implements DataSource {
    private final String datamartRootPath;
    private final String metadataRootPath;

    public DatamartDataSource(String datamartRootPath, String metadataRootPath) {
        this.datamartRootPath = datamartRootPath;
        this.metadataRootPath = metadataRootPath;
    }

    @Override
    public Map<String, Set<String>> loadIndex() {
        return null; // Not used in Datamart
    }

    public Set<String> searchWord(String word) {
        File currentDir = new File(datamartRootPath);

        StringBuilder prefix = new StringBuilder();
        for (char c : word.toCharArray()) {
            prefix.append(c);
            currentDir = new File(currentDir, prefix.toString());
            if (!currentDir.exists() || !currentDir.isDirectory()) {
                System.out.println("Could not find the word: " + word);
                return new HashSet<>();
            }
        }

        File wordFile = new File(currentDir, word + ".txt");
        if (wordFile.exists() && wordFile.isFile()) {
            return readWordFile(wordFile);
        } else {
            System.out.println("File not found for the word: " + word);
            return new HashSet<>();
        }
    }

    private Set<String> readWordFile(File file) {
        Set<String> references = new HashSet<>();
        try {
            String content = new String(Files.readAllBytes(Paths.get(file.getPath())));
            JSONObject jsonObject = new JSONObject(content);

            // Extraer las referencias desde el JSON
            if (jsonObject.has("references")) {
                JSONArray refs = jsonObject.getJSONArray("references");
                for (int i = 0; i < refs.length(); i++) {
                    references.add(refs.getString(i));
                }
            }
        } catch (IOException e) {
            System.err.println("Error reading the file " + file.getPath() + ": " + e.getMessage());
        }
        return references;
    }

    public Map<String, Map<String, String>> loadMetadata(Set<String> ebookNumbers) {
        Map<String, Map<String, String>> metadata = new HashMap<>();

        for (String ebookNumber : ebookNumbers) {
            File metadataFile = new File(metadataRootPath, ebookNumber + "/metadata.json");
            if (metadataFile.exists()) {
                try {
                    String content = new String(Files.readAllBytes(Paths.get(metadataFile.getPath())));
                    JSONObject jsonObject = new JSONObject(content);

                    Map<String, String> ebookMetadata = new HashMap<>();
                    for (String key : jsonObject.keySet()) {
                        ebookMetadata.put(key, jsonObject.getString(key));
                    }
                    metadata.put(ebookNumber, ebookMetadata);
                } catch (IOException e) {
                    System.err.println("Error reading metadata for ebook " + ebookNumber + ": " + e.getMessage());
                }
            }
        }
        return metadata;
    }
}
