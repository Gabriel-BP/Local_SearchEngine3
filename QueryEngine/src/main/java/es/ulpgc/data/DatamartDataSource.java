package es.ulpgc.data;

import io.minio.GetObjectArgs;
import io.minio.MinioClient;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DatamartDataSource implements DataSource {

    private final MinioClient minioClient;
    private final String bucket = "datalake";

    public DatamartDataSource(String endpoint) {
        this.minioClient = MinioClient.builder()
                .endpoint(endpoint)
                .credentials("admin", "admin123")
                .build();
    }

    @Override
    public Map<String, Set<String>> loadIndex() {
        return null; // Not used for Datamart
    }

    @Override
    public Map<String, Map<String, String>> loadMetadata(Set<String> ebookNumbers) {
        Map<String, Map<String, String>> metadata = new HashMap<>();
        for (String ebookNumber : ebookNumbers) {
            String remotePath = "datamart_metadata/" + ebookNumber + "/metadata.json";
            try (InputStream stream = minioClient.getObject(
                    GetObjectArgs.builder().bucket(bucket).object(remotePath).build())) {

                String content = new String(stream.readAllBytes(), StandardCharsets.UTF_8);
                JSONObject jsonObject = new JSONObject(content);

                Map<String, String> ebookMetadata = new HashMap<>();
                for (String key : jsonObject.keySet()) {
                    ebookMetadata.put(key, jsonObject.getString(key));
                }
                metadata.put(ebookNumber, ebookMetadata);

            } catch (Exception e) {
                System.err.println("[WARN] Metadata not found: " + remotePath + " -> " + e.getMessage());
            }
        }
        return metadata;
    }

    public Set<String> searchWord(String word) {
        StringBuilder prefix = new StringBuilder();
        String triePath = "datamart_content";

        for (char c : word.toCharArray()) {
            prefix.append(c);
            triePath += "/" + prefix;
        }

        triePath += "/" + word + ".txt";

        try {
            String content = MinioClientHelper.getObjectAsString(triePath);
            JSONObject jsonObject = new JSONObject(content);

            Set<String> references = new HashSet<>();
            if (jsonObject.has("references")) {
                JSONArray refs = jsonObject.getJSONArray("references");
                for (int i = 0; i < refs.length(); i++) {
                    references.add(refs.getString(i));
                }
            }
            return references;

        } catch (Exception e) {
            System.out.println("[INFO] Word not found in MinIO: " + triePath + " -> " + e.getMessage());
            return new HashSet<>();
        }
    }



}
