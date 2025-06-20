package es.ulpgc.service;

import es.ulpgc.data.CSVDataSource;
import es.ulpgc.data.DataSource;
import es.ulpgc.data.MinioClientHelper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import java.nio.file.Files;
import java.nio.file.Paths;

@Configuration
public class DataSourceConfig {

    @Bean
    @Scope("prototype") // Crea una nueva instancia en cada llamada
    public DataSource csvDataSource() {
        try {
            String contentPath = "/app/csv_data/index_content.csv";
            String metadataPath = "/app/csv_data/index_metadata.csv";

            System.out.println("[INFO] Ensuring directory exists: /app/csv_data");
            Files.createDirectories(Paths.get("/app/csv_data"));

            System.out.println("[INFO] Downloading index files from MinIO...");
            for (String remoteFile : new String[]{"index/index_content.csv", "index/index_metadata.csv"}) {
                String localFile = "/app/csv_data/" + remoteFile.substring("index/".length());
                MinioClientHelper.downloadFile(remoteFile, localFile);
            }
            System.out.println("[INFO] Index files downloaded successfully.");

            return new CSVDataSource(contentPath, metadataPath);

        } catch (Exception e) {
            System.err.println("[ERROR] Cannot download index files from MinIO: " + e.getMessage());
            throw new RuntimeException("Failed to initialize CSVDataSource from MinIO", e);
        }
    }
}
