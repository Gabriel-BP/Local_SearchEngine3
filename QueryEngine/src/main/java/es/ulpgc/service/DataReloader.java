package es.ulpgc.service;

import es.ulpgc.data.CSVDataSource;
import es.ulpgc.data.DataSource;
import es.ulpgc.data.MinioClientHelper;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

@Component
public class DataReloader {
    private final ApplicationContext context;
    private final QueryEngine queryEngine;

    public DataReloader(ApplicationContext context, QueryEngine queryEngine) {
        this.context = context;
        this.queryEngine = queryEngine;
    }

    @Scheduled(fixedRate = 60000) // Cada 60 segundos
    public void reloadData() {
        try {
            System.out.println("[INFO] Scheduled reload: downloading updated CSVs from MinIO...");
            Files.createDirectories(Path.of("/app/csv_data"));

            for (String remote : new String[]{"index/index_content.csv", "index/index_metadata.csv"}) {
                String temp = "/app/csv_data/" + remote.substring("index/".length()) + ".tmp";
                String finalPath = "/app/csv_data/" + remote.substring("index/".length());
                try {
                    MinioClientHelper.downloadFile(remote, temp);
                    Files.move(Path.of(temp), Path.of(finalPath), StandardCopyOption.REPLACE_EXISTING);
                    System.out.println("[INFO] Updated: " + remote);
                } catch (Exception e) {
                    System.err.println("[WARN] Skipped " + remote + ": " + e.getMessage());
                }
            }

            DataSource newDataSource = new CSVDataSource(
                    "/app/csv_data/index_content.csv",
                    "/app/csv_data/index_metadata.csv"
            );
            queryEngine.reloadIndex(newDataSource);
            System.out.println("[INFO] Index reloaded successfully.");
        } catch (Exception e) {
            System.err.println("[ERROR] Failed to reload data from MinIO: " + e.getMessage());
        }
    }
}
