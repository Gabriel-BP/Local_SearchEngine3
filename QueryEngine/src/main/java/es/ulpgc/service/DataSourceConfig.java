package es.ulpgc.service;

import es.ulpgc.data.CSVDataSource;
import es.ulpgc.data.DataSource;
import es.ulpgc.data.DatamartDataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DataSourceConfig {

    @Bean
    public DataSource dataSource() {
        String mode = System.getenv().getOrDefault("QUERYENGINE_MODE", "datamart").toLowerCase();
        String endpoint = System.getenv().getOrDefault("MINIO_ENDPOINT", "http://localhost:9000");

        if (mode.equals("csv")) {
            System.out.println("✅ Using CSVDataSource (CSV files).");
            return new CSVDataSource();
        } else if (mode.equals("datamart")) {
            System.out.println("✅ Using DatamartDataSource (MinIO).");
            return new DatamartDataSource(endpoint);
        } else {
            throw new RuntimeException("❌ Invalid QUERYENGINE_MODE: " + mode);
        }
    }
}
