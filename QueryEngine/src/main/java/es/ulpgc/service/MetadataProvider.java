package es.ulpgc.service;

import es.ulpgc.data.CSVDataSource;
import es.ulpgc.data.DataSource;
import es.ulpgc.data.DatamartDataSource;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class MetadataProvider {
    private final DataSource dataSource;

    public MetadataProvider(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public Map<String, Map<String, String>> getMetadataForResults(Set<String> ebookNumbers) {
        if (dataSource instanceof CSVDataSource) {
            return ((CSVDataSource) dataSource).loadMetadata(ebookNumbers);
        } else if (dataSource instanceof DatamartDataSource) {
            return ((DatamartDataSource) dataSource).loadMetadata(ebookNumbers);
        }
        return new HashMap<>();
    }
}
