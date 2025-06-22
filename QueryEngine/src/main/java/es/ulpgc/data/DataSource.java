package es.ulpgc.data;

import java.util.Map;
import java.util.Set;

public interface DataSource {
    Map<String, Set<String>> loadIndex();
    Map<String, Map<String, String>> loadMetadata(Set<String> ebookNumbers);
}

