package es.ulpgc.service;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class QueryTokenizer {
    public List<String> tokenize(Query query) {
        List<String> tokens = new ArrayList<>();
        StringTokenizer tokenizer = new StringTokenizer(query.getQuery());

        while (tokenizer.hasMoreTokens()) {
            tokens.add(tokenizer.nextToken().toLowerCase()); // Convertir a minúsculas para consistencia
        }

        return tokens;
    }
}
