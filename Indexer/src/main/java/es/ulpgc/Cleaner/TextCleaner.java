package es.ulpgc.Cleaner;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class TextCleaner {
    private final Set<String> stopwords;

    public TextCleaner(Set<String> stopwords) {
        this.stopwords = stopwords;
    }

    public List<String> cleanText(String text) {
        List<String> meaningfulWords = new ArrayList<>();
        String[] words = text.toLowerCase().split("\\W+");

        for (String word : words) {
            word = word.replace("_", "").trim();
            if (!stopwords.contains(word) && word.length() > 2 && !word.matches("\\d+")) {
                meaningfulWords.add(word);
            }
        }
        return meaningfulWords;
    }
}
