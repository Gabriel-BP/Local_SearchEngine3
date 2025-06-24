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
            if (
                    word.length() > 2 &&
                            !stopwords.contains(word) &&
                            word.matches("^[a-z]+$")  // solo letras minúsculas
            ) {
                meaningfulWords.add(word);
            }

        }
        return meaningfulWords;
    }
}
