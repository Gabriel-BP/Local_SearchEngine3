package es.ulpgc.Cleaner;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MetadataExtractor {
    public static Map<String, String> extractMetadata(String content) {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("title", extractField("Title:\\s*(.+)", content, "Unknown Title"));
        metadata.put("author", extractField("Author:\\s*(.+)", content, "Unknown Author"));
        metadata.put("date", extractField("Release date:\\s*(\\d{4})", content, "Unknown Date"));
        metadata.put("language", extractField("Language:\\s*(.+)", content, "Unknown Language"));
        metadata.put("credits", extractField("Credits:\\s*(.+)", content, "Unknown Credits"));
        metadata.put("ebook_number", extractField("eBook (#\\d+)", content, "Unknown eBook Number"));
        return metadata;
    }
    private static String extractField(String regex, String content, String defaultValue) {
        Matcher matcher = Pattern.compile(regex, Pattern.MULTILINE).matcher(content);
        return matcher.find() ? matcher.group(1).trim() : defaultValue;
    }
}

