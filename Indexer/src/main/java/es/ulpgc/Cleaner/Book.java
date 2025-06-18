package es.ulpgc.Cleaner;

import java.util.List;

public class Book {
    public String title;
    public String author;
    public String date;
    public String language;
    public String credits;
    public String ebookNumber;
    public List<String> words;
    public String fullContent;

    public Book(String title, String author, String date, String language, String credits, String ebookNumber, List<String> words, String fullContent) {
        this.title = title;
        this.author = author;
        this.date = date;
        this.language = language;
        this.credits = credits;
        this.ebookNumber = ebookNumber;
        this.words = words;
        this.fullContent = fullContent;
    }
}
