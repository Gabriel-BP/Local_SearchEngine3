package es.ulpgc.client;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Scanner;

public class CLIHandler {
    private static final String BASE_URL = "http://localhost:8080/queryengine";

    public static void handleStats(HttpClient httpClient, Scanner scanner) {
        System.out.print("Enter the type of statistics (word_count, doc_count, top_words): ");
        String type = scanner.nextLine();

        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(BASE_URL + "/stats/" + type))
                    .GET()
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                System.out.println("Response:");
                System.out.println("-----------------------------");
                System.out.println(response.body());
                System.out.println("-----------------------------");
            } else {
                System.out.println("Error: " + response.statusCode());
                System.out.println(response.body());
            }
        } catch (Exception e) {
            System.err.println("Error getting statistics: " + e.getMessage());
        }
    }

    public static void handleDocuments(HttpClient httpClient, Scanner scanner) {
        System.out.print("Enter the words separated by '+': ");
        String words = scanner.nextLine();

        System.out.print("Enter the parameter 'from' (optional): ");
        String from = scanner.nextLine();

        System.out.print("Enter the parameter 'to' (optional): ");
        String to = scanner.nextLine();

        System.out.print("Enter the parameter 'author' (optional): ");
        String author = scanner.nextLine();

        StringBuilder urlBuilder = new StringBuilder(BASE_URL + "/documents/" + words + "?");
        if (!from.isBlank()) urlBuilder.append("from=").append(from).append("&");
        if (!to.isBlank()) urlBuilder.append("to=").append(to).append("&");
        if (!author.isBlank()) urlBuilder.append("author=").append(author).append("&");

        String url = urlBuilder.toString().replaceAll("&$", "");

        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .GET()
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                System.out.println("Response:");
                System.out.println("-----------------------------");
                System.out.println(response.body());
                System.out.println("-----------------------------");
            } else {
                System.out.println("Error: " + response.statusCode());
                System.out.println(response.body());
            }
        } catch (Exception e) {
            System.err.println("Error  searching for documents: " + e.getMessage());
        }
    }
}
