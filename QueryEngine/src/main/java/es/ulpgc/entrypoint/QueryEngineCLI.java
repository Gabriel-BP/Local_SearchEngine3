package es.ulpgc.entrypoint;

import es.ulpgc.client.CLIHandler;

import java.net.http.HttpClient;
import java.util.Scanner;

public class QueryEngineCLI {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        HttpClient httpClient = HttpClient.newHttpClient();

        System.out.println("Welcome to the Query Engine CLI");
        while (true) {
            System.out.println("\nOptions:");
            System.out.println("1. Get statistics (/stats/:type)");
            System.out.println("2. Search Documents (/documents/:words?params)");
            System.out.println("3. Exit");
            System.out.print("Choose an Option: ");
            String option = scanner.nextLine();

            switch (option) {
                case "1":
                    CLIHandler.handleStats(httpClient, scanner);
                    break;
                case "2":
                    CLIHandler.handleDocuments(httpClient, scanner);
                    break;
                case "3":
                    System.out.println("Leaving...");
                    return;
                default:
                    System.out.println("Invalid option. Try again.");
            }
        }
    }
}

