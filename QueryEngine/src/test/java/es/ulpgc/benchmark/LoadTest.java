package es.ulpgc.benchmark;

import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class LoadTest {

    private static final String BASE_URL = "http://localhost:8080/queryengine/documents/govern";
    private static final int NUM_THREADS = 3000; // Número de usuarios concurrentes
    private static final int NUM_REQUESTS_PER_THREAD = 10; // Solicitudes por usuario

    public static void main(String[] args) throws InterruptedException {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);

            for (int i = 0; i < NUM_THREADS; i++) {
                executor.submit(() -> {
                    for (int j = 0; j < NUM_REQUESTS_PER_THREAD; j++) {
                        performRequest(httpClient);
                    }
                });
            }

            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.HOURS);

            System.out.println("Test de carga completado.");
        } catch (IOException e) {
            System.err.println("Error al crear el cliente HTTP: " + e.getMessage());
        }
    }

    private static void performRequest(CloseableHttpClient httpClient) {
        HttpGet request = new HttpGet(BASE_URL);
        request.addHeader("Content-Type", "application/json");

        try (CloseableHttpResponse response = httpClient.execute(request)) {
            String responseBody = EntityUtils.toString(response.getEntity());
            System.out.println("Response: " + responseBody);
        } catch (IOException | ParseException e) {
            System.err.println("Error al ejecutar la solicitud: " + e.getMessage());
        }
    }
}
