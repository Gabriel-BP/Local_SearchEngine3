package es.ulpgc.data;

import io.minio.*;
import io.minio.messages.Item;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class MinioClientHelper {
    private static final String ENDPOINT = "http://host.docker.internal:9000";
    private static final String ACCESS_KEY = "admin";
    private static final String SECRET_KEY = "admin123";
    private static final String BUCKET = "datalake";

    private static final MinioClient client = MinioClient.builder()
            .endpoint(ENDPOINT)
            .credentials(ACCESS_KEY, SECRET_KEY)
            .build();

    public static void downloadFile(String remotePath, String localPath) throws Exception {
        // Evita descargar archivos incompletos .part.minio
        if (remotePath.endsWith(".part.minio")) {
            System.out.println("[WARN] Skipping temporary MinIO file: " + remotePath);
            return;
        }

        Path localFile = Paths.get(localPath);
        if (Files.exists(localFile)) {
            System.out.println("[INFO] Skipping download (already exists): " + localPath);
            return;
        }

        client.downloadObject(
                DownloadObjectArgs.builder()
                        .bucket(BUCKET)
                        .object(remotePath)
                        .filename(localPath)
                        .build()
        );
        System.out.println("Downloaded from MinIO: " + remotePath);
    }


    public static void uploadFile(String localPath, String remotePath) throws Exception {
        client.uploadObject(
                UploadObjectArgs.builder()
                        .bucket(BUCKET)
                        .object(remotePath)
                        .filename(localPath)
                        .build()
        );
        System.out.println("Uploaded to MinIO: " + remotePath);
    }

    public static List<String> listFilesWithPrefix(String prefix) throws Exception {
        List<String> results = new ArrayList<>();
        Iterable<Result<Item>> objects = client.listObjects(
                ListObjectsArgs.builder()
                        .bucket(BUCKET)
                        .prefix(prefix)
                        .recursive(true) // Habilita búsqueda en subdirectorios
                        .build()
        );
        for (Result<Item> r : objects) {
            results.add(r.get().objectName());
        }
        return results;
    }
}
