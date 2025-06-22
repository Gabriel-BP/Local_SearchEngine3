package es.ulpgc.data;

import io.minio.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class MinioClientHelper {
    private static final String ENDPOINT = "http://192.168.0.56:9000";
    private static final String ACCESS_KEY = "admin";
    private static final String SECRET_KEY = "admin123";
    private static final String BUCKET = "datalake";

    private static final MinioClient client = MinioClient.builder()
            .endpoint(ENDPOINT)
            .credentials(ACCESS_KEY, SECRET_KEY)
            .build();

    public static void downloadFile(String remotePath, String localPath) throws Exception {
        // Siempre descargamos el archivo, sin importar si existe
        Path localFile = Paths.get(localPath);

        // Si el archivo ya existe, lo eliminamos antes de volver a descargarlo
        if (Files.exists(localFile)) {
            System.out.println("[INFO] Deleting existing file: " + localPath);
            Files.delete(localFile); // Eliminar el archivo existente
        }

        // Descargar el archivo de MinIO
        client.downloadObject(
                DownloadObjectArgs.builder()
                        .bucket(BUCKET)
                        .object(remotePath)
                        .filename(localPath)
                        .build()
        );
        System.out.println("[INFO] Downloaded from MinIO: " + remotePath);
    }
}
