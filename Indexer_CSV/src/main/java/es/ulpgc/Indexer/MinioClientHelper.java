package es.ulpgc.Indexer;

import io.minio.*;
import io.minio.messages.Item;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class MinioClientHelper {
    private static final String ENDPOINT = "http://192.168.0.56:9000";
    private static final String ACCESS_KEY = "admin";
    private static final String SECRET_KEY = "admin123";
    private static final String BUCKET = "datalake";

    private static final MinioClient client = MinioClient.builder()
            .endpoint(ENDPOINT)
            .credentials(ACCESS_KEY, SECRET_KEY)
            .build();

    static {
        try {
            boolean exists = client.bucketExists(BucketExistsArgs.builder().bucket(BUCKET).build());
            if (!exists) {
                client.makeBucket(MakeBucketArgs.builder().bucket(BUCKET).build());
                System.out.println("[INFO] Bucket '" + BUCKET + "' creado en MinIO.");
            } else {
                System.out.println("[INFO] Bucket '" + BUCKET + "' ya existe.");
            }
        } catch (Exception e) {
            System.err.println("[ERROR] Error comprobando o creando el bucket en MinIO: " + e.getMessage());
        }
    }

    public static void downloadFile(String remotePath, String localPath) throws Exception {
        client.downloadObject(
                DownloadObjectArgs.builder()
                        .bucket(BUCKET)
                        .object(remotePath)
                        .filename(localPath)
                        .build()
        );
        System.out.println("[INFO] Downloaded from MinIO: " + remotePath);
    }

    public static void uploadFile(String localPath, String remotePath) throws Exception {
        File file = new File(localPath);
        if (!file.exists()) {
            throw new IllegalArgumentException("El archivo local no existe: " + localPath);
        }

        try {
            client.uploadObject(
                    UploadObjectArgs.builder()
                            .bucket(BUCKET)
                            .object(remotePath)
                            .filename(localPath)
                            .build()
            );
            System.out.println("[INFO] Uploaded to MinIO: " + remotePath);
        } catch (Exception e) {
            System.err.println("[ERROR] Fallo al subir a MinIO: " + remotePath + " -> " + e.getMessage());
            throw e;
        }
    }

    public static List<String> listFilesWithPrefix(String prefix) throws Exception {
        List<String> results = new ArrayList<>();
        Iterable<Result<Item>> objects = client.listObjects(
                ListObjectsArgs.builder()
                        .bucket(BUCKET)
                        .prefix(prefix)
                        .recursive(true)
                        .build()
        );
        for (Result<Item> r : objects) {
            results.add(r.get().objectName());
        }
        return results;
    }
}
