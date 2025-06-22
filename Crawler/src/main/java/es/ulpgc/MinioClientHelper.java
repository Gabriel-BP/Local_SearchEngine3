package es.ulpgc;

import io.minio.*;
import io.minio.messages.Item;

import java.io.File;
import java.util.*;

public class MinioClientHelper {
    private static final String ENDPOINT = "http://192.168.0.56:9000"; // IP del equipo con MinIO
    private static final String ACCESS_KEY = "admin";
    private static final String SECRET_KEY = "admin123";
    private static final String BUCKET = "datalake";

    private static final MinioClient client = MinioClient.builder()
            .endpoint(ENDPOINT)
            .credentials(ACCESS_KEY, SECRET_KEY)
            .build();

    public static void uploadFile(String localPath, String remotePath) throws Exception {
        try {
            boolean found = client.bucketExists(BucketExistsArgs.builder().bucket(BUCKET).build());
            if (!found) {
                client.makeBucket(MakeBucketArgs.builder().bucket(BUCKET).build());
                System.out.println("Bucket '" + BUCKET + "' creado.");
            }
        } catch (Exception e) {
            if (!e.getMessage().contains("BucketAlreadyOwnedByYou") &&
                    !e.getMessage().contains("BucketAlreadyExists")) {
                throw e;
            }
            System.out.println("El bucket ya existía, sin problema.");
        }

        client.uploadObject(
                UploadObjectArgs.builder()
                        .bucket(BUCKET)
                        .object(remotePath)
                        .filename(localPath)
                        .build()
        );
        System.out.println("Uploaded " + localPath + " to MinIO as " + remotePath);
    }

    public static int getLastDownloadedBookId() {
        try {
            // Obtener carpetas de fecha en /raw/
            List<String> folders = new ArrayList<>();
            Iterable<Result<Item>> objects = client.listObjects(
                    ListObjectsArgs.builder()
                            .bucket(BUCKET)
                            .prefix("raw/")
                            .recursive(true)
                            .build()
            );

            for (Result<Item> r : objects) {
                String name = r.get().objectName(); // raw/ddMMyyyy/01xxxx.txt
                String[] parts = name.split("/");
                if (parts.length == 3) {
                    folders.add(parts[1]);
                }
            }

            if (folders.isEmpty()) return 0;

            // Obtener la carpeta con fecha más reciente
            String latestDate = folders.stream()
                    .distinct().min(Comparator.reverseOrder())
                    .orElse(null);

            if (latestDate == null) return 0;

            // Obtener archivos en esa carpeta
            List<Integer> ids = new ArrayList<>();
            Iterable<Result<Item>> recentObjects = client.listObjects(
                    ListObjectsArgs.builder()
                            .bucket(BUCKET)
                            .prefix("raw/" + latestDate + "/")
                            .recursive(false)
                            .build()
            );

            for (Result<Item> r : recentObjects) {
                String name = new File(r.get().objectName()).getName(); // 01xxxx.txt
                try {
                    int id = Integer.parseInt(name.substring(2, name.lastIndexOf('.')));
                    ids.add(id);
                } catch (Exception ignored) {}
            }

            return ids.stream().max(Integer::compareTo).orElse(0);

        } catch (Exception e) {
            System.err.println("Error obteniendo último ID desde MinIO: " + e.getMessage());
            return 0;
        }
    }

}
