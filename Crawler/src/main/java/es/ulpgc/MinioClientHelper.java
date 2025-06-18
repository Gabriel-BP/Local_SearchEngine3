package es.ulpgc;

import io.minio.*;

public class MinioClientHelper {
    private static final String ENDPOINT = "http://192.168.16.1:9000"; // IP del equipo con MinIO
    private static final String ACCESS_KEY = "admin";
    private static final String SECRET_KEY = "admin123";
    private static final String BUCKET = "datalake";

    private static final MinioClient client = MinioClient.builder()
            .endpoint(ENDPOINT)
            .credentials(ACCESS_KEY, SECRET_KEY)
            .build();

    public static void uploadFile(String localPath, String remotePath) throws Exception {
        client.uploadObject(
                UploadObjectArgs.builder()
                        .bucket(BUCKET)
                        .object(remotePath)
                        .filename(localPath)
                        .build()
        );
        System.out.println("Uploaded " + localPath + " to MinIO as " + remotePath);
    }
}
