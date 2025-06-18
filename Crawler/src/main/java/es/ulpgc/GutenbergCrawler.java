package es.ulpgc;

public class GutenbergCrawler {
    private static final String DOWNLOAD_FOLDER = "datalake";
    private static final String webID = "01";

    public void crawlBooks(int startBookId, int numBooks) {
        String date = FileManager.getCurrentDate();
        String folderPath = DOWNLOAD_FOLDER + "/" + date;
        if (FileManager.getFoldersInPath().stream().noneMatch(folder -> folder.equals(date))) {
            FileManager.createFolder(folderPath);
        }

        for (int i = startBookId; i < startBookId + numBooks; i++) {
            boolean success = GutenbergDownloader.downloadBook(String.valueOf(i), folderPath, webID);
            if (!success) {
                System.out.println("Skipping book ID " + i);
            }
        }
    }

}

