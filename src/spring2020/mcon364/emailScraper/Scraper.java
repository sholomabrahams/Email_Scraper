package spring2020.mcon364.emailScraper;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Scraper {
    private final Set<String> EMAILS, VISITED;
    private final LinkedBlockingQueue<String> URLS;
    private final int MAX_EMAILS;
    private final Connection CONNECTION;

    public Scraper(int maxEmails, int maxThreads, int uploadThreshold) throws IOException, SQLException {
        long START_TIME = System.currentTimeMillis();
        MAX_EMAILS = maxEmails;
        CONNECTION = DriverManager.getConnection(connectionString());
        ExecutorService pool = Executors.newFixedThreadPool(maxThreads);
        URLS = new LinkedBlockingQueue<>();
        URLS.add("https://www.touro.edu/");
        VISITED = Collections.synchronizedSet(new HashSet<>((int) (MAX_EMAILS * 1.25)));
        EMAILS = Collections.synchronizedSet(new HashSet<>(MAX_EMAILS));
        crawl(pool, uploadThreshold);
        pool.shutdownNow();
        if (!EMAILS.isEmpty()) {
            new Thread(new Uploader(new ArrayList<>(EMAILS))).start();
        }
        double executionTime = (System.currentTimeMillis() - START_TIME) / 1000.0;
        log(maxThreads, uploadThreshold, executionTime);
        System.out.println("Completed crawl and upload.\nElapsed time: "
                + executionTime + "s.");
    }

    private void log(int maxThreads, int uploadThreshold, double executionTime) {
        try {
            PreparedStatement logQuery = CONNECTION.prepareStatement("INSERT INTO Log VALUES(SYSDATETIME(), ?, ?, ?, ?)");
            logQuery.setInt(1, MAX_EMAILS);
            logQuery.setInt(2, maxThreads);
            logQuery.setInt(3, uploadThreshold);
            logQuery.setDouble(4, executionTime);
            logQuery.execute();
            logQuery.closeOnCompletion();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    private String connectionString() throws IOException {
        FileInputStream in = new FileInputStream("src/secrets.txt");
        Properties props = new Properties();
        props.load(in);
        return props.getProperty("url") +
                ";databaseName=" + props.getProperty("database") +
                ";user=" + props.getProperty("username") +
                ";password=" + props.getProperty("password");
    }

    private void crawl(ExecutorService pool, int uploadThreshold) {
        int uploadCount = 0;
        String url;
        while (EMAILS.size() + uploadCount < MAX_EMAILS) {
            url = URLS.poll();
            if (url != null) {
                VISITED.add(url);
                pool.execute(new ScraperWorker(url));
            }
            if (EMAILS.size() > uploadThreshold) {
                synchronized (EMAILS) {
                    uploadCount += EMAILS.size();
                    pool.execute(new Uploader(new ArrayList<>(EMAILS)));
                    EMAILS.clear();
                    System.out.println(uploadCount);
                }
            }
        }
        System.out.println("Number of emails: " + (EMAILS.size() + uploadCount * uploadThreshold));
    }

    class ScraperWorker implements Runnable {
        private final String URL;

        ScraperWorker(String url) {
            URL = url;
        }

        @Override
        public void run() {
            try {
                Document doc = Jsoup.connect(URL).get();
                //URLs:
                doc.select("a[href]").eachAttr("abs:href").forEach(a -> {
                    if (!VISITED.contains(a) && /*Exclude links that are not webpages*/ (a.contains("https://") || a.contains("http://"))) {
                        URLS.add(a);
                    }
                });
                //Emails:
                Matcher emailMatcher = Pattern.compile("[A-Za-z][A-Za-z.\\-_%+]{0,63}@" +
                        "(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,62}[A-Za-z0-9])?\\.){1,8}" +
                        "[A-Za-z]{2,63}").matcher(doc.toString());
                while (emailMatcher.find()) {
                    String email = filterEmail(emailMatcher.group());
                    if (EMAILS.size() < MAX_EMAILS && email != null) {
                        EMAILS.add(email);
                    }
                }
            } catch (Exception ignored) {}
        }

        //Removes
        private String filterEmail(String email) {
            String[] fileTypes = new String[] {"png", "jpg", "gif", "pdf", "mp3", "mp4", "mov", "7z", "zip", "mkv", "avi", "jpeg"};
            String lc = email.substring(email.indexOf('@')).toLowerCase();
            for (String t: fileTypes) {
                if (lc.contains('.' + t)) {
                    email = null;
                    break;
                }
            }
            return email;
        }
    }

    private class Uploader implements Runnable {
        private final ArrayList<String> DATA;

        public Uploader(ArrayList<String> strings) {
            DATA = strings;
        }

        @Override
        public void run() {
            try {
                PreparedStatement stmt = CONNECTION.prepareStatement("INSERT INTO Emails VALUES (?)");
                for (String e : DATA) {
                    stmt.setString(1, e);
                    stmt.addBatch();
                }
                stmt.executeBatch();
                stmt.closeOnCompletion();
            } catch (SQLException e) {
                System.out.println("Data upload failed:");
                e.printStackTrace();
            }
        }
    }
}
