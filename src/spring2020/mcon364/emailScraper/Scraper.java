package spring2020.mcon364.emailScraper;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Scraper {
    private final Set<String> EMAILS, VISITED;
    private final ThreadPoolExecutor POOL;
    private final int MAX_EMAILS, MAX_THREADS, MAX_URLS;
    private Connection dbConnection = null;
    private boolean completed = false;
    private final long START_TIME;
    private int numCommitted = 0;

    public Scraper(int maxEmails, int maxThreads) {
        START_TIME = System.currentTimeMillis();
        MAX_EMAILS = maxEmails;
        MAX_THREADS = maxThreads;
        MAX_URLS = (int) (MAX_THREADS * 1.5);
        POOL = (ThreadPoolExecutor) Executors.newFixedThreadPool(MAX_THREADS);
        VISITED = Collections.synchronizedSet(new HashSet<>(MAX_EMAILS * 5));
        EMAILS = Collections.synchronizedSet(new HashSet<>(MAX_EMAILS));
        POOL.execute(new ScraperWorker("https://www.touro.edu/"));
    }

    private void finish() {
        if (completed) return;
        completed = true;
        POOL.shutdownNow();
        double executionTime = (System.currentTimeMillis() - START_TIME) / 1000.0;
        System.out.println("Completed crawl.\nElapsed time: " + executionTime);
        //When done crawling:
        try {
            dbConnection = DriverManager.getConnection(connectionString());
            upload();
            log(executionTime);
            executionTime = (System.currentTimeMillis() - START_TIME) / 1000.0;
            System.out.println("Completed upload.\nTotal elapsed time: "
                    + executionTime + "s.\nURLs visited: " + VISITED.size());
        } catch (SQLException e) {
            System.out.println("Could not connect to database:");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("Could not retrieve database secrets:");
            e.printStackTrace();
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

    private void upload() {
        try {
            PreparedStatement stmt = dbConnection.prepareStatement(
                    "INSERT INTO Emails VALUES (?)"
            );
            for (String e : EMAILS) {
                stmt.setString(1, e);
                stmt.addBatch();
            }
            stmt.executeBatch();
        } catch (SQLException e) {
            System.out.println("Could not complete query:");
            e.printStackTrace();
        }
    }

    private void log(double executionTime) {
        try {
            PreparedStatement logQuery = dbConnection.prepareStatement(
                    "INSERT INTO Log VALUES(SYSDATETIME(), ?, ?, ?, ?)"
            );
            logQuery.setInt(1, MAX_EMAILS);
            logQuery.setInt(2, MAX_THREADS);
            logQuery.setDouble(3, executionTime);
            logQuery.setInt(4, VISITED.size());
            logQuery.execute();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    class ScraperWorker implements Runnable {
        private final String URL;

        ScraperWorker(String url) {
            URL = url;
            VISITED.add(URL);
            numCommitted++;
        }

        @Override
        public void run() {
            if (EMAILS.size() >= MAX_EMAILS) {
                finish();
                return;
            }
            try {
                Document doc = Jsoup.connect(URL).get();
                //URLs:
                if (numCommitted <= MAX_URLS * 2) {
                    doc.select("a[href]").eachAttr("abs:href").forEach(a -> {
                        a = trimURL(a);
                        if (!VISITED.contains(a) &&
                                /* Exclude links that are not webpages: */
                                (a.contains("https://") || a.contains("http://"))) {
                            POOL.execute(new ScraperWorker(a));
                        }
                    });
                }
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
            } catch (Exception ignored) {} finally {
                numCommitted--;
            }
        }

        private String filterEmail(String email) {
            final String[] FILE_TYPES = new String[] {"png", "jpg", "gif", "pdf", "mp3", "css",
                    "mp4", "mov", "7z", "zip", "mkv", "avi", "jpeg"};
            String lc = email.substring(email.indexOf('@')).toLowerCase();
            for (String t: FILE_TYPES) {
                if (lc.contains('.' + t)) {
                    email = null;
                    break;
                }
            }
            return email;
        }

        private String trimURL(String url) {
            int i = url.indexOf('#');
            if (i >= 12) {
                url = url.substring(0, i);
            }
            if (!url.contains("?") && url.charAt(url.length() - 1) != '/') {
                url = url + '/';
            }
            return url;
        }
    }
}
