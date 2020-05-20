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
    private final long START_TIME;

    public Scraper(int maxEmails, int maxThreads) {
        START_TIME = System.currentTimeMillis();
        MAX_EMAILS = maxEmails;
        ExecutorService pool = Executors.newFixedThreadPool(maxThreads);
        URLS = new LinkedBlockingQueue<>();
        URLS.add("https://www.touro.edu/");
        VISITED = Collections.synchronizedSet(new HashSet<>((int) (MAX_EMAILS * 1.25)));
        EMAILS = Collections.synchronizedSet(new HashSet<>(MAX_EMAILS));
        while (EMAILS.size() < MAX_EMAILS) {
            String url = URLS.poll();
            if (url != null) {
                VISITED.add(url);
                pool.execute(new ScraperWorker(url));
            }
        }
        pool.shutdownNow();
        upload();
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
            Connection connection = DriverManager.getConnection(connectionString());
            query("Emails", connection, new ArrayList<>(EMAILS));
            query("URLs", connection, new ArrayList<>(VISITED));

            final long END_TIME = System.currentTimeMillis();
            System.out.println("Successfully uploaded results.\nElapsed time: " + (END_TIME - START_TIME) + "ms.");
        } catch (SQLException throwables) {
            System.out.println("Could not complete query:");
            throwables.printStackTrace();
        } catch (IOException e) {
            System.out.println("Could not retrieve database secrets:");
            e.printStackTrace();
        }
    }

    private void query(String table, Connection connection, ArrayList<String> data) throws SQLException {
        PreparedStatement stmt = connection.prepareStatement("INSERT INTO " + table + " VALUES (?)");
        for (String e : data) {
            stmt.setString(1, e);
            stmt.addBatch();
        }
        stmt.executeBatch();
        stmt.closeOnCompletion();
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
                    a = trimURL(a);
                    if (!VISITED.contains(a) && (a.contains("https://") || a.contains("http://"))) {
                        URLS.add(a);
                    }
                });
                //Emails:
                Matcher emailMatcher = Pattern.compile("[A-Za-z][A-Za-z.\\-_%+]{0,63}@" +
                        "(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,62}[A-Za-z0-9])?\\.){1,8}" +
                        "[A-Za-z]{2,63}").matcher(doc.toString()); //TODO: exclude files which contain @ in their name
                while (emailMatcher.find()) {
                    if (EMAILS.size() < MAX_EMAILS) {
                        EMAILS.add(emailMatcher.group());
                    }
                }
            } catch (Exception ignored) {}
        }

        private String trimURL(String url) {
            int i = url.indexOf('#');
            if (i >= 0) {
                url = url.substring(0, i);
            }
            if (!url.contains("?") && url.charAt(url.length() - 1) != '/') {
                url = url + '/';
            }
            return url;
        }
    }
}
