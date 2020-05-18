package spring2020.mcon364.emailScraper;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Scraper {
    private Connection connection;
    private Set<String> emails;
    private HashSet<String> visited;
    private LinkedBlockingQueue<String> urls;
    private final int MAX_EMAILS = 5;

    public Scraper() throws IOException, SQLException {
        final int MAX_THREADS = 75;

        connection = DriverManager.getConnection(connectionString());
        ExecutorService pool = Executors.newFixedThreadPool(MAX_THREADS);

        urls = new LinkedBlockingQueue<>((int) (MAX_EMAILS * 1.25));
        urls.add("https://www.touro.edu/");
        visited = new HashSet<>();
        emails = Collections.synchronizedSet(new HashSet<>(MAX_EMAILS));
        while (emails.size() < MAX_EMAILS && !urls.isEmpty()) {
            pool.execute(new ScraperWorker(urls.poll()));
        }
        pool.shutdownNow();
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
        // TODO: upload data to db
        System.out.println(emails);
        System.out.println(visited);
    }

    class ScraperWorker implements Runnable {
        private final String URL;

        ScraperWorker(String url) {
            URL = url;
        }

        @Override
        public void run() {
            try {
                Document doc = Jsoup.connect("https://touro.edu").get();
                Elements links = doc.select("a");
                links.eachAttr("abs:href").forEach(System.out::println);
                links.eachAttr("abs:href").forEach(a -> {
                    System.out.println(a);
                    if (!visited.contains(a)) {
                        urls.add(a);
                    }
                });
                Matcher emailMatcher = Pattern.compile("[A-Za-z][A-Za-z.-_%+]{0,63}@" +
                        "(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,62}[A-Za-z0-9])?\\.)" +
                        "{1,8}[A-Za-z]{2,63}").matcher(doc.toString());
                while (emailMatcher.find()) {
                    System.out.println(emailMatcher.group());
                    emails.add(emailMatcher.group());
                }
                visited.add(URL);
                if (emails.size() >= MAX_EMAILS) {
                    upload();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
