package spring2020.mcon364.emailScraper;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.sql.Connection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Scraper {
    private final Connection CONNECTION = null;
    private final Set<String> EMAILS;
    private final HashSet<String> VISITED;
    private final LinkedBlockingQueue<String> URLS;
    private final int MAX_EMAILS = 10;

    public Scraper() {
        final int MAX_THREADS = 75;

        //connection = DriverManager.getConnection(connectionString());
        ExecutorService pool = Executors.newFixedThreadPool(MAX_THREADS);

        URLS = new LinkedBlockingQueue<>();
        URLS.add("https://touro.edu/");
        VISITED = new HashSet<>();
        EMAILS = Collections.synchronizedSet(new HashSet<>(MAX_EMAILS));
        while (EMAILS.size() < MAX_EMAILS) {
            if (URLS.peek() != null) {
                System.out.println(EMAILS.size());
                pool.execute(new ScraperWorker(URLS.poll()));
            }
        }
        pool.shutdown();
        upload();
    }

//    private String connectionString() throws IOException {
//        FileInputStream in = new FileInputStream("src/secrets.txt");
//        Properties props = new Properties();
//        props.load(in);
//        return props.getProperty("url") +
//                ";databaseName=" + props.getProperty("database") +
//                ";user=" + props.getProperty("username") +
//                ";password=" + props.getProperty("password");
//    }

    private void upload() {
        // TODO: upload data to db
        System.out.println("Emails:  " + EMAILS);
        System.out.println("URLs:  " + VISITED);
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
                doc.select("a").eachAttr("abs:href").forEach(a -> {
                    if (!VISITED.contains(a) && a.indexOf('@') < 0 && !a.contains("tel:")) {
                        int i = a.indexOf('#');
                        if (i >= 0) {
                            a = a.substring(0, i);
                        }
                        if (!a.contains("?") && a.charAt(a.length() - 1) != '/') {
                            a = a + '/';
                        }
                        URLS.add(a);
                    }
                });
                /*
                 * This will return all ul elements with text matching
                 * with regex pattern "C[+]?" (Character C followed
                 * by zero or more + sign):
                 * Elements uls = document.select("ul:matches(C[+]?)");*/
                Matcher emailMatcher = Pattern.compile("[A-Za-z][A-Za-z.-_%+]{0,63}@" +
                        "(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,62}[A-Za-z0-9])?\\.)" +
                        "{1,8}[A-Za-z]{2,63}").matcher(doc.toString());
                //Matcher emailMatcher = Pattern.compile("").matcher(doc.toString());
                while (emailMatcher.find()) {
                    //System.out.println(emailMatcher.group());
                    if (EMAILS.size() < MAX_EMAILS) {
                        EMAILS.add(emailMatcher.group());
                    }
                }
                //EMAILS.add("sholom@" + URL);
                VISITED.add(URL);
                /*if (EMAILS.size() >= MAX_EMAILS) {
                    upload();
                }*/
            } catch (Exception e) {
                //e.printStackTrace();
            }
        }
    }
}
