package spring2020.mcon364.emailScraper;

public class Main {
    public static void main(String[] args) {
        new Scraper(Integer.parseInt(args[0]), Integer.parseInt(args[1]));
//        new Scraper(10_000, 750);
    }
}
