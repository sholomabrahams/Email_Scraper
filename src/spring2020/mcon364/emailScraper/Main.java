package spring2020.mcon364.emailScraper;

import java.io.IOException;
import java.sql.SQLException;

public class Main {
    public static void main(String[] args) {
        try {
//            new Scraper(500, 750, 500);
            new Scraper(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]));
        } catch (SQLException e) {
            System.out.println("Could not connect to Database:");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("Could not retrieve database secrets.");
            e.printStackTrace();
        }
    }
}
