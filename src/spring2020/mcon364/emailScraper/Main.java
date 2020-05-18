package spring2020.mcon364.emailScraper;

import java.io.IOException;
import java.sql.SQLException;

public class Main {
    public static void main(String[] args) {
        try {
            new Scraper();
        } catch (IOException | SQLException e) {
            e.printStackTrace();
        }
    }
}
