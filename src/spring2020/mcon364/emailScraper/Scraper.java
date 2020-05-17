package spring2020.mcon364.emailScraper;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class Scraper {
    Connection connection;

    public Scraper() {
        try {
            connection = DriverManager.getConnection(connectionString());
        } catch (SQLException | IOException throwables) {
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
}
