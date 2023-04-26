package org.example.simpledb;

import jdk.jfr.DataAmount;
import lombok.Data;

import java.sql.*;

@Data
public class SimpleDb {

    private final String url;
    private final String username;
    private final String password;
    private boolean devMode;

    public SimpleDb(String host, String username, String password, String dbName) {
        this.url = "jdbc:mysql://" + host + ":3306/" + dbName;
        this.username = username;
        this.password = password;
    }

    public void setDevMod(boolean devMode) {
        this.devMode = devMode;
    }

    public void run(String sql) {
        try (Connection conn = DriverManager.getConnection(url, username, password);
             Statement stmt = conn.createStatement()) {

            if (devMode) {
                System.out.println(sql);
            }

            stmt.executeUpdate(sql);

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void run(String sql, Object... params) {
        try (Connection conn = DriverManager.getConnection(url, username, password);
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            for (int i = 0; i < params.length; i++) {
                stmt.setObject(i + 1, params[i]);
            }

            stmt.executeUpdate();

        } catch (SQLException e) {
            if (devMode) {
                e.printStackTrace();
            }
        }
    }

    public Sql genSql() {
        return new Sql(this);
    }


}
