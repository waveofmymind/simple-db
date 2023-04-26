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

    private ThreadLocal<Connection> threadLocalConnection = new ThreadLocal<>();

    public SimpleDb(String host, String username, String password, String dbName) {
        this.url = "jdbc:mysql://" + host + ":3306/" + dbName;
        this.username = username;
        this.password = password;
    }

    public void setDevMod(boolean devMode) {
        this.devMode = devMode;
    }

    public Connection getConnection() throws SQLException {
        Connection conn = threadLocalConnection.get();

        if (conn == null) {
            conn = DriverManager.getConnection(url, username, password);
            threadLocalConnection.set(conn);
        }

        return conn;
    }

    public void closeConnection() {
        Connection conn = threadLocalConnection.get();
        if (conn != null) {
            try {
                conn.close();
                threadLocalConnection.remove();
            } catch (SQLException e) {
                e.printStackTrace();

            }
        }
    }

    public void run(String sql) {
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {

            if (devMode) {
                System.out.println(sql);
            }

            stmt.executeUpdate(sql);

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeConnection();
        }
    }

    public void run(String sql, Object... params) {
        try (Connection conn = getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            for (int i = 0; i < params.length; i++) {
                pstmt.setObject(i + 1, params[i]);
            }

            if (devMode) {
                System.out.println(sql);
            }

            pstmt.executeUpdate();

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeConnection();
        }
    }

    public Sql genSql() {
        return new Sql(this);
    }

    public void startTransaction() {
        try (Connection conn = getConnection()) {
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeConnection();
        }
    }

    public void commit() {
        try (Connection conn = getConnection()) {
            conn.commit();
            conn.setAutoCommit(true);
        } catch (SQLException e) {
            e.printStackTrace();

        } finally {
            closeConnection();
        }
    }

    public void rollback() {
        try (Connection conn = getConnection()) {
            conn.rollback();
            conn.setAutoCommit(true);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeConnection();
        }
    }


}
