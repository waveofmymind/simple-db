package org.example.simpledb;

import jdk.jfr.DataAmount;
import lombok.Data;

import java.sql.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Data
public class SimpleDb {

    private final String url;
    private final String username;
    private final String password;
    private boolean devMode;

    private final int MAX_POOL_SIZE = 2;

    private BlockingQueue<Connection> connectionPool;

    private ThreadLocal<Connection> threadLocalConnection = new ThreadLocal<>();

    public SimpleDb(String host, String username, String password, String dbName) {
        this.url = "jdbc:mysql://" + host + ":3306/" + dbName;
        this.username = username;
        this.password = password;

        initializeConnectionPool();
    }

    public void setDevMod(boolean devMode) {
        this.devMode = devMode;
    }

    private void initializeConnectionPool() {
        try {
            connectionPool = new LinkedBlockingQueue<>(MAX_POOL_SIZE);
            for (int i = 0; i < MAX_POOL_SIZE; i++) {
                connectionPool.offer(createConnection());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public int getAvailableConnectionCount() {
        return connectionPool.size();
    }

    private Connection createConnection() throws SQLException {
        return DriverManager.getConnection(url, username, password);
    }

    public Connection getConnection() throws SQLException {
        Connection connection = threadLocalConnection.get();
        if (connection == null) {
            try {
                connection = connectionPool.take();
                threadLocalConnection.set(connection);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return connection;
    }

    public void releaseConnection(Connection connection) {
        if (connection != null) {
            connectionPool.offer(connection);
            threadLocalConnection.remove();
        }
    }


    public void run(String sql) {
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = getConnection();
            stmt = conn.createStatement();

            if (devMode) {
                System.out.println(sql);
            }

            stmt.executeUpdate(sql);

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            releaseConnection(conn);
        }
    }

    public void run(String sql, Object... params) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        try {
            conn = getConnection();
            pstmt = conn.prepareStatement(sql);

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
            if (pstmt != null) {
                try {
                    pstmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            releaseConnection(conn);
        }
    }
    public Sql genSql() throws SQLException {
        return new Sql(this);
    }




}
