package org.example.simpledb;

import lombok.Data;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.Map;
import java.util.concurrent.*;

@Data
public class SimpleDb {

    private final String url;
    private final String username;
    private final String password;
    private boolean devMode;

    private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final Map<Connection, Long> connectionTimestamps = new ConcurrentHashMap<>();

    private final int MAX_POOL_SIZE=1;

    private final int CONNECTION_TIME_OUT=10;

    private BlockingQueue<Connection> connectionPool;

    private ThreadLocal<Connection> threadLocalConnection = new ThreadLocal<>();

    public SimpleDb(String host, String username, String password, String dbName) {
        this.url = "jdbc:mysql://" + host + ":3306/" + dbName;
        this.username = username;
        this.password = password;

        initializeConnectionPool();
        startConnectionTimeoutCheck();
    }

    private void startConnectionTimeoutCheck() {
        scheduler.scheduleAtFixedRate(() -> {
            long currentTime = System.currentTimeMillis();
            for (Map.Entry<Connection, Long> entry : connectionTimestamps.entrySet()) {
                Connection connection = entry.getKey();
                long lastUsedTime = entry.getValue();

                if (currentTime - lastUsedTime > 30000) { // 30초 경과 체크
                    releaseConnection(connection);
                }
            }
        }, 0, CONNECTION_TIME_OUT, TimeUnit.SECONDS); // 10초마다 커넥션 사용 시간 확인
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
            startConnectionTimeoutCheck(); // 주기적으로 커넥션 사용 시간 확인 시작
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public int getAvailableConnectionCount() {
        return connectionPool.size();
    }

    public void startTransaction(Connection conn) throws SQLException {
        System.out.println("== 트랜잭션 시작 ==");
        conn.setAutoCommit(false);
    }

    public void commitTransaction(Connection conn) throws SQLException {
        System.out.println("== 트랜잭션 커밋 ==");
        conn.commit();
    }

    public void rollbackTransaction(Connection conn) {
        try {
            System.out.println("== 트랜잭션 롤백 ==");
            conn.rollback();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void endTransaction(Connection conn) {
        try {
            System.out.println("== 트랜잭션 종료 ==");
            conn.setAutoCommit(true);
        } catch (SQLException e) {
            e.printStackTrace();
        }
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
        connectionTimestamps.put(connection, System.currentTimeMillis()); // 사용 시간 갱신
        return connection;
    }

    public void releaseConnection(Connection connection) {
        if (connection != null) {
            connectionPool.offer(connection);
            threadLocalConnection.remove();
            connectionTimestamps.remove(connection); // 사용 시간 정보 제거
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
        return new Sql();
    }


    public void generateDDL(Class<?> clazz) throws SQLException {
        Connection conn = getConnection();
        StringBuilder ddl = new StringBuilder("CREATE TABLE ");
        ddl.append(clazz.getSimpleName().toLowerCase()).append(" (\n");

        Field[] fields = clazz.getDeclaredFields();
        for (Field field : fields) {
            Column column = field.getAnnotation(Column.class);

            if (column != null) {
                ddl.append(field.getName()).append(" ").append(column.type());

                if (!column.nullable()) {
                    ddl.append(" NOT NULL");
                }

                if (!column.defaultValue().isEmpty()) {
                    ddl.append(" DEFAULT ").append(column.defaultValue());
                }
                ddl.append(",\n");
            }
        }

        // 마지막 콤마 제거 및 괄호 닫기
        ddl.setLength(ddl.length() - 2);
        ddl.append("\n)");
        System.out.println(ddl);

        run(ddl.toString());
    }



}
