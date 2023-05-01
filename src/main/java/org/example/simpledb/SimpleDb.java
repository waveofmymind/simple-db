package org.example.simpledb;

import lombok.Data;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

    private final int MAX_POOL_SIZE = 1;

    private final int CONNECTION_TIME_OUT = 10;

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
                    releaseExpiredConnection(connection);
                }
            }
        }, 0, CONNECTION_TIME_OUT, TimeUnit.SECONDS); // 10초마다 커넥션 사용 시간 확인
    }

    public void releaseExpiredConnection(Connection conn) {
        if (conn != null) {
            connectionPool.offer(conn);
            threadLocalConnection.remove();
            connectionTimestamps.remove(conn); // 사용 시간 정보 제거
        }
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
        threadLocalConnection.set(conn);
    }

    private PreparedStatement prepareStatement(String sql, Object... parameters) throws SQLException {
        PreparedStatement pstmt = getConnection().prepareStatement(sql);
        for (int i = 0; i < parameters.length; i++) {
            pstmt.setObject(i + 1, parameters[i]);
        }
        return pstmt;
    }

    public void commitTransaction(Connection conn) {
        System.out.println(conn);

        if (conn != null) {
            try {
                System.out.println("== 트랜잭션 커밋 ==");
                conn.commit();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }

    public void rollbackTransaction(Connection conn) {
        System.out.println(conn);

        if (conn != null) {
            try {
                conn.rollback();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void endTransaction(Connection conn) {
        System.out.println(conn);
        if (conn != null) {
            try {
                conn.setAutoCommit(true);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private Connection createConnection() throws SQLException {
        return DriverManager.getConnection(url, username, password);
    }

    public Connection getConnection() throws SQLException {
        Connection connection = threadLocalConnection.get();
        if (connection == null) {
            try {
                connection = connectionPool.take(); // 커넥션 풀에서 커넥션을 가져옴
                threadLocalConnection.set(connection);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        connectionTimestamps.put(connection, System.currentTimeMillis()); // 사용 시간 갱신
        return connection;
    }

    public void releaseConnection(Connection conn) {
        if (conn != null) {
            connectionPool.offer(conn);
            threadLocalConnection.remove();
            connectionTimestamps.remove(conn); // 사용 시간 정보 제거
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


    public void generateDDL(Class<?> clazz) {
        String tableName = clazz.getSimpleName().toLowerCase();

        // 테이블 삭제 (존재하는 경우)
        run("DROP TABLE IF EXISTS " + tableName);

        // 테이블 생성 쿼리 구성
        StringBuilder ddl = new StringBuilder("CREATE TABLE ");
        ddl.append(tableName).append(" (\n");

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

                // id 필드인 경우 PRIMARY KEY 추가
                if (field.getName().equals("id")) {
                    ddl.append(", PRIMARY KEY(id)");
                }

                ddl.append(",\n");
            }
        }

        // 마지막 콤마 제거 및 괄호 닫기
        ddl.setLength(ddl.length() - 2);
        ddl.append("\n)");

        // 테이블 생성
        run(ddl.toString());
    }


    public long executeQueryWithGeneratedKeys(String sql, Object[] parameters) throws SQLException {
        Connection conn = getConnection();
        try (PreparedStatement pstmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            for (int i = 0; i < parameters.length; i++) {
                pstmt.setObject(i + 1, parameters[i]);
            }
            pstmt.executeUpdate();
            try (ResultSet rs = pstmt.getGeneratedKeys()) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
            }
            System.out.println(conn);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                releaseConnection(conn);
            }
        }
        return -1;
    }

    public long executeQuery(String sql, Object[] parameters) throws SQLException {

        Connection conn = getConnection();

        try (PreparedStatement pstmt = prepareStatement(sql, parameters)) {
            return pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();

        } finally {
            if (conn != null) {
                releaseConnection(conn);
            }
        }
        return -1;
    }

    public LocalDateTime selectDatetime(String sql) throws SQLException {
        Connection conn = getConnection();
        LocalDateTime datetime = null;
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            if (rs.next()) {
                Timestamp timestamp = rs.getTimestamp(1);
                datetime = timestamp.toLocalDateTime();
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                releaseConnection(conn);
            }
        }
        return datetime;
    }

    public Map<String, Object> selectRow(String sql, Object[] parameters) throws SQLException {
        Map<String, Object> map = new HashMap<>();
        Connection conn = getConnection();
        try (PreparedStatement pstmt = conn.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            if (rs.next()) { //레코드가 있을 경우
                for (int i = 1; i <= columnCount; i++) {
                    //컬럼의 수만큼 모든 데이터를 Map에 넣는다.
                    String columnName = metaData.getColumnName(i);
                    Object columnValue = rs.getObject(i);
                    map.put(columnName, columnValue);
                }
                return map;
            } else {
                return null;
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {

            releaseConnection(conn);

        }
        return null;
    }

    public <T> List<T> selectRows(Class<T> clazz, String sql, Object[] parameters) throws SQLException {
        List<T> result = new ArrayList<>();
        try (PreparedStatement pstmt = prepareStatement(sql, parameters);
             ResultSet rs = pstmt.executeQuery()) {

            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (rs.next()) {
                T instance = clazz.getDeclaredConstructor().newInstance();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    Object columnValue = rs.getObject(i);

                    Field field = clazz.getDeclaredField(columnName);
                    field.setAccessible(true);
                    field.set(instance, columnValue);
                }
                result.add(instance);
            }
        } catch (SQLException | InstantiationException | IllegalAccessException | NoSuchMethodException |
                 InvocationTargetException | NoSuchFieldException e) {
            e.printStackTrace();
        }
        return result;
    }

    public String selectString(String sql, Object[] parameters) throws SQLException {
        Connection conn = getConnection();
        try (PreparedStatement pstmt = conn.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {

            return rs.next() ? rs.getString(1) : null;

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                releaseConnection(conn);
            }
        }
        return null;
    }

    public Long selectLong(String sql, Object[] parameters) throws SQLException {
        Connection conn = getConnection();
        try (PreparedStatement pstmt = conn.prepareStatement(sql)){

            for (int i = 0; i < parameters.length; i++) {
                pstmt.setObject(i + 1, parameters[i]);
            }
            try(ResultSet rs = pstmt.executeQuery()) {
                return rs.next() ? rs.getLong(1) : null;
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            releaseConnection(conn);
        }
        return null;
    }

    public List<Long> selectLongs(String sql, Object[] parameters) {
        List<Long> result = new ArrayList<>();
        try (PreparedStatement pstmt = prepareStatement(sql, parameters);
             ResultSet rs = pstmt.executeQuery()) {

            while (rs.next()) {
                result.add(rs.getLong(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }
}
