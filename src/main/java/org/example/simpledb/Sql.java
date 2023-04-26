package org.example.simpledb;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.example.simpledb.article.Article;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.time.LocalDateTime;
import java.util.*;
import java.sql.*;

@Data
public class Sql {
    private final StringBuilder sqlBuilder;
    private final List<Object> parameters;
    private final SimpleDb simpleDb;

    public Sql(SimpleDb simpleDb) {
        this.sqlBuilder = new StringBuilder();
        this.parameters = new ArrayList<>();
        this.simpleDb = simpleDb;
    }

    public Sql append(String sqlPart, Object... params) {
        sqlBuilder.append(" ").append(sqlPart);
        parameters.addAll(Arrays.asList(params));
        return this;
    }

    public Sql appendIn(String sqlPart, Collection<?> values) {
        StringBuilder inClause = new StringBuilder();
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                inClause.append(", ");
            }
            inClause.append("?");
            parameters.add(values.toArray()[i]);

        }

        String newSqlPart = sqlPart.replace("?", inClause.toString());
        this.append(newSqlPart);

        return this;
    }


    public String getSql() {
        return sqlBuilder.toString();
    }

    public Object[] getParameters() {
        return parameters.toArray();
    }

    public long insert() {
        Connection conn = null;
        PreparedStatement pstmt = null;
        try {
            conn = getSimpleDb().getConnection();
            pstmt = conn.prepareStatement(getSql(), Statement.RETURN_GENERATED_KEYS);

            if (getSimpleDb().isDevMode()) {
                System.out.println(getSql());
            }

            Object[] parameters = getParameters();
            for (int i = 0; i < parameters.length; i++) {
                pstmt.setObject(i + 1, parameters[i]);
            }

            pstmt.executeUpdate();
            ResultSet rs = pstmt.getGeneratedKeys();

            if (rs.next()) {
                return rs.getLong(1);
            }

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
            if (conn != null) {
                getSimpleDb().closeConnection();
            }
        }
        return -1;
    }



    public long update() {
        try (Connection conn = DriverManager.getConnection(simpleDb.getUrl(), simpleDb.getUsername(), simpleDb.getPassword());
             PreparedStatement pstmt = conn.prepareStatement(getSql())) {

            Object[] parameters = getParameters();

            for (int i = 0; i < parameters.length; i++) {
                pstmt.setObject(i + 1, parameters[i]);
            }

            if (simpleDb.isDevMode()) {
                System.out.println(getSql());
            }

            return pstmt.executeUpdate();

        } catch (SQLException e) {
            if (simpleDb.isDevMode()) {
                e.printStackTrace();
            }
        }
        return -1;
    }

    public long delete() {
        try (Connection conn = DriverManager.getConnection(simpleDb.getUrl(), simpleDb.getUsername(), simpleDb.getPassword());
             PreparedStatement pstmt = conn.prepareStatement(getSql())) {

            Object[] parameters = getParameters();

            for (int i = 0; i < parameters.length; i++) {
                pstmt.setObject(i + 1, parameters[i]);
            }

            if (simpleDb.isDevMode()) {
                System.out.println(getSql());
            }

            return pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    public LocalDateTime selectDatetime() {
        LocalDateTime datetime = null;
        try (Connection conn = DriverManager.getConnection(simpleDb.getUrl(), simpleDb.getUsername(), simpleDb.getPassword());
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(getSql())) {

            if (rs.next()) {
                Timestamp timestamp = rs.getTimestamp(1);
                datetime = timestamp.toLocalDateTime();
            }

            if (simpleDb.isDevMode()) {
                System.out.println(getSql());
            }


        } catch (SQLException e) {
            e.printStackTrace();
        }
        return datetime;
    }

    public Long selectLong() {
        try (Connection conn = DriverManager.getConnection(simpleDb.getUrl(), simpleDb.getUsername(), simpleDb.getPassword());
             PreparedStatement pstmt = conn.prepareStatement(getSql())) {

            Object[] parameters = getParameters();

            for (int i = 0; i < parameters.length; i++) {
                pstmt.setObject(i + 1, parameters[i]);
            }

            if (simpleDb.isDevMode()) {
                System.out.println(getSql());
            }

            try (ResultSet rs = pstmt.executeQuery()) {
                return rs.next() ? rs.getLong(1) : null;
            }
        } catch (SQLException e) {
            e.printStackTrace();

        }
        return null;
    }

    public String selectString() {
        try (Connection conn = DriverManager.getConnection(simpleDb.getUrl(), simpleDb.getUsername(), simpleDb.getPassword());
             PreparedStatement pstmt = conn.prepareStatement(getSql())) {

            Object[] parameters = getParameters();

            for (int i = 0; i < parameters.length; i++) {
                pstmt.setObject(i + 1, parameters[i]);
            }

            if (simpleDb.isDevMode()) {
                System.out.println(getSql());
            }

            try (ResultSet rs = pstmt.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }

        } catch (SQLException e) {
            e.printStackTrace();

        }
        return null;
    }

    public Map<String, Object> selectRow() {
        Map<String, Object> map = new HashMap<>();
        try (Connection conn = DriverManager.getConnection(simpleDb.getUrl(), simpleDb.getUsername(), simpleDb.getPassword());
             PreparedStatement pstmt = conn.prepareStatement(getSql())) {

            if (simpleDb.isDevMode()) {
                System.out.println(getSql());
            }

            try (ResultSet rs = pstmt.executeQuery()) {
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
            }

        } catch (SQLException e) {
            e.printStackTrace();

        }
        return null;
    }

    public <T> List<T> selectRows(Class<T> clazz) {
        List<T> result = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(simpleDb.getUrl(), simpleDb.getUsername(), simpleDb.getPassword());
             PreparedStatement pstmt = conn.prepareStatement(getSql())) {

            if (simpleDb.isDevMode()) {
                System.out.println(getSql());
            }

            Object[] parameters = getParameters();

            for (int i = 0; i< parameters.length; i++) {
                pstmt.setObject(i+1, parameters[i]);
            }

            try (ResultSet rs = pstmt.executeQuery()) {
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();

                if(rs.next()) {
                    T instance = clazz.getDeclaredConstructor().newInstance();
                    for (int i = 1; i<= columnCount; i++) {
                        String columnName = metaData.getColumnName(i);
                        Object columnValue = rs.getObject(i);

                        Field field = clazz.getDeclaredField(columnName);
                        field.setAccessible(true);
                        field.set(instance, columnValue);
                    }
                    result.add(instance);
                }
            }


        } catch (SQLException | InstantiationException | IllegalAccessException | NoSuchMethodException |
                 InvocationTargetException | NoSuchFieldException e) {
            e.printStackTrace();
        }

        return result;


    }

    public List<Long> selectLongs() {

        List<Long> result = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(simpleDb.getUrl(), simpleDb.getUsername(), simpleDb.getPassword());
             PreparedStatement pstmt = conn.prepareStatement(getSql())) {

            if (simpleDb.isDevMode()) {
                System.out.println(getSql());
            }

            Object[] parameters = getParameters();

            for (int i = 0; i< parameters.length; i++) {
                pstmt.setObject(i+1, parameters[i]);
            }

            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    result.add(rs.getLong(1));
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }
}
