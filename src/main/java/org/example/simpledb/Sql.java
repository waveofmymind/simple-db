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

    public Sql(SimpleDb simpleDb) throws SQLException {
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

    private PreparedStatement prepareStatement(Connection conn, String sql, Object... parameters) throws SQLException {
        PreparedStatement pstmt = conn.prepareStatement(sql);
        for (int i = 0; i < parameters.length; i++) {
            pstmt.setObject(i + 1, parameters[i]);
        }
        return pstmt;
    }

    public long insert(Connection conn) {
        String sql = getSql();
        Object[] parameters = getParameters();

        if (simpleDb.isDevMode()) {
            System.out.println(sql);
        }

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
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    public Map<String, Object> selectRow(Connection conn) {
        Map<String, Object> map = new HashMap<>();
        try (PreparedStatement pstmt = prepareStatement(conn, getSql(), getParameters());
             ResultSet rs = pstmt.executeQuery()) {

            if (simpleDb.isDevMode()) {
                System.out.println(getSql());
            }

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
        }
        return null;
    }

    public long update(Connection conn) {
        String sql = getSql();
        Object[] parameters = getParameters();

        if (simpleDb.isDevMode()) {
            System.out.println(sql);
        }

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            for (int i = 0; i < parameters.length; i++) {
                pstmt.setObject(i + 1, parameters[i]);
            }
            return pstmt.executeUpdate();
        } catch (SQLException e) {
            if (simpleDb.isDevMode()) {
                e.printStackTrace();
            }
        }
        return -1;
    }

    public long delete(Connection conn) {
        String sql = getSql();
        Object[] parameters = getParameters();

        if (simpleDb.isDevMode()) {
            System.out.println(sql);
        }

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            for (int i = 0; i < parameters.length; i++) {
                pstmt.setObject(i + 1, parameters[i]);
            }
            return pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }



    public LocalDateTime selectDatetime(Connection conn) {
        LocalDateTime datetime = null;
        try (Statement stmt = conn.createStatement();
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

    public Long selectLong(Connection conn) {
        try (PreparedStatement pstmt = prepareStatement(conn, getSql(), getParameters());
             ResultSet rs = pstmt.executeQuery()) {

            if (simpleDb.isDevMode()) {
                System.out.println(getSql());
            }

            return rs.next() ? rs.getLong(1) : null;

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public String selectString(Connection conn) {
        try (PreparedStatement pstmt = prepareStatement(conn, getSql(), getParameters());
             ResultSet rs = pstmt.executeQuery()) {

            if (simpleDb.isDevMode()) {
                System.out.println(getSql());
            }

            return rs.next() ? rs.getString(1) : null;

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public <T> List<T> selectRows(Connection conn, Class<T> clazz) {
        List<T> result = new ArrayList<>();
        try (PreparedStatement pstmt = prepareStatement(conn, getSql(), getParameters());
             ResultSet rs = pstmt.executeQuery()) {

            if (simpleDb.isDevMode()) {
                System.out.println(getSql());
            }

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


    public List<Long> selectLongs(Connection conn) {
        List<Long> result = new ArrayList<>();
        try (PreparedStatement pstmt = prepareStatement(conn, getSql(), getParameters());
             ResultSet rs = pstmt.executeQuery()) {

            if (simpleDb.isDevMode()) {
                System.out.println(getSql());
            }

            while (rs.next()) {
                result.add(rs.getLong(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }
}
