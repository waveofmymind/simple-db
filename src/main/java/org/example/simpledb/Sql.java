package org.example.simpledb;

import lombok.Data;


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

    private PreparedStatement prepareStatement(Connection conn, String sql, Object... parameters) throws SQLException {
        PreparedStatement pstmt = conn.prepareStatement(sql);
        for (int i = 0; i < parameters.length; i++) {
            pstmt.setObject(i + 1, parameters[i]);
        }
        return pstmt;
    }

    public Map<String, Object> selectRow(Connection conn) {
        Map<String, Object> map = new HashMap<>();
        try (PreparedStatement pstmt = prepareStatement(conn, getSql(), getParameters());
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
        }
        return null;
    }


    public Long selectLong(Connection conn) {
        try (PreparedStatement pstmt = prepareStatement(conn, getSql(), getParameters());
             ResultSet rs = pstmt.executeQuery()) {

            return rs.next() ? rs.getLong(1) : null;

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public List<Long> selectLongs(Connection conn) {
        List<Long> result = new ArrayList<>();
        try (PreparedStatement pstmt = prepareStatement(conn, getSql(), getParameters());
             ResultSet rs = pstmt.executeQuery()) {


            while (rs.next()) {
                result.add(rs.getLong(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    public long insert() throws SQLException {
        return simpleDb.executeQueryWithGeneratedKeys(sqlBuilder.toString(), parameters.toArray());
    }

    public long update() throws SQLException {
        return simpleDb.executeQuery(sqlBuilder.toString(), parameters.toArray());
    }

    public long delete() throws SQLException {
        return simpleDb.executeQuery(sqlBuilder.toString(), parameters.toArray());
    }

    public Map<String, Object> selectRow() throws SQLException {
        return simpleDb.selectRow(sqlBuilder.toString(), parameters.toArray());
    }

    public <T> List<T> selectRows(Class<T> clazz) throws SQLException {
        return simpleDb.selectRows(clazz, sqlBuilder.toString(), parameters.toArray());
    }

    public String selectString() throws SQLException {
        return simpleDb.selectString(sqlBuilder.toString(), parameters.toArray());
    }

    public Long selectLong() throws SQLException {
        return simpleDb.selectLong(sqlBuilder.toString(), parameters.toArray());
    }

    public List<Long> selectLongs() throws SQLException {
        return simpleDb.selectLongs(sqlBuilder.toString(), parameters.toArray());
    }

    public LocalDateTime selectDatetime() throws SQLException {
        return simpleDb.selectDatetime(sqlBuilder.toString());
    }



}
