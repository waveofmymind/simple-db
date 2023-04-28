package org.example.simpledb;

import org.example.simpledb.article.Article;
import org.example.simpledb.article.Column;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)

class SimpleDbTest {

    private SimpleDb simpleDb;

    @BeforeAll
    public void beforeAll() {
        simpleDb = new SimpleDb("localhost", "wave", "0913", "simpleDb__test");
        simpleDb.setDevMod(true);

//        createArticleTable();
        System.out.println(generateDDL(Article.class));
    }

    @BeforeEach
    public void beforeEach() {
        truncateArticleTable();
        makeArticleTestData();
    }

    public String generateDDL(Class<?> clazz) {
        StringBuilder ddl = new StringBuilder("CREATE TABLE ");
        ddl.append(clazz.getSimpleName().toUpperCase()).append(" (\n");

        Field[] fields = clazz.getDeclaredFields();
        for (Field field : fields) {
            Column column = field.getAnnotation(Column.class);

            if (column != null) {
                ddl.append(field.getName().toUpperCase()).append(" ").append(column.type());

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

        return ddl.toString();
    }


    private void makeArticleTestData() {
        IntStream.rangeClosed(1, 6).forEach(no -> {
            boolean isBlind = no > 3;
            String title = "제목%d".formatted(no);
            String body = "내용%d".formatted(no);

            simpleDb.run("""
                    INSERT INTO article
                    SET createdDate = NOW(),
                    modifiedDate = NOW(),
                    title = ?,
                    `body` = ?,
                    isBlind = ?
                    """, title, body, isBlind);
        });
    }

    private void truncateArticleTable() {
        simpleDb.run("TRUNCATE article");
    }

    @Test
    public void insert() throws SQLException {
        Sql sql = simpleDb.genSql();
        Connection conn = simpleDb.getConnection();

        sql.append("INSERT INTO article")
                .append("SET createdDate = NOW()")
                .append(", modifiedDate = NOW()")
                .append(", title = ?", "제목 new")
                .append(", body = ?", "내용 new");

        long newId = sql.insert(conn);

        assertThat(newId).isGreaterThan(0);

    }

    @Test
    public void update() throws SQLException {
        Sql sql = simpleDb.genSql();
        Connection conn = simpleDb.getConnection();
        // id가 0, 1, 2, 3인 글 수정
        // id가 0인 글은 없으니, 실제로는 3개의 글이 삭제됨

        /*
        == rawSql ==
        UPDATE article
        SET title = '제목 new'
        WHERE id IN ('0', '1', '2', '3')
        */
        sql
                .append("UPDATE article")
                .append("SET title = ?", "제목 new")
                .append("WHERE id IN (?, ?, ?, ?)", 0, 1, 2, 3);

        // 수정된 row 개수
        long affectedRowsCount = sql.update(conn);

        assertThat(affectedRowsCount).isEqualTo(3);
    }

    @Test
    public void delete() throws SQLException {
        Sql sql = simpleDb.genSql();
        Connection conn = simpleDb.getConnection();

        // id가 0, 1, 3인 글 삭제
        // id가 0인 글은 없으니, 실제로는 2개의 글이 삭제됨
        /*
        == rawSql ==
        DELETE FROM article
        WHERE id IN ('0', '1', '3')
        */
        sql.append("DELETE")
                .append("FROM article")
                .append("WHERE id IN (?, ?, ?)", 0, 1, 3);

        // 삭제된 row 개수
        long affectedRowsCount = sql.delete(conn);

        assertThat(affectedRowsCount).isEqualTo(2);
    }

    @Test
    public void selectDatetime() throws SQLException {
        Sql sql = simpleDb.genSql();
        Connection conn = simpleDb.getConnection();

        /*
        == rawSql ==
        SELECT NOW()
        */
        sql.append("SELECT NOW()");
        LocalDateTime datetime = sql.selectDatetime(conn);

        long diff = ChronoUnit.SECONDS.between(datetime, LocalDateTime.now());

        assertThat(diff).isLessThanOrEqualTo(1L);
    }

    @Test
    public void selectLong() throws SQLException {
        Sql sql = simpleDb.genSql();
        Connection conn = simpleDb.getConnection();

        /*
        == rawSql ==
        SELECT id
        FROM article
        WHERE id = 1
        */
        sql.append("SELECT id")
                .append("FROM article")
                .append("WHERE id = 1");

        Long id = sql.selectLong(conn);

        assertThat(id).isEqualTo(1);
    }

    @Test
    public void selectString() throws SQLException {
        Sql sql = simpleDb.genSql();

        Connection conn = simpleDb.getConnection();

        /*
        == rawSql ==
        SELECT title
        FROM article
        WHERE id = 1
        */
        sql.append("SELECT title")
                .append("FROM article")
                .append("WHERE id = 1");

        String title = sql.selectString(conn);

        assertThat(title).isEqualTo("제목1");
    }

    @Test
    public void selectRow() throws SQLException {
        Sql sql = simpleDb.genSql();

        Connection conn = simpleDb.getConnection();

        /*
        == rawSql ==
        SELECT *
        FROM article
        WHERE id = 1
        */
        sql.append("SELECT * FROM article WHERE id = 1");
        Map<String, Object> articleMap = sql.selectRow(conn);

        assertThat(articleMap.get("id")).isEqualTo(1L);
        assertThat(articleMap.get("title")).isEqualTo("제목1");
        assertThat(articleMap.get("body")).isEqualTo("내용1");
        assertThat(articleMap.get("createdDate")).isInstanceOf(LocalDateTime.class);
        assertThat(articleMap.get("modifiedDate")).isInstanceOf(LocalDateTime.class);
        assertThat(articleMap.get("isBlind")).isEqualTo(false);
    }

    @Test
    public void selectArticles() throws SQLException {
        Sql sql = simpleDb.genSql();
        Connection conn = simpleDb.getConnection();

        /*
        == rawSql ==
        SELECT *
        FROM article
        ORDER BY id ASC
        LIMIT 3
        */
        sql.append("SELECT * FROM article ORDER BY id ASC LIMIT 3");
        List<Article> articleDtoList = sql.selectRows(conn,Article.class);

        IntStream.range(0, articleDtoList.size()).forEach(i -> {
            long id = i + 1;

            Article articleDto = articleDtoList.get(i);

            assertThat(articleDto.getId()).isEqualTo(id);
            assertThat(articleDto.getTitle()).isEqualTo("제목%d".formatted(id));
            assertThat(articleDto.getBody()).isEqualTo("내용%d".formatted(id));
            assertThat(articleDto.getCreatedDate()).isNotNull();
            assertThat(articleDto.getModifiedDate()).isNotNull();
            assertThat(articleDto.getIsBlind()).isFalse();
        });
    }

    @Test
    public void selectBind() throws SQLException {
        Sql sql = simpleDb.genSql();
        Connection conn = simpleDb.getConnection();

        sql.append("SELECT COUNT(*)")
                .append("FROM article")
                .append("WHERE id BETWEEN ? AND ?", 1, 3)
                .append("AND title LIKE CONCAT('%', ? '%')", "제목");

        long count = sql.selectLong(conn);

        assertThat(count).isEqualTo(3);
    }

    @Test
    public void selectIn() throws SQLException {
        Sql sql = simpleDb.genSql();
        Connection conn = simpleDb.getConnection();

        /*
        == rawSql ==
        SELECT COUNT(*)
        FROM article
        WHERE id IN ('1','2','3')
        */
        sql.append("SELECT COUNT(*)")
                .append("FROM article")
                .appendIn("WHERE id IN (?)", Arrays.asList(1L, 2L, 3L));

        long count = sql.selectLong(conn);

        assertThat(count).isEqualTo(3);
    }

    @Test
    public void selectOrderByField() throws SQLException {
        List<Long> ids = Arrays.asList(2L, 3L, 1L);
        Connection conn = simpleDb.getConnection();

        Sql sql = simpleDb.genSql();
        /*
        SELECT id
        FROM article
        WHERE id IN ('2','3','1')
        ORDER BY FIELD (id, '2','3','1')
        */
        sql.append("SELECT id")
                .append("FROM article")
                .appendIn("WHERE id IN (?)", ids)
                .appendIn("ORDER BY FIELD (id, ?)", ids);

        List<Long> foundIds = sql.selectLongs(conn);

        assertThat(foundIds).isEqualTo(ids);
    }



    @Test
    public void transactionTest() {
        Connection conn = null;
        try {
            conn = simpleDb.getConnection();

            // 트랜잭션 시작
            simpleDb.startTransaction(conn);
            // Insert a new record
            Sql insertSql = simpleDb.genSql();
            insertSql.append("INSERT INTO article")
                    .append("SET createdDate = NOW()")
                    .append(", modifiedDate = NOW()")
                    .append(", title = ?", "제목 new")
                    .append(", body = ?", "내용 new");

            long insertedId = insertSql.insert(conn);

            // Select the inserted record
            Sql selectSql = simpleDb.genSql();
            selectSql.append("SELECT * FROM article WHERE id = ?", insertedId);

            Map<String, Object> rowData = selectSql.selectRow(conn);

            // Assert the inserted record data
            assertThat(rowData.get("id")).isEqualTo(insertedId);
            assertThat(rowData.get("title")).isEqualTo("제목 new");

            // Update the inserted record
            Sql updateSql = simpleDb.genSql();
            updateSql.append("UPDATE article SET title = ? WHERE id = ?", "제목 수정", insertedId);

            updateSql.update(conn);

            // 트랜잭션 커밋
            simpleDb.commitTransaction(conn);
        } catch (SQLException e) {
            e.printStackTrace();
            simpleDb.rollbackTransaction(conn);
        } finally {
            if (conn != null) {
                simpleDb.endTransaction(conn);
                simpleDb.releaseConnection(conn);
            }
        }
    }

    @Test
    public void transactionTestWithRollback() {
        Connection conn = null;
        Map<String, Object> originalData = null;

        try {
            conn = simpleDb.getConnection();
            // 트랜잭션 시작 전 원본 데이터를 가져옵니다.
            Sql selectSql = simpleDb.genSql();
            selectSql.append("SELECT * FROM article WHERE id = ?", 1);
            originalData = selectSql.selectRow(conn);

            // 트랜잭션 시작
            conn.setAutoCommit(false);

            // 데이터를 수정합니다.
            Sql updateSql = simpleDb.genSql();
            updateSql.append("UPDATE article")
                    .append("SET modifiedDate = NOW(), title = ?", "Updated Title")
                    .append("WHERE id = ?", 1);

            updateSql.update(conn);

            // 의도적으로 예외를 발생시킵니다.
            throw new SQLException("SQL 예외 의도적으로 발생");

        } catch (SQLException e) {
            e.printStackTrace();
            // 트랜잭션 롤백
            if (conn != null) {
                try {
                    conn.rollback();
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
            }
        } finally {
            if (conn != null) {
                try {
                    conn.setAutoCommit(true);
                    simpleDb.releaseConnection(conn);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        Map<String, Object> afterRollbackData = null;
        try {
            conn = simpleDb.getConnection();
            Sql selectSql = simpleDb.genSql();
            selectSql.append("SELECT * FROM article WHERE id = ?", 1);
            afterRollbackData = selectSql.selectRow(conn);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                simpleDb.releaseConnection(conn);
            }
        }

        // 롤백이 실행되었다면, 원본 데이터와 롤백 후 데이터가 동일해야 합니다.
        assertThat(originalData).isEqualTo(afterRollbackData);
    }


}



