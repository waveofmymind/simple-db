package org.example.simpledb;

import org.example.simpledb.article.Article;
import org.junit.jupiter.api.*;

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
    public void beforeAll() throws SQLException {
        simpleDb = new SimpleDb("localhost", "wave", "0913", "simpleDb__test");
        simpleDb.setDevMod(true);
        Sql sql = simpleDb.genSql();

        simpleDb.generateDDL(Article.class);
    }

    @BeforeEach
    public void beforeEach() {
        truncateArticleTable();
        makeArticleTestData();
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

    @DisplayName("레코드 생성 테스트")
    @Test
    public void insert() throws SQLException {
        Sql sql = simpleDb.genSql();

        sql.append("INSERT INTO article")
                .append("SET createdDate = NOW()")
                .append(", modifiedDate = NOW()")
                .append(", title = ?", "제목 new")
                .append(", body = ?", "내용 new");

        if (simpleDb.isDevMode()) {
            System.out.println(sql.getSql());
        }
        long newId = sql.insert();

        assertThat(newId).isGreaterThan(0);

    }

    @DisplayName("레코드 수정 테스트")
    @Test
    public void update() throws SQLException {
        Sql sql = simpleDb.genSql();
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

        if (simpleDb.isDevMode()) {
            System.out.println(sql.getSql());
        }

        // 수정된 row 개수
        long affectedRowsCount = sql.update();

        assertThat(affectedRowsCount).isEqualTo(3);
    }

    @DisplayName("레코드 삭제 테스트")
    @Test
    public void delete() throws SQLException {
        Sql sql = simpleDb.genSql();
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

        if (simpleDb.isDevMode()) {
            System.out.println(sql.getSql());
        }

        // 삭제된 row 개수
        long affectedRowsCount = sql.delete();

        assertThat(affectedRowsCount).isEqualTo(2);
    }

    @DisplayName("DB 현재 시간 조회 테스트")
    @Test
    public void selectDatetime() throws SQLException {
        Sql sql = simpleDb.genSql();
        /*
        == rawSql ==
        SELECT NOW()
        */
        sql.append("SELECT NOW()");

        if (simpleDb.isDevMode()) {
            System.out.println(sql.getSql());
        }

        LocalDateTime datetime = sql.selectDatetime();

        long diff = ChronoUnit.SECONDS.between(datetime, LocalDateTime.now());

        assertThat(diff).isLessThanOrEqualTo(1L);
    }

    @DisplayName("레코드 ID값 조회 테스트")
    @Test
    public void selectLong() throws SQLException {
        Sql sql = simpleDb.genSql();

        sql.append("SELECT id")
                .append("FROM article")
                .append("WHERE id = 1");

        if (simpleDb.isDevMode()) {
            System.out.println(sql.getSql());
        }

        Long id = sql.selectLong();

        assertThat(id).isEqualTo(1);
    }

    @DisplayName("레코드 제목 조회 테스트")
    @Test
    public void selectString() throws SQLException {
        Sql sql = simpleDb.genSql();

        sql.append("SELECT title")
                .append("FROM article")
                .append("WHERE id = 1");

        if (simpleDb.isDevMode()) {
            System.out.println(sql.getSql());
        }

        String title = sql.selectString();

        assertThat(title).isEqualTo("제목1");
    }

    @DisplayName("레코드 전체 조회 테스트")
    @Test
    public void selectRow() throws SQLException {
        Sql sql = simpleDb.genSql();

        sql.append("SELECT * FROM article WHERE id = 1");

        if (simpleDb.isDevMode()) {
            System.out.println(sql.getSql());
        }

        Map<String, Object> articleMap = sql.selectRow();

        assertThat(articleMap.get("id")).isEqualTo(1L);
        assertThat(articleMap.get("title")).isEqualTo("제목1");
        assertThat(articleMap.get("body")).isEqualTo("내용1");
        assertThat(articleMap.get("createdDate")).isInstanceOf(LocalDateTime.class);
        assertThat(articleMap.get("modifiedDate")).isInstanceOf(LocalDateTime.class);
        assertThat(articleMap.get("isBlind")).isEqualTo(false);
    }

    @DisplayName("레코드 3개 리밋 조회 테스트")
    @Test
    public void selectArticles() throws SQLException {
        Sql sql = simpleDb.genSql();

        sql.append("SELECT * FROM article ORDER BY id ASC LIMIT 3");

        if (simpleDb.isDevMode()) {
            System.out.println(sql.getSql());
        }

        List<Article> articleDtoList = sql.selectRows(Article.class);

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

    @DisplayName("파라미터 바인딩 테스트")
    @Test
    public void selectBind() throws SQLException {
        Sql sql = simpleDb.genSql();

        sql.append("SELECT COUNT(*)")
                .append("FROM article")
                .append("WHERE id BETWEEN ? AND ?", 1, 3)
                .append("AND title LIKE CONCAT('%', ? '%')", "제목");

        if (simpleDb.isDevMode()) {
            System.out.println(sql.getSql());
        }

        long count = sql.selectLong();

        assertThat(count).isEqualTo(3);
    }

    @DisplayName("IN 절 테스트")
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

        if (simpleDb.isDevMode()) {
            System.out.println(sql.getSql());
        }

        long count = sql.selectLong(conn);

        assertThat(count).isEqualTo(3);
    }

    @DisplayName("정렬 바인딩 테스트")
    @Test
    public void selectOrderByField() throws SQLException {
        List<Long> ids = Arrays.asList(2L, 3L, 1L);

        Sql sql = simpleDb.genSql();

        sql.append("SELECT id")
                .append("FROM article")
                .appendIn("WHERE id IN (?)", ids)
                .appendIn("ORDER BY FIELD (id, ?)", ids);

        if (simpleDb.isDevMode()) {
            System.out.println(sql.getSql());
        }

        List<Long> foundIds = sql.selectLongs();

        assertThat(foundIds).isEqualTo(ids);
    }


    @DisplayName("트랜잭션 커밋전 조회시 이전 내용 확인 테스트")
    @Test
    public void transactionTest() throws SQLException {
        Connection conn = simpleDb.getConnection();
        try {
            // 트랜잭션 시작
            simpleDb.startTransaction(conn);
            Sql insertSql = simpleDb.genSql();
            insertSql.append("INSERT INTO article")
                    .append("SET createdDate = NOW()")
                    .append(", modifiedDate = NOW()")
                    .append(", title = ?", "제목 new")
                    .append(", body = ?", "내용 new");

            if (simpleDb.isDevMode()) {
                System.out.println(insertSql.getSql());
            }

            long insertedId = insertSql.insert();

            // Select the inserted record
            Sql selectSql = simpleDb.genSql();
            selectSql.append("SELECT * FROM article WHERE id = ?", insertedId);

            if (simpleDb.isDevMode()) {
                System.out.println(selectSql.getSql());
            }

            Map<String, Object> rowData = selectSql.selectRow(conn);

            assertThat((rowData.get("id"))).isEqualTo(insertedId);
            assertThat(rowData.get("title")).isEqualTo("제목 new");

            Sql updateSql = simpleDb.genSql();
            updateSql.append("UPDATE article SET title = ? WHERE id = ?", "제목 수정", insertedId);

            if (simpleDb.isDevMode()) {
                System.out.println(updateSql.getSql());
            }

            updateSql.update();

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

    @DisplayName("트랜잭션 롤백 테스트")
    @Test
    public void transactionTestWithRollback() throws SQLException {
        Map<String, Object> originalData = null;
        Connection conn = simpleDb.getConnection();

        try {
            // 트랜잭션 시작 전 원본 데이터를 가져옵니다.
            Sql selectSql = simpleDb.genSql();
            selectSql.append("SELECT * FROM article WHERE id = ?", 1);
            originalData = selectSql.selectRow();

            // 트랜잭션 시작
            simpleDb.startTransaction(conn);

            // 데이터를 수정합니다.
            Sql updateSql = simpleDb.genSql();
            updateSql.append("UPDATE article")
                    .append("SET modifiedDate = NOW(), title = ?", "Updated Title")
                    .append("WHERE id = ?", 1);

            if (simpleDb.isDevMode()) {
                System.out.println(updateSql.getSql());
            }

            updateSql.update();

            // 의도적으로 예외를 발생시킵니다.
            throw new SQLException("SQL 예외 의도적으로 발생");

        } catch (SQLException e) {
            e.printStackTrace();
            // 트랜잭션 롤백
            simpleDb.rollbackTransaction(conn);
            System.out.println("Transaction rolled back.");

        } finally {
            simpleDb.endTransaction(conn);
            simpleDb.releaseConnection(conn);

        }
        Map<String, Object> afterRollbackData = null;
        try {
            Sql selectSql = simpleDb.genSql();
            selectSql.append("SELECT * FROM article WHERE id = ?", 1);
            afterRollbackData = selectSql.selectRow();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            simpleDb.releaseConnection(conn);

        }

        // 롤백이 실행되었다면, 원본 데이터와 롤백 후 데이터가 동일해야 합니다.
        assertThat(originalData).isEqualTo(afterRollbackData);
    }


}



