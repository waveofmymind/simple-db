package org.example.simpledb;

import org.example.simpledb.article.Article;
import org.example.simpledb.article.Column;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import java.time.LocalDateTime;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)

public class MultiThreadTest {

    private SimpleDb simpleDb;

    @BeforeAll
    public void beforeAll() {
        simpleDb = new SimpleDb("localhost", "wave", "0913", "simpleDb__test");
        simpleDb.setDevMod(true);

        System.out.println(generateDDL(Article.class));
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


    @Test
    public void selectRowMultiThreaded() throws InterruptedException, SQLException {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch latch = new CountDownLatch(2);

        Runnable firstThreadTask = () -> {
            Connection conn = null;
            try {
                conn = simpleDb.getConnection();
                Sql sql = simpleDb.genSql();
                sql.append("SELECT * FROM article WHERE id = 1");

                Map<String, Object> articleMap = sql.selectRow(conn);
                assertArticleMap(articleMap);

                Thread.sleep(5000); // 5초 쉬기

                articleMap = sql.selectRow(conn);
                assertArticleMap(articleMap);
            } catch (SQLException | InterruptedException e) {
                e.printStackTrace();
            } finally {
                if (conn != null) {
                    simpleDb.releaseConnection(conn);
                }
                latch.countDown();
            }
        };

        Runnable secondThreadTask = () -> {
            Connection conn = null;
            try {
                conn = simpleDb.getConnection();
                Sql sql = simpleDb.genSql();
                sql.append("SELECT * FROM article WHERE id = 1");

                Map<String, Object> articleMap = sql.selectRow(conn);
                assertArticleMap(articleMap);

                Thread.sleep(1000); // 1초 쉬기

                articleMap = sql.selectRow(conn);
                assertArticleMap(articleMap);
            } catch (SQLException | InterruptedException e) {
                e.printStackTrace();
            } finally {
                if (conn != null) {
                    simpleDb.releaseConnection(conn);
                }
                latch.countDown();
            }
        };

        executor.submit(firstThreadTask);
        executor.submit(secondThreadTask);

        latch.await(); // Wait for both tasks to complete
        executor.shutdown();
    }

    @Test
    public void testConnectionPoolCapacity() throws InterruptedException {

        int threadCount = 5; // 테스트할 쓰레드 수
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        Runnable task = () -> {
            Connection conn = null;
            try {
                conn = simpleDb.getConnection();
                Sql sql = simpleDb.genSql();
                sql.append("SELECT * FROM article WHERE id = 1");

                Map<String, Object> articleMap = sql.selectRow(conn);
                assertArticleMap(articleMap);

            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                if (conn != null) {
                    simpleDb.releaseConnection(conn);
                }
                latch.countDown();
            }
        };

        for (int i = 0; i < threadCount; i++) {
            executor.submit(task);
        }

        latch.await(); // 모든 쓰레드가 작업을 완료할 때까지 기다림

        int availableConnections = simpleDb.getAvailableConnectionCount();
        assertEquals(2, availableConnections); // 커넥션 풀의 사용 가능한 커넥션 수 확인
    }


    private void assertArticleMap(Map<String, Object> articleMap) {
        assertThat(articleMap.get("id")).isEqualTo(1L);
        assertThat(articleMap.get("title")).isEqualTo("제목1");
        assertThat(articleMap.get("body")).isEqualTo("내용1");
        assertThat(articleMap.get("createdDate")).isInstanceOf(LocalDateTime.class);
        assertThat(articleMap.get("modifiedDate")).isInstanceOf(LocalDateTime.class);
        assertThat(articleMap.get("isBlind")).isEqualTo(false);
    }
}
