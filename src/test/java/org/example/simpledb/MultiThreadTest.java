package org.example.simpledb;

import org.example.simpledb.article.Article;
import org.example.simpledb.article.Column;
import org.junit.jupiter.api.*;

import java.time.LocalDateTime;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
@DisplayName("멀티스레드 테스트")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)

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


    @DisplayName("두번째 태스크가 먼저 실행되고, 5초 후에 첫번째 태스크가 실행된다.")
    @Test
    @Order(1)
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
    @DisplayName("커넥션 풀의 용량을 초과하는 쓰레드가 실행되면, 쓰레드가 종료되기 전까지 대기한다.")
    @Test
    @Order(2)
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
        assertEquals(1, availableConnections); // 커넥션 풀의 사용 가능한 커넥션 수 확인
    }

    @DisplayName("쓰레드당 커넥션을 최대 30초까지 사용하고 자동 반납한다.")
    @Test
    @Order(3)
    public void testConnectionTimeout() throws InterruptedException {
        // 쓰레드 풀 생성
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch latch = new CountDownLatch(2);

        // 첫 번째 쓰레드 작업: 커넥션을 가져온 후 35초 동안 대기
        Runnable firstThreadTask = () -> {
            Connection conn = null;
            try {
                conn = simpleDb.getConnection();
                Thread.sleep(35000); // 35초 대기
            } catch (SQLException | InterruptedException e) {
                e.printStackTrace();
            } finally {
                if (conn != null) {
                    simpleDb.releaseConnection(conn);
                }
                latch.countDown();
            }
        };

        // 두 번째 쓰레드 작업: 31초 후에 커넥션을 요청하고, 사용 가능한지 확인
        Runnable secondThreadTask = () -> {
            try {
                Thread.sleep(31000);
                Connection conn = simpleDb.getConnection();
                assertNotNull(conn);
            } catch (SQLException | InterruptedException e) {
                e.printStackTrace();
            } finally {
                latch.countDown();
            }
        };

        // 쓰레드 작업 제출
        executor.submit(firstThreadTask);
        executor.submit(secondThreadTask);
        // 모든 작업이 완료될 때까지 대기
        latch.await();
        executor.shutdown();
    }

    @DisplayName("한 쓰레드에서 생성된 커넥션이 30초후 자동 반납되고, 그 이후에 다른 쓰레드에서 커넥션을 가져올 수 있다.")
    @Test
    @Order(4)
    public void testConnectionPoolSizeOne() throws InterruptedException {
        // 쓰레드 풀 생성
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch latch = new CountDownLatch(2);

        // 첫 번째 쓰레드 작업: 커넥션을 가져온 후 35초 동안 대기
        Runnable firstThreadTask = () -> {
            Connection conn = null;
            try {
                conn = simpleDb.getConnection();
                Thread.sleep(35000); // 35초 대기
            } catch (SQLException | InterruptedException e) {
                e.printStackTrace();
            } finally {
                if (conn != null) {
                    simpleDb.releaseConnection(conn);
                }
                latch.countDown();
            }
        };

        // 두 번째 쓰레드 작업: 15초 후에 커넥션을 요청하고, 사용 가능한지 확인
        Runnable secondThreadTask = () -> {
            try {
                Thread.sleep(15000); // 15초 대기
                long startTime = System.currentTimeMillis();
                Connection conn = simpleDb.getConnection();
                long elapsedTime = System.currentTimeMillis() - startTime;
                assertTrue(elapsedTime >= 15000);
                assertNotNull(conn);
            } catch (SQLException | InterruptedException e) {
                e.printStackTrace();
            } finally {
                latch.countDown();
            }
        };

        // 쓰레드 작업 제출
        executor.submit(firstThreadTask);
        executor.submit(secondThreadTask);
        // 모든 작업이 완료될 때까지 대기
        latch.await();
        executor.shutdown();
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
