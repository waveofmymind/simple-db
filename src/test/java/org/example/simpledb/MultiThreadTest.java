package org.example.simpledb;

import org.example.simpledb.article.Article;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class MultiThreadTest {

    private SimpleDb simpleDb;

    @BeforeAll
    public void beforeAll() {
        simpleDb = new SimpleDb("localhost", "wave", "0913", "simpleDb__test");
        simpleDb.setDevMod(true);

        createArticleTable();
    }

    @BeforeEach
    public void beforeEach() {
        truncateArticleTable();
        makeArticleTestData();
    }

    private void createArticleTable() {
        simpleDb.run("DROP TABLE IF EXISTS article");

        simpleDb.run("""                                                
                CREATE TABLE article (
                    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
                    PRIMARY KEY(id),
                    createdDate DATETIME NOT NULL,
                    modifiedDate DATETIME NOT NULL,
                    title VARCHAR(100) NOT NULL,
                    `body` TEXT NOT NULL,
                    isBlind BIT(1) NOT NULL DEFAULT(0)
                )
                """);
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
    public void MultiThread_Test() {
        // 스레드 풀 생성 (예: 10개의 스레드를 가진 풀)
        ExecutorService executorService = Executors.newFixedThreadPool(10);

        // 테스트할 작업 수 (예: 100)
        int tasks = 100;

        Runnable insertTask = getInsertRunnable();
        Runnable updateTask = getUpdateRunnable();
        Runnable selectTask = getSelectRunnable();
        Runnable deleteTask = getDeleteRunnable();

        // 각 작업을 스레드 풀에 제출
        for (int i = 0; i < tasks; i++) {
            executorService.submit(insertTask);
            executorService.submit(updateTask);
            executorService.submit(selectTask);
            executorService.submit(deleteTask);


        }

        // 스레드 풀 종료를 요청하고 모든 작업이 완료될 때까지 대기
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(1, TimeUnit.MINUTES)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
        }
    }

    private Runnable getInsertRunnable() {
        // 작업을 실행할 Runnable 객체 생성
        return () -> {
            // Sql 인스턴스 생성 및 쿼리 작성
            Sql sql = simpleDb.genSql();
            sql.append("INSERT INTO article")
                    .append("SET createdDate = NOW()")
                    .append(", modifiedDate = NOW()")
                    .append(", title = ?", "제목 new")
                    .append(", body = ?", "내용 new");

            // insert 메서드 호출 및 결과 출력
            long id = sql.insert();
            System.out.println("생성된 ID: " + id);
        };

    }

    private Runnable getUpdateRunnable() {
        // 작업을 실행할 Runnable 객체 생성
        return () -> {
            // Sql 인스턴스 생성 및 쿼리 작성
            Sql sql = simpleDb.genSql();
            sql
                    .append("UPDATE article")
                    .append("SET title = ?", "제목 new")
                    .append("WHERE id IN (?, ?, ?, ?)", 0, 1, 2, 3);

            // update 메서드 호출 및 결과 출력
            long affectedRows = sql.update();
            System.out.println("Affected Rows: " + affectedRows);
        };
    }

    private Runnable getSelectRunnable() {
        // 작업을 실행할 Runnable 객체 생성
        return () -> {
            // Sql 인스턴스 생성 및 쿼리 작성
            Sql sql = simpleDb.genSql();
            sql
                    .append("SELECT * FROM article")
                    .append("WHERE id IN (?, ?, ?, ?)", 0, 1, 2, 3);

            // select 메서드 호출 및 결과 출력
            List<Article> articles = sql.selectRows(Article.class);
            System.out.println("Articles: " + articles);
        };
    }

    private Runnable getDeleteRunnable() {
        // 작업을 실행할 Runnable 객체 생성
        return () -> {
            // Sql 인스턴스 생성 및 쿼리 작성
            Sql sql = simpleDb.genSql();
            sql
                    .append("DELETE FROM article")
                    .append("WHERE id IN (?, ?, ?, ?)", 0, 1, 2, 3);

            // delete 메서드 호출 및 결과 출력
            long affectedRows = sql.delete();
            System.out.println("Affected Rows: " + affectedRows);
        };
    }



}


