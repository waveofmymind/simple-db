package org.example.simpledb.article;

import lombok.Data;

import java.lang.reflect.Field;
import java.time.LocalDateTime;

@Data
public class Article {

    @Column(type = "BIGINT UNSIGNED AUTO_INCREMENT", nullable = true)
    private Long id;
    @Column(type = "VARCHAR(100)", nullable = true)
    private String title;
    @Column(type = "TEXT", nullable = true)
    private String body;

    @Column(type = "DATETIME", nullable = true)
    private LocalDateTime createdDate;

    @Column(type = "DATETIME", nullable = true)
    private LocalDateTime modifiedDate;

    @Column(type = "BIT(1)", nullable = true, defaultValue = "0")
    private Boolean isBlind;

}
