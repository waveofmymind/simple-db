package org.example.simpledb.article;

import lombok.Data;

import java.time.LocalDateTime;

@Data
public class Article {

    private Long id;

    private String title;

    private String body;

    private LocalDateTime createdDate;

    private LocalDateTime modifiedDate;

    private Boolean isBlind;
}
