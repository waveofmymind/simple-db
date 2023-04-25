package org.example.simpledb.article;

import java.time.LocalDateTime;

public record ArticleDto(
        Long id,
        String title,
        String body,
        LocalDateTime createdDate,
        LocalDateTime modifiedDate,
        Boolean isBlind) {
}
