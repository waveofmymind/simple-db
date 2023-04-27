package org.example.simpledb.article;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Column {
    String type() default "VARCHAR(255)";
    boolean nullable() default false;
    String defaultValue() default "";
}
