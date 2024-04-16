package com.ibm.balloon.ballooning.data.knowledgebase.movies;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MovieConfig {
    public static String FILE_PATH;

    @Value("${ballooning.data.filePath.movie}")
    private String filePath;

    @PostConstruct
    public void init() {
        FILE_PATH = filePath;
    }
}
