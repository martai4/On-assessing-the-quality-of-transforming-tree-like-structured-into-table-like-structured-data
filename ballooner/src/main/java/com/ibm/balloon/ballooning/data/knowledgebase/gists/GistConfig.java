package com.ibm.balloon.ballooning.data.knowledgebase.gists;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class GistConfig {
    public static String FILE_PATH;

    @Value("${ballooning.data.filePath.gists}")
    private String filePath;

    @PostConstruct
    public void init() {
        FILE_PATH = filePath;
    }
}
