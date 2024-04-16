package com.ibm.balloon.ballooning.data.knowledgebase.nasa;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MeteoriteConfig {
    public static String FILE_PATH, TEST_FILE_PATH;

    @Value("${ballooning.data.filePath.meteorite}")
    private String filePath;

    @Value("${ballooning.data.filePath.meteoriteTest}")
    private String testFilePath;

    @PostConstruct
    public void init() {
        FILE_PATH = filePath;
        TEST_FILE_PATH = testFilePath;
    }
}
