package com.ibm.balloon.ballooning.processing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.ibm.balloon.ballooning.data.AbstractBalloonFactory;
import com.ibm.balloon.ballooning.data.BalloonFactory;
import com.ibm.balloon.ballooning.data.BalloonStrategyEnum;
import com.ibm.balloon.ballooning.data.domain.InputBalloonData;
import com.ibm.balloon.ballooning.flatter.FlatterClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class ProcessingService {
    private final FlatterClient flatterClient;
    private final AbstractBalloonFactory abstractBalloonFactory;

    @Value("${ballooning.processing.output.host}")
    private String host;

    @Value("${ballooning.processing.output.buffer}")
    private int bufferSize;

    @Value("${ballooning.processing.output.objectSeparator}")
    private String separator;

    @Value("${ballooning.processing.output.testFilesDir}")
    private String testFilesDir;

    public String printProbability(BalloonStrategyEnum balloonStrategyEnum) throws IOException {
        final BalloonFactory factory = abstractBalloonFactory.getFactory(balloonStrategyEnum);
        return factory.showRootProbability();
    }

    @Async
    public void processing(
            BalloonStrategyEnum balloonStrategyEnum,
            int socketPort,
            int recordsToSend
    ) throws Exception {
        log.info("Processing data...");
        Thread.sleep(2000L); // Wait for socket to open
        log.info("Strategy: {}, port: {}", balloonStrategyEnum, socketPort);

        final BalloonFactory factory = abstractBalloonFactory.getFactory(balloonStrategyEnum);
        final ObjectWriter objectWriter = new ObjectMapper().writer();

        List<Thread> threads = new ArrayList<>();
        int numberOfThreads = Runtime.getRuntime().availableProcessors();
        int recordsPerThread = recordsToSend / numberOfThreads;
        log.info("All records: {}; " +
                "Using treads: {}; " +
                "Each thread produces: {} records per package", recordsToSend, numberOfThreads, recordsPerThread
        );

        try (Socket requestSocket = new Socket(host, socketPort);
             BufferedWriter out = new BufferedWriter(new OutputStreamWriter(requestSocket.getOutputStream()), bufferSize)
        ) {
            for (int i = 0; i < numberOfThreads; i++) {
                Thread thread = new Thread(() -> {
                    for (int j = 0; j < recordsPerThread; j++) {
                        try {
                            String json = objectWriter.writeValueAsString(factory.generateObject());
                            out.write(json + separator);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });

                threads.add(thread);
                thread.start();
            }

            out.flush();
            for (Thread thread : threads) {
                thread.join();
            }
        }

        log.info("End of processing");
    }

    public String connection(ProcessingStrategyEnum strategy, Integer socketPort, Integer serverPort, String filename) {
        return flatterClient.openPort(strategy, socketPort, serverPort, filename);
    }

    public String createTestFile(BalloonStrategyEnum balloonStrategyEnum, int size) throws IOException {
        final BalloonFactory factory = abstractBalloonFactory.getFactory(balloonStrategyEnum);
        final String filename = String.format("%s/%s-%s.txt", testFilesDir, balloonStrategyEnum, size);

        ObjectMapper objectMapper = new ObjectMapper();

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename))) {
            writer.write("[\n");
            for (int i = 0; i < size; i++) {
                InputBalloonData generatedObject = factory.generateObject();
                String jsonObject = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(generatedObject);

                writer.write(jsonObject);
                if (i < size - 1) {
                    writer.write(",\n");
                }
            }
            writer.write("\n]");
        }

        return String.format("File %s created.", filename);
    }
}
