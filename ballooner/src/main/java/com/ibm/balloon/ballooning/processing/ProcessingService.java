package com.ibm.balloon.ballooning.processing;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.ibm.balloon.ballooning.data.BalloonFactory;
import com.ibm.balloon.ballooning.data.BalloonStrategyEnum;
import com.ibm.balloon.ballooning.flatter.FlatterClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class ProcessingService {
    private final FlatterClient flatterClient;

    @Value("${ballooning.processing.output.host}")
    private String host;

    @Value("${ballooning.processing.output.buffer}")
    private int bufferSize;

    @Value("${ballooning.processing.output.objectSeparator}")
    private String separator;

    public String printProbability(BalloonStrategyEnum balloonStrategyEnum) throws IOException {
        final BalloonFactory factory = new BalloonFactory(balloonStrategyEnum);
        return factory.showRootProbability();
    }

    @Async
    public void processing(
            BalloonStrategyEnum balloonStrategyEnum,
            int socketPort,
            int recordsToSend,
            int recordsPerPackage,
            int sleepIntervalInSeconds
    ) throws Exception {
        log.info("Processing data...");
        Thread.sleep(2000L); // Wait for socket to open
        log.info("Strategy: {}, port: {}", balloonStrategyEnum, socketPort);

        final BalloonFactory factory = new BalloonFactory(balloonStrategyEnum);
        final ObjectWriter objectWriter = new ObjectMapper().writer();

        StringBuffer stringBuffer = new StringBuffer();
        List<Thread> threads = new ArrayList<>();
        int numberOfThreads = Runtime.getRuntime().availableProcessors();
        int recordsPerThread = recordsPerPackage / numberOfThreads;
        log.info("All records: {}; " +
                "Records per package: {}; " +
                "Using treads: {}; " +
                "Each thread produces: {} records per package", recordsToSend, recordsPerPackage, numberOfThreads, recordsPerThread
        );

        try (Socket requestSocket = new Socket(host, socketPort);
             BufferedWriter out = new BufferedWriter(new OutputStreamWriter(requestSocket.getOutputStream()), bufferSize)
        ) {
            int recordsAlreadySent = 0;
            while (recordsAlreadySent < recordsToSend) {
                for (int i = 0; i < numberOfThreads; i++) {
                    Thread thread = new Thread(() -> {
                        for (int j = 0; j < recordsPerThread; j++) {
                            try {
                                String json = objectWriter.writeValueAsString(factory.generateObject());
                                stringBuffer.append(String.format("%s%s", json, separator));
                            } catch (JsonProcessingException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    });
                    threads.add(thread);
                    thread.start();
                }

                for (Thread thread : threads) {
                    thread.join();
                }

                out.write(stringBuffer.toString());
                out.flush();

                recordsAlreadySent += recordsPerPackage;
                stringBuffer.setLength(0);
                TimeUnit.MILLISECONDS.sleep(1000L * sleepIntervalInSeconds);
            }
        }
        log.info("End of processing");
    }

    public String connection(BalloonStrategyEnum balloonStrategyEnum, Integer socketPort, Integer serverPort) {
        return flatterClient.openPort(balloonStrategyEnum, socketPort, serverPort);
    }
}
