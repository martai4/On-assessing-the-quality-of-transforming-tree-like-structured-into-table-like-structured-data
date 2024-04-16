package com.ibm.balloon.ballooning.processing;

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
            int port,
            int durationInSeconds,
            int recordsPerPackage,
            int sleepIntervalInSeconds
    ) throws Exception {
        log.info("Processing data...");
        log.info("Strategy: {}, port: {}", balloonStrategyEnum, port);

        final BalloonFactory factory = new BalloonFactory(balloonStrategyEnum);
        final ObjectWriter objectWriter = new ObjectMapper().writer().withDefaultPrettyPrinter();

        Socket requestSocket = new Socket(host, port);
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(requestSocket.getOutputStream()), bufferSize);

        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() < startTime + (1000L * durationInSeconds)) {
            for (int i = 0; i < recordsPerPackage; i++) {
                String json = objectWriter.writeValueAsString(factory.generateObject());

                out.write(String.format("%s%s", json, separator));
                out.flush();
            }

            TimeUnit.MILLISECONDS.sleep(1000L * sleepIntervalInSeconds);
        }

        out.close();
        requestSocket.close();
    }

    public String connection(Integer port) {
        return flatterClient.openPort(port);
    }

    //@PostConstruct // todo del later
    public void test() throws Exception {
        BalloonStrategyEnum balloonStrategyEnum = BalloonStrategyEnum.MOVIES;

//        final BalloonFactory factory = new BalloonFactory(balloonStrategyEnum);
//        System.out.println(factory.showRootProbability());
//
//        int limit = 20;
//        for (int i = 0; i < limit; i++) {
//            System.out.println(factory.generateObject());
//        }
    }
}
