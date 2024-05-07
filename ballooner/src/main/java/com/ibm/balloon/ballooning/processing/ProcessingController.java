package com.ibm.balloon.ballooning.processing;

import com.ibm.balloon.ballooning.data.BalloonStrategyEnum;
import com.ibm.balloon.ballooning.processing.dto.TestSocketDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.Optional;

@Slf4j
@RequiredArgsConstructor
@RestController
@RequestMapping("/api/v1/ballooning")
public class ProcessingController {
    private final ProcessingService service;

    @Value("${ballooning.processing.defaultValue.recordsToSend}")
    private int defaultRecordsToSend;

    @Value("${ballooning.processing.defaultValue.recordsPerPackage}")
    private int defaultRecordsPerPackage;

    @Value("${ballooning.processing.defaultValue.sleepIntervalInSeconds}")
    private int defaultSleepIntervalInSeconds;

    @PostMapping("/probability")
    public String printProbability(@RequestParam BalloonStrategyEnum strategy) throws IOException {
        return service.printProbability(strategy);
    }

    @PostMapping("/balloon-test")
    public ResponseEntity<String> balloonTest(
            @RequestParam Optional<Integer> recordsToSend,
            @RequestParam Optional<Integer> recordsPerPackage,
            @RequestParam Optional<Integer> sleepIntervalInSeconds,
            @RequestBody TestSocketDto dto
    ) {
        try {
            log.info("[ProcessingController] Opening connection to: {}", dto.getSocketPort());
            String response = service.connection(dto.getStrategy(), dto.getSocketPort(), dto.getServerPort());

            log.info("[ProcessingController] [{}] Processing...", response);
            service.processing(
                    dto.getStrategy(),
                    dto.getSocketPort(),
                    recordsToSend.orElse(defaultRecordsToSend),
                    recordsPerPackage.orElse(defaultRecordsPerPackage),
                    sleepIntervalInSeconds.orElse(defaultSleepIntervalInSeconds)
            );

            return ResponseEntity.ok("Processing request on port " + dto.getSocketPort());
        } catch (Exception e) {
            log.error("Error during test execution!", e);
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(e.getMessage());
        }
    }
}
