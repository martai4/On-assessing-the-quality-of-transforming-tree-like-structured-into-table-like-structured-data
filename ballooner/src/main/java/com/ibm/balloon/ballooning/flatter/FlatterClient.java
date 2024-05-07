package com.ibm.balloon.ballooning.flatter;

import com.ibm.balloon.ballooning.data.BalloonStrategyEnum;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

@Component
@RequiredArgsConstructor
public class FlatterClient {
    private final WebClient flatterWebClient;

    public String openPort(BalloonStrategyEnum strategy, Integer socketPort, Integer serverPort) {
        return flatterWebClient
                .post()
                .uri(uriBuilder -> uriBuilder
                        .path("/socket-test/")
                        .queryParam("strategy", strategy)
                        .queryParam("socket_port", socketPort)
                        .queryParam("server_port", serverPort)
                        .build())
                .retrieve()
                .bodyToMono(String.class)
                .onErrorResume(throwable -> {
                    throw new RuntimeException(throwable);
                })
                .block();
    }
}
