package com.ibm.balloon.ballooning.flatter;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

@Component
@RequiredArgsConstructor
public class FlatterClient {
    private final WebClient flatterWebClient;

    public String openPort(Integer socketPort, Integer serverPort) {
        return flatterWebClient
                .post()
                .uri(String.format("/socket-test/%d/%d", socketPort, serverPort))
                .retrieve()
                .bodyToMono(String.class)
                .onErrorResume(throwable -> {
                    throw new RuntimeException(throwable);
                })
                .block();
    }
}
