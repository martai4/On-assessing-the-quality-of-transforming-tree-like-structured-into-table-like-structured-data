package com.ibm.balloon.ballooning.flatter;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

@Component
@RequiredArgsConstructor
public class FlatterClient {
    private final WebClient flatterWebClient;

    public String openPort(Integer socketPort) {
        return flatterWebClient
                .post()
                .uri(uriBuilder -> uriBuilder
                        .path("/prepare-test/")
                        .queryParam("socket_port", socketPort)
                        .build())
                .retrieve()
                .bodyToMono(String.class)
                .onErrorResume(throwable -> {
                    throw new RuntimeException(throwable);
                })
                .block();
    }
}
