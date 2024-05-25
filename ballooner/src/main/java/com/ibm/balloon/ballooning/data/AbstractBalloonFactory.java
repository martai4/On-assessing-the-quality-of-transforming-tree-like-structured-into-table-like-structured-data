package com.ibm.balloon.ballooning.data;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component
@DependsOn("balloonStrategyConfig")
public class AbstractBalloonFactory {
    private final Map<BalloonStrategyEnum, BalloonFactory> factoryMap = new HashMap<>();

    public AbstractBalloonFactory() throws IOException {
        for (BalloonStrategyEnum strategyEnum : BalloonStrategyEnum.values()) {
            factoryMap.put(strategyEnum, new BalloonFactory(strategyEnum));
        }
        log.info("[AbstractBalloonFactory] All factories are created!");
    }

    public BalloonFactory getFactory(BalloonStrategyEnum strategyEnum) {
        return factoryMap.get(strategyEnum);
    }
}
