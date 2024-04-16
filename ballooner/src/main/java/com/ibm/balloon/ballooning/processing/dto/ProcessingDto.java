package com.ibm.balloon.ballooning.processing.dto;

import com.ibm.balloon.ballooning.data.BalloonStrategyEnum;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class ProcessingDto {
    private BalloonStrategyEnum strategy;
}
