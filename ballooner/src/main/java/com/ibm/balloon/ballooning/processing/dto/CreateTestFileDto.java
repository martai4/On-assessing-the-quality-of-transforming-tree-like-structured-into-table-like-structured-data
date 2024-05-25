package com.ibm.balloon.ballooning.processing.dto;

import com.ibm.balloon.ballooning.data.BalloonStrategyEnum;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class CreateTestFileDto {
    private BalloonStrategyEnum balloonStrategyEnum;
    private Integer size;
}
