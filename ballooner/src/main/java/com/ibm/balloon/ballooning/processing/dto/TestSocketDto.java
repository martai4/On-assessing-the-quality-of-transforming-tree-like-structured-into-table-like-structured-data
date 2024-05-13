package com.ibm.balloon.ballooning.processing.dto;

import com.ibm.balloon.ballooning.data.BalloonStrategyEnum;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class TestSocketDto {
    private BalloonStrategyEnum datasetStrategy;
    private Integer recordsToSend;
    private Integer socketPort;
}
