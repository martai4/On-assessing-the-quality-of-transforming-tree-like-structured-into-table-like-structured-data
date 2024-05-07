package com.ibm.balloon.ballooning.processing.dto;

import com.ibm.balloon.ballooning.data.BalloonStrategyEnum;
import com.ibm.balloon.ballooning.processing.ProcessingStrategyEnum;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class TestSocketDto {
    private BalloonStrategyEnum datasetStrategy;
    private ProcessingStrategyEnum processingStrategy;
    private Integer recordsToSend, recordsPerPackage;
    private Integer socketPort, serverPort;
    private String outputFilename;
}
