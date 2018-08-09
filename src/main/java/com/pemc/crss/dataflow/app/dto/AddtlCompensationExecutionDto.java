package com.pemc.crss.dataflow.app.dto;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class AddtlCompensationExecutionDto extends BaseTaskExecutionDto {

    private DistinctAddtlCompDto distinctAddtlCompDto;

    private List<AddtlCompensationExecDetailsDto> addtlCompensationExecDetailsDtos;

    public DistinctAddtlCompDto getDistinctAddtlCompDto() {
        return distinctAddtlCompDto;
    }

    public void setDistinctAddtlCompDto(DistinctAddtlCompDto distinctAddtlCompDto) {
        this.distinctAddtlCompDto = distinctAddtlCompDto;
    }

    public List<AddtlCompensationExecDetailsDto> getAddtlCompensationExecDetailsDtos() {
        return addtlCompensationExecDetailsDtos;
    }

    public void setAddtlCompensationExecDetailsDtos(List<AddtlCompensationExecDetailsDto> addtlCompensationExecDetailsDtos) {
        this.addtlCompensationExecDetailsDtos = addtlCompensationExecDetailsDtos;
    }

    public boolean isCanFinalize() {
        LocalDateTime calcGmrStart = Optional.ofNullable(distinctAddtlCompDto).map(DistinctAddtlCompDto::getCalcGmrStartDate)
                .orElse(null);

        LocalDateTime latestAcCalc = addtlCompensationExecDetailsDtos.stream().map(AddtlCompensationExecDetailsDto::getRunStartDate)
                .sorted(Collections.reverseOrder()).findFirst().orElse(null);

        // GMR Calc must be the latest run
        return calcGmrStart != null && latestAcCalc != null && calcGmrStart.isAfter(latestAcCalc);
    }
}
