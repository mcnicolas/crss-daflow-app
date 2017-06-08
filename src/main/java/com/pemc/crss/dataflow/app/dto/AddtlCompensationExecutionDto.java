package com.pemc.crss.dataflow.app.dto;

import java.util.List;

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
}
