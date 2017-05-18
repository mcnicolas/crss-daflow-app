package com.pemc.crss.dataflow.app.dto;

import java.util.List;

/**
 * Created by kacejo on 5/16/17.
 */
public class AddtlCompensationRunListDto {
    private List<AddtlCompensationRunDto> addtlCompensationRunDtos;

    public List<AddtlCompensationRunDto> getAddtlCompensationRunDtos() {
        return addtlCompensationRunDtos;
    }

    public void setAddtlCompensationRunDtos(List<AddtlCompensationRunDto> addtlCompensationRunDtos) {
        this.addtlCompensationRunDtos = addtlCompensationRunDtos;
    }
}
