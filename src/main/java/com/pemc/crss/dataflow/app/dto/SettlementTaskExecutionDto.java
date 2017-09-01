package com.pemc.crss.dataflow.app.dto;

import com.pemc.crss.dataflow.app.dto.parent.StubTaskExecutionDto;
import com.pemc.crss.shared.commons.reference.MeterProcessType;
import lombok.Data;
import org.springframework.batch.core.BatchStatus;

import java.util.Date;
import java.util.Map;

@Data
public class SettlementTaskExecutionDto extends StubTaskExecutionDto {

    private Long parentId;

    private String stlReadyGroupId;

    private String status;

    private Date runDateTime;

    private BatchStatus stlReadyStatus;

    private StlJobGroupDto parentStlJobGroupDto;

    private Map<String, StlJobGroupDto> stlJobGroupDtoMap;

    private Date billPeriodStartDate;

    private Date billPeriodEndDate;

    private Date dailyDate;

    private MeterProcessType processType;

    private String billPeriodStr;
}
