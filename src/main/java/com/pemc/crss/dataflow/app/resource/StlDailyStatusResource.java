package com.pemc.crss.dataflow.app.resource;

import com.pemc.crss.dataflow.app.dto.StlDailyStatusDisplay;
import com.pemc.crss.dataflow.app.service.StlDailyStatusService;
import com.pemc.crss.dataflow.app.support.PageableRequest;
import com.pemc.crss.shared.commons.reference.MeterProcessType;
import com.pemc.crss.shared.commons.util.DateUtil;
import com.pemc.crss.shared.core.dataflow.entity.ViewStlDailyStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/stl-daily-status")
public class StlDailyStatusResource {


    @Autowired
    private StlDailyStatusService stlDailyStatusService;

    @PostMapping("/list")
    public ResponseEntity<Page<StlDailyStatusDisplay>> getAllByBillingPeriod(@RequestBody PageableRequest pageRequest) {

        String billPeriodStartStr = pageRequest.getMapParams().getOrDefault("billPeriodStart", null);
        String billPeriodEndStr = pageRequest.getMapParams().getOrDefault("billPeriodEnd", null);
        String processType = pageRequest.getMapParams().getOrDefault("processType", null);
        String groupId = pageRequest.getMapParams().getOrDefault("groupId", null);
        String regionGroup = pageRequest.getMapParams().getOrDefault("regionGroup", null);
        Pageable pageable = pageRequest.getPageable();

        LocalDateTime billPeriodStart = DateUtil.parseStringDateToLocalDateTime(billPeriodStartStr, DateUtil.DEFAULT_DATE_FORMAT);
        LocalDateTime billPeriodEnd = DateUtil.parseStringDateToLocalDateTime(billPeriodEndStr, DateUtil.DEFAULT_DATE_FORMAT);
        MeterProcessType meterProcessType = MeterProcessType.get(processType);

        Page<ViewStlDailyStatus> dailyStatusPage = stlDailyStatusService.getByBillingPeriod(billPeriodStart, billPeriodEnd,
                groupId, regionGroup, meterProcessType, pageable);

        List<StlDailyStatusDisplay> result = dailyStatusPage.getContent().stream().map(StlDailyStatusDisplay::new)
                .collect(Collectors.toList());

        return ResponseEntity.ok(new PageImpl<>(result, pageable, dailyStatusPage.getTotalElements()));
    }

}