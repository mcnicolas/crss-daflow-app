package com.pemc.crss.dataflow.app.service;


import com.pemc.crss.shared.commons.reference.MeterProcessType;
import com.pemc.crss.shared.core.dataflow.entity.ViewStlDailyStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import java.time.LocalDateTime;

public interface StlDailyStatusService {

    Page<ViewStlDailyStatus> getByBillingPeriod(LocalDateTime billPeriodStart, LocalDateTime billPeriodEnd,
                                                String groupId, String regionGroup, MeterProcessType processType,
                                                Pageable pageable);
}
