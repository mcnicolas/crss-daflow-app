package com.pemc.crss.dataflow.app.service.impl;

import com.pemc.crss.dataflow.app.service.StlDailyStatusService;
import com.pemc.crss.shared.commons.reference.MeterProcessType;
import com.pemc.crss.shared.core.dataflow.entity.ViewStlDailyStatus;
import com.pemc.crss.shared.core.dataflow.repository.ViewStlDailyStatusRepository;
import com.querydsl.core.BooleanBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

import static com.pemc.crss.shared.core.dataflow.entity.QViewStlDailyStatus.viewStlDailyStatus;

@Slf4j
@Service
public class StlDailyStatusServiceImpl implements StlDailyStatusService {

    @Autowired
    private ViewStlDailyStatusRepository viewStlDailyStatusRepository;

    @Override
    public Page<ViewStlDailyStatus> getByBillingPeriod(LocalDateTime billPeriodStart,
                                                       LocalDateTime billPeriodEnd,
                                                       String groupId,
                                                       MeterProcessType processType,
                                                       Pageable pageable) {

        BooleanBuilder predicate = new BooleanBuilder()
            .and(viewStlDailyStatus.groupId.eq(groupId))
            .and(viewStlDailyStatus.meterProcessType.eq(processType))
            .and(viewStlDailyStatus.tradingDate.between(billPeriodStart, billPeriodEnd));

        return viewStlDailyStatusRepository.findAll(predicate, pageable);
    }
}
