package com.pemc.crss.dataflow.app.service;

import com.pemc.crss.dataflow.app.support.PageableRequest;
import com.pemc.crss.shared.commons.reference.MeterProcessType;
import com.pemc.crss.shared.core.dataflow.dto.DistinctStlReadyJob;
import com.pemc.crss.shared.core.dataflow.entity.ViewSettlementJob;
import org.springframework.data.domain.Page;

import java.util.List;

public interface StlReadyJobQueryService {

    Page<DistinctStlReadyJob> findDistinctStlReadyJobsForMarketFee(final PageableRequest pageableRequest);

    Page<DistinctStlReadyJob> findDistinctStlReadyJobsForTradingAmounts(final PageableRequest pageableRequest);

    List<ViewSettlementJob> getStlReadyJobsByParentIdAndProcessTypeAndRegionGroup(final MeterProcessType processType,
                                                                                  final String parentId,
                                                                                  final String regionGroup);
}
