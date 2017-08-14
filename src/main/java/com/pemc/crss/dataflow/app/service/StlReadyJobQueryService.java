package com.pemc.crss.dataflow.app.service;

import com.pemc.crss.dataflow.app.support.PageableRequest;
import com.pemc.crss.shared.core.dataflow.dto.DistinctStlReadyJob;
import org.springframework.data.domain.Page;

public interface StlReadyJobQueryService {

    Page<DistinctStlReadyJob> findDistinctStlReadyJobsForMarketFee(final PageableRequest pageableRequest);

    Page<DistinctStlReadyJob> findDistinctStlReadyJobsForTradingAmounts(final PageableRequest pageableRequest);
}
