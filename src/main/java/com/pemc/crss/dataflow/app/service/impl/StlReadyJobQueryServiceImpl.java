package com.pemc.crss.dataflow.app.service.impl;

import com.pemc.crss.dataflow.app.service.StlReadyJobQueryService;
import com.pemc.crss.dataflow.app.support.PageableRequest;
import com.pemc.crss.dataflow.app.support.StlQueryProcessType;
import com.pemc.crss.shared.commons.reference.MeterProcessType;
import com.pemc.crss.shared.commons.util.DateUtil;
import com.pemc.crss.shared.core.dataflow.dto.DistinctStlReadyJob;
import com.pemc.crss.shared.core.dataflow.entity.ViewSettlementJob;
import com.pemc.crss.shared.core.dataflow.repository.ViewSettlementJobRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.pemc.crss.shared.commons.reference.MeterProcessType.*;

@Slf4j
@Service
public class StlReadyJobQueryServiceImpl implements StlReadyJobQueryService {

    private static final String WILD_CARD = "%";
    private static final String DEFAULT_TRADING_DATE_START = "01/01/1970";
    private static final String DEFAULT_TRADING_DATE_END = "12/31/2099";
    private static final String UI_DATE_PATTERN = "MM/dd/yyyy";

    @Autowired
    private ViewSettlementJobRepository viewSettlementJobRepository;

    @Override
    public Page<DistinctStlReadyJob> findDistinctStlReadyJobsForMarketFee(final PageableRequest pageableRequest) {
        final Pageable pageable = pageableRequest.getPageable();
        StlQueryProcessType processType = resolveProcessType(pageableRequest.getMapParams());

        List<MeterProcessType> processTypes = new ArrayList<>();
        switch (processType) {
            case FINAL:
                processTypes.add(FINAL);
                break;
            case PRELIM:
                processTypes.add(PRELIM);
                break;
            case ADJUSTED:
                processTypes.add(ADJUSTED);
                break;
            case ALL:
            case ALL_MONTHLY:
            default:
                processTypes.addAll(Arrays.asList(PRELIM, ADJUSTED, FINAL));
                break;
        }

        String billingPeriod = resolveBillingPeriod(pageableRequest.getMapParams());

        return viewSettlementJobRepository.findDistinctStlReadyJobs(processTypes, billingPeriod, pageable);
    }

    @Override
    public Page<DistinctStlReadyJob> findDistinctStlReadyJobsForTradingAmounts(PageableRequest pageableRequest) {
        final Pageable pageable = pageableRequest.getPageable();
        Map<String, String> mapParams = pageableRequest.getMapParams();

        StlQueryProcessType stlQueryProcessType = resolveProcessType(mapParams);

        String billingPeriod = resolveBillingPeriod(mapParams);

        List<MeterProcessType> processTypes = new ArrayList<>();
        switch (stlQueryProcessType) {
            case FINAL:
                processTypes.add(FINAL);
                break;
            case PRELIM:
                processTypes.add(PRELIM);
                break;
            case ADJUSTED:
                processTypes.add(ADJUSTED);
                break;
            case ALL_MONTHLY:
                processTypes.addAll(Arrays.asList(PRELIM, ADJUSTED, FINAL));
                break;
            case ALL:
            default:
                billingPeriod = WILD_CARD;
                processTypes.addAll(Arrays.asList(PRELIM, ADJUSTED, FINAL, DAILY));
                break;
        }


        if (stlQueryProcessType == StlQueryProcessType.DAILY) {
            LocalDate tradingStartDate = parseTradingDateForQuery(mapParams, "tradingDateStart", DEFAULT_TRADING_DATE_START);
            LocalDate tradingEndDate = parseTradingDateForQuery(mapParams, "tradingDateEnd", DEFAULT_TRADING_DATE_END);

            return viewSettlementJobRepository.findDistinctStlReadyJobsDailyFilter(tradingStartDate.atStartOfDay(),
                    tradingEndDate.atStartOfDay(), pageable);
        } else {
            return viewSettlementJobRepository.findDistinctStlReadyJobs(processTypes, billingPeriod, pageable);
        }
    }

    @Override
    public List<ViewSettlementJob> getStlReadyJobsByParentIdAndProcessType(final MeterProcessType processType,
                                                                           final String parentId) {
        return viewSettlementJobRepository.getStlReadyJobsByParentIdAndProcessType(processType, parentId);
    }

    private StlQueryProcessType resolveProcessType(final Map<String, String> mapParams) {
        String processType = mapParams.getOrDefault("processType", null);

        if (processType == null) {
            processType = "ALL";
        }

        return StlQueryProcessType.valueOf(processType);
    }

    private String resolveBillingPeriod(final Map<String, String> mapParams) {
        String startDate = mapParams.getOrDefault("startDate", null);
        String endDate = mapParams.getOrDefault("endDate", null);

        if (startDate == null || endDate == null) {
            return WILD_CARD;
        }

        try {

            return formatDateStrForFilter(startDate) + formatDateStrForFilter(endDate);
        } catch (Exception e) {
            log.warn("Encountered error parsing provided startDate {}, endDate {}", startDate, endDate);

            return WILD_CARD;
        }
    }

    private String formatDateStrForFilter(final String date) {
        String year = date.split("/")[2].substring(2);
        String month = date.split("/")[0];
        String day = date.split("/")[1];

        return year + month + day;
    }

    private String getStringValFromMap(final Map<String, String> mapParams, final String key, final String defaultValue) {
        return mapParams.containsKey(key) && mapParams.get(key) != null ? mapParams.get(key) : defaultValue;
    }


    private LocalDate parseTradingDateForQuery(final Map<String, String> mapParams, final String key, final String defaultDate) {
        String tradingDateStr = getStringValFromMap(mapParams, key, defaultDate);
        LocalDate localDate = DateUtil.parseLocalDate(tradingDateStr, UI_DATE_PATTERN);

        return localDate != null ? localDate : DateUtil.parseLocalDate(defaultDate, UI_DATE_PATTERN);

    }
}
