package com.pemc.crss.restresource.app.service;

import com.pemc.crss.shared.commons.reference.MeterProcessType;

import java.util.Date;

public interface MonthlyStlDerivedService {

    Boolean saveStlReady(MeterProcessType processType, Long executionId, Date startDate, Date endDate);
}
