package com.pemc.crss.restresource.app.service;

import java.util.Date;

public interface DailyStlDerivedService {

    Boolean saveStlReady(Long executionId, Date targetDate);
}
