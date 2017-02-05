package com.pemc.crss.restresource.app.service;

import com.pemc.crss.restresource.app.dto.GraphDto;
import com.pemc.crss.restresource.app.dto.GroupDto;
import com.pemc.crss.restresource.app.dto.MTNDto;

import java.util.Date;
import java.util.List;

/**
 * Created on 1/10/17.
 */
public interface RTUComparisonService {
    public static final String DAILY = "Daily";
    public static final String MONTHLY = "Monthly";
    public static final String RTU = "RTU";
    public static final String MTN = "MTN";
    public static final String X_AXIS = "x";
    public static final String Y_AXIS = "y";

    List<MTNDto> getAllMTNGroupConfigs();

    GraphDto getRTUData(Date startDate, Date endDate, String mtn);

    GraphDto getRawData(Date startDate, Date endDate, String meterDataType, String mtnConfigName);

    Boolean saveShift(Date effDate, String mtnGroupName);

    GraphDto getRawAndRTUData(Date startDate, Date endDate, String meterDataType, String mtn, String mtnConfigName);

    List<Object[]> getRTU(Date startDate, Date endDate, String mtn);

    List<Object[]> getRaw(Date startDate, Date endDate, String meterDataType, String mtn);

    List<GroupDto> getGroupsByMtn(String mtn);

    Boolean applyShift(String mtn, Date startDate, Date endDate);

    List<GroupDto> getSavedGroupSchedulesByMtn(String mtn, Date startDate, Date endDate);

    List<Object[]> getMTNDataShift(Date startDate, Date endDate, String mtnGroupName, Date effDate, String meterDataType, String mtn);
}
