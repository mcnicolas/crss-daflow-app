package com.pemc.crss.restresource.app.service.impl;

import com.pemc.crss.restresource.app.dto.ConfigDto;
import com.pemc.crss.restresource.app.dto.GraphDto;
import com.pemc.crss.restresource.app.dto.GroupDto;
import com.pemc.crss.restresource.app.dto.MTNDto;
import com.pemc.crss.restresource.app.service.RTUComparisonService;
import com.pemc.crss.meterprocess.core.main.entity.mtn.*;
import com.pemc.crss.meterprocess.core.main.repository.*;
import com.pemc.crss.shared.core.nmms.entity.ActualDispatchData;
import com.pemc.crss.shared.core.nmms.repository.ActualDispatchDataRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

/**
 * Created on 1/10/17.
 */
@Service
@Transactional("mainTransactionManager")
public class RTUComparisonServiceImpl implements RTUComparisonService {

    @Autowired
    MTNModelGroupRepository groupRepository;

    @Autowired
    MTNModelConfigRepository configRepository;

    @Autowired
    ViewDailyMqRepository dailyMqRepository;

    @Autowired
    ViewMonthlyMqRepository monthlyMqRepository;

    @Autowired
    ActualDispatchDataRepository dispatchDataRepository;

    @Autowired
    MTNModelGroupScheduleTemporaryRepository groupScheduleTemporaryRepository;

    @Autowired
    MTNModelGroupScheduleRepository groupScheduleRepository;

    @Override
    public List<MTNDto> getAllMTNGroupConfigs() {

        List<MTNDto> list = new ArrayList<>();

        //List<MTNModelGroup> groups = groupRepository.findAll();

        List<Object[]> objects = configRepository.findDistinctMtnNamesWithGroup();
        for (Object[] o : objects) {
            if (o[1] != null) {
                MTNDto bean = new MTNDto();
                bean.setMtnGroupName(o[1].toString());
                bean.setMtn(o[0].toString());
                list.add(bean);
            }
        }
        return list;
    }

    @Override
    public GraphDto getRTUData(Date startDate, Date endDate, String mtn) {
        GraphDto result = new GraphDto();
        ActualDispatchData[] objects = dispatchDataRepository.findDispatchDataByMTNAndDates(convert(startDate), convert(endDate), mtn);
        Double[] y = new Double[objects.length];
        String[] x = new String[objects.length];

        for (int i = 0; i < objects.length; i++) {
            x[i] = ((ActualDispatchData) objects[i]).getDispatchInterval().toString();
            y[i] = ((ActualDispatchData) objects[i]).getMw().doubleValue();
        }

        result.setxAxis(x);
        result.setyAxisRTU(y);

        return result;
    }

    @Override
    public GraphDto getRawData(Date startDate, Date endDate, String meterDataType, String mtnConfigName) {
        GraphDto result = new GraphDto();
        List<Object[]> objects = new ArrayList();
        if (DAILY.equalsIgnoreCase(meterDataType)) {
            objects = dailyMqRepository.findRawMqByMtnAndDates(startDate, endDate, mtnConfigName);
        } else {
            objects = monthlyMqRepository.findRawMqByMtnAndDates(startDate, endDate, mtnConfigName);
        }

        Double[] y = new Double[objects.size()];
        String[] x = new String[objects.size()];

        for (int i = 0; i < objects.size(); i++) {
            x[i] = objects.get(i)[0].toString();
            y[i] = (Double) objects.get(i)[1];
        }

        result.setxAxis(x);
        result.setyAxisMTN(y);

        return result;
    }

    @Override
    public Boolean saveShift(Date effDate, String mtnGroupName) {
        Boolean result = Boolean.TRUE;
        try {
            MTNModelGroupScheduleTemporary sched = new MTNModelGroupScheduleTemporary();
            sched.setEffectiveStartDate(effDate);
            sched.setMtnModelGroup(groupRepository.findByGroupName(mtnGroupName));
            groupScheduleTemporaryRepository.save(sched);
        } catch (Exception e) {
            result = Boolean.FALSE;
        }
        return result;
    }

    @Override
    public GraphDto getRawAndRTUData(Date startDate, Date endDate, String meterDataType, String mtn, String mtnConfigName) {
        GraphDto result = new GraphDto();

        ActualDispatchData[] objects = dispatchDataRepository.findDispatchDataByMTNAndDates(convert(startDate), convert(endDate), mtn);
        Double[] y = new Double[objects.length];
        Double[] y2 = new Double[objects.length];
        String[] x = new String[objects.length];

        for (int i = 0; i < objects.length; i++) {
            x[i] = ((ActualDispatchData) objects[i]).getDispatchInterval().toString();
            y[i] = ((ActualDispatchData) objects[i]).getMw().doubleValue();
        }

        List<Object[]> objects2 = new ArrayList();
        if (DAILY.equalsIgnoreCase(meterDataType)) {
            objects2 = dailyMqRepository.findRawMqByMtnAndDates(startDate, endDate, mtnConfigName);
        } else {
            objects2 = monthlyMqRepository.findRawMqByMtnAndDates(startDate, endDate, mtnConfigName);
        }

        for (int i = 0; i < objects2.size(); i++) {
            y2[i] = (Double) objects2.get(i)[1];
        }

        result.setxAxis(x);
        result.setyAxisRTU(y);
        result.setyAxisMTN(y2);
        return result;
    }

    @Override
    public List<Object[]> getRTU(Date startDate, Date endDate, String mtn) {
        return dispatchDataRepository.findRTUByMTNAndDates(convert(startDate), convert(endDate), mtn);
    }

    @Override
    public List<Object[]> getRaw(Date startDate, Date endDate, String meterDataType, String mtn) {
        if (DAILY.equalsIgnoreCase(meterDataType)) {
            return dailyMqRepository.findRawMqByMtnAndDates(startDate, endDate, mtn);
        } else {
            return monthlyMqRepository.findRawMqByMtnAndDates(startDate, endDate, mtn);
        }
    }

    @Override
    public List<GroupDto> getGroupsByMtn(String mtn) {
        List<MTNModelGroup> list = groupRepository.getGroupsByMtn(mtn);
        List<GroupDto> retList = new ArrayList<>();
        for (MTNModelGroup group : list) {
            GroupDto bean = new GroupDto();
            bean.setGroupName(group.getGroupName());
            for (MTNModelConfig conf : group.getMtnModelConfigList()) {
                bean.addConfig(new ConfigDto(conf.getMtnConfigName(), conf.getMtnName()));
            }
            retList.add(bean);
        }
        return retList;
    }

    @Override
    public Boolean applyShift(String mtn, Date startDate, Date endDate) {
        Boolean result = Boolean.TRUE;
        try {
            List<MTNModelGroupScheduleTemporary> scheds = groupScheduleTemporaryRepository.findByMtnAndDates(mtn, startDate, endDate);

            List<MTNModelGroupSchedule> list = new ArrayList<>();
            for (MTNModelGroupScheduleTemporary sched : scheds) {
                MTNModelGroupSchedule bean = new MTNModelGroupSchedule();
                bean.setEffectiveStartDate(sched.getEffectiveStartDate());
                bean.setMtnModelGroup(sched.getMtnModelGroup());
                list.add(bean);
            }
            groupScheduleRepository.save(list);
            groupScheduleTemporaryRepository.delete(scheds);
        } catch (Exception e) {
            result = Boolean.FALSE;
        }
        return result;
    }

    @Override
    public List<GroupDto> getSavedGroupSchedulesByMtn(String mtn, Date startDate, Date endDate) {
        List<GroupDto> retList = new ArrayList<>();
        List<MTNModelGroupScheduleTemporary> scheds = groupScheduleTemporaryRepository.findByMtnAndDates(mtn, startDate, endDate);
        for (MTNModelGroupScheduleTemporary sched : scheds) {
            GroupDto bean = new GroupDto();
            bean.setGroupName(sched.getMtnModelGroup().getGroupName());
            bean.setEffDatetime(sched.getEffectiveStartDate());
            retList.add(bean);
        }
        return retList;
    }

    @Override
    public List<Object[]> getMTNDataShift(Date startDate, Date endDate, String mtnGroupName, Date effStartDate, String meterDataType, String mtn) {
        MTNModelGroupScheduleTemporary in = new MTNModelGroupScheduleTemporary();
        in.setEffectiveStartDate(effStartDate);
        MTNModelGroup inGroup = new MTNModelGroup();
        inGroup.setGroupName(mtnGroupName);
        in.setMtnModelGroup(inGroup);

        List<MTNModelGroupScheduleTemporary> tempScheds = groupScheduleTemporaryRepository.findByMtnAndDates(mtn, startDate, endDate);
        tempScheds.add(in);

        Collections.sort(tempScheds, new Comparator<MTNModelGroupScheduleTemporary>() {
            @Override
            public int compare(MTNModelGroupScheduleTemporary t0, MTNModelGroupScheduleTemporary t1) {
                if (t0.getEffectiveStartDate() == null || t1.getEffectiveStartDate() == null)
                    return 0;
                return t0.getEffectiveStartDate().compareTo(t1.getEffectiveStartDate());
            }
        });

        List<Object[]> retList = new ArrayList<>();
        if (!tempScheds.isEmpty()) {
            retList = getRaw(startDate, tempScheds.get(0).getEffectiveStartDate(), meterDataType, mtn);

            if (tempScheds.size() > 1) {
                for (int i = 1; i < tempScheds.size(); i++) {
                    retList.addAll(getRawShift(tempScheds.get(i - 1), tempScheds.get(i).getEffectiveStartDate(), meterDataType, mtn));
                }
            }

            retList.addAll(getRawShift(tempScheds.get(tempScheds.size() - 1), endDate, meterDataType, mtn));
        } else {
            retList = getRaw(startDate, endDate, meterDataType, mtn);
        }

        return retList;
    }


    private List<Object[]> getRawShift(MTNModelGroupScheduleTemporary tempSched, Date endDate, String meterDataType, String mtn) {

        MTNModelConfig config = configRepository.findByGroupNameAndConfigName(tempSched.getMtnModelGroup().getGroupName(), mtn);

        List<String> seins = new ArrayList<>();
        for (MTNModelMeteringPoint mp : config.getMeteringPoints()) {
            if (!mp.getSein().contains("+")) {
                seins.add(mp.getSein());
            }
        }

        if (seins.isEmpty()) {
            return Collections.emptyList();
        }

        if (DAILY.equalsIgnoreCase(meterDataType)) {
            return dailyMqRepository.findRawMqSumBySeinsAndDates(tempSched.getEffectiveStartDate(), endDate, seins);
        } else {
            return monthlyMqRepository.findRawMqSumBySeinsAndDates(tempSched.getEffectiveStartDate(), endDate, seins);
        }
    }

    private LocalDateTime convert(Date in) {
        LocalDateTime ldt = LocalDateTime.ofInstant(in.toInstant(), ZoneId.systemDefault());
        return ldt;
    }
}
