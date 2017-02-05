package com.pemc.crss.restresource.app.dto;

import com.pemc.crss.meterprocess.core.main.entity.mtn.MTNModelGroupScheduleTemporary;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created on 1/16/17.
 */
public class GroupDto {
    String groupName;
    Date effDatetime;
    List<ConfigDto> configs = new ArrayList<>();

    public GroupDto() {
    }

    public GroupDto(MTNModelGroupScheduleTemporary temp) {
        this.groupName = temp.getMtnModelGroup().getGroupName();
        this.effDatetime = temp.getEffectiveStartDate();
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public List<ConfigDto> getConfigs() {
        return configs;
    }

    public void setConfigs(List<ConfigDto> configs) {
        this.configs = configs;
    }

    public void addConfig(ConfigDto config) {
        this.configs.add(config);
    }

    public Date getEffDatetime() {
        return effDatetime;
    }

    public void setEffDatetime(Date effDatetime) {
        this.effDatetime = effDatetime;
    }
}