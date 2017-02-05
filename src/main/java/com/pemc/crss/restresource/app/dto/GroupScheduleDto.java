package com.pemc.crss.restresource.app.dto;

/**
 * Created on 1/16/17.
 */
public class GroupScheduleDto {
    String effDate;
    String mtnGroupName;

    public String getEffDate() {
        return effDate;
    }

    public void setEffDate(String effDate) {
        this.effDate = effDate;
    }

    public String getMtnGroupName() {
        return mtnGroupName;
    }

    public void setMtnGroupName(String mtnGroupName) {
        this.mtnGroupName = mtnGroupName;
    }
}
