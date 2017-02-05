package com.pemc.crss.restresource.app.dto;

/**
 * Created on 1/12/17.
 */
public class MTNDto {
    private String mtnConfigName;
    private String mtn;
    private String mtnGroupName;

    public String getMtnConfigName() {
        return mtnConfigName;
    }

    public void setMtnConfigName(String mtnConfigName) {
        this.mtnConfigName = mtnConfigName;
    }

    public String getMtn() {
        return mtn;
    }

    public void setMtn(String mtn) {
        this.mtn = mtn;
    }

    public String getMtnGroupName() {
        return mtnGroupName;
    }

    public void setMtnGroupName(String mtnGroupName) {
        this.mtnGroupName = mtnGroupName;
    }
}
