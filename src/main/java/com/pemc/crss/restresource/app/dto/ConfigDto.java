package com.pemc.crss.restresource.app.dto;

public class ConfigDto {
    String configName;
    String mtnName;

    public ConfigDto(String configName, String mtnName) {
        this.configName = configName;
        this.mtnName = mtnName;
    }

    public String getConfigName() {
        return configName;
    }

    public void setConfigName(String configName) {
        this.configName = configName;
    }

    public String getMtnName() {
        return mtnName;
    }

    public void setMtnName(String mtnName) {
        this.mtnName = mtnName;
    }
}
