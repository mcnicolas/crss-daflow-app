package com.pemc.crss.meterprocess.task.mqcomputation.support;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;

/**
 * Created on 7/20/17.
 * Should've put this in core-main, but current dataflow contexts require this on the SAME package.
 * This will do... for now.
 */
public class GapDto {
    @JsonProperty("sein")
    String sein;
    @JsonProperty("readingDatetime")
    Date readingDatetime;
    @JsonProperty("meterType")
    String meterType;

    public GapDto() {
    }

    public GapDto(String sein, Date readingDatetime, String meterType) {
        this.sein = sein;
        this.readingDatetime = readingDatetime;
        this.meterType = meterType;
    }

    public String getSein() {
        return sein;
    }

    public void setSein(String sein) {
        this.sein = sein;
    }

    public Date getReadingDatetime() {
        return readingDatetime;
    }

    public void setReadingDatetime(Date readingDatetime) {
        this.readingDatetime = readingDatetime;
    }

    public String getMeterType() {
        return meterType;
    }

    public void setMeterType(String meterType) {
        this.meterType = meterType;
    }
}
