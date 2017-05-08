package com.pemc.crss.dataflow.app.dto;

public class DistinctAddtlCompDto {
    private String startDate;
    private String endDate;
    private String pricingCondition;

    public DistinctAddtlCompDto(String startDate, String endDate, String pricingCondition) {
        this.startDate = startDate;
        this.endDate = endDate;
        this.pricingCondition = pricingCondition;
    }

    public String getStartDate() {
        return startDate;
    }

    public void setStartDate(String startDate) {
        this.startDate = startDate;
    }

    public String getEndDate() {
        return endDate;
    }

    public void setEndDate(String endDate) {
        this.endDate = endDate;
    }

    public String getPricingCondition() {
        return pricingCondition;
    }

    public void setPricingCondition(String pricingCondition) {
        this.pricingCondition = pricingCondition;
    }

    public static DistinctAddtlCompDto create(final String startDate, final String endDate, final String pricingCondition) {
        return new DistinctAddtlCompDto(startDate, endDate, pricingCondition);
    }
}
