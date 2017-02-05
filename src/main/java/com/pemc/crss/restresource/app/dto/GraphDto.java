package com.pemc.crss.restresource.app.dto;

/**
 * Created on 1/11/17.
 */
public class GraphDto {
    private String[] xAxis;
    private Double[] yAxisMTN;
    private Double[] yAxisRTU;

    public String[] getxAxis() {
        return xAxis;
    }

    public void setxAxis(String[] xAxis) {
        this.xAxis = xAxis;
    }

    public Double[] getyAxisMTN() {
        return yAxisMTN;
    }

    public void setyAxisMTN(Double[] yAxisMTN) {
        this.yAxisMTN = yAxisMTN;
    }

    public Double[] getyAxisRTU() {
        return yAxisRTU;
    }

    public void setyAxisRTU(Double[] yAxisRTU) {
        this.yAxisRTU = yAxisRTU;
    }
}
