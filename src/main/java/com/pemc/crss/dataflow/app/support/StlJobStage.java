package com.pemc.crss.dataflow.app.support;


public enum StlJobStage {

    GENERATE_IWS("GENERATE-INPUT-WORKSPACE"),
    CALCULATE_STL("SETTLEMENT-CALCULATION"),
    CALCULATE_LR("LR-CALCULATION"),
    CALCULATE_GMR("CALCULATE-GMR"),
    FINALIZE("TAGGING"),
    FINALIZE_LR("TAGGING-LINE-RENTAL"),
    GENERATE_FILE("GENERATE-FILE");

    private String label;

    StlJobStage(String label) {
        this.label = label;
    }

    public String getLabel() {
        return label;
    }
}
