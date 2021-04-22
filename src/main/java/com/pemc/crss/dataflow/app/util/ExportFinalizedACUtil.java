package com.pemc.crss.dataflow.app.util;

import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.cellprocessor.time.FmtLocalDate;

import java.time.format.DateTimeFormatter;

/**
 * Created by jadona on 4/23/21.
 */
public class ExportFinalizedACUtil {
    private static DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    public static String getFilename() {
        return "ACSummary_"; // datetime will be appended later on
    }

    public static String[] getHeaders() {
        return new String[]{
                "Billing Period",
                "Billing ID",
                "MTN",
                "Pricing Condition",
                "Approved Rate",
                "AC Amount",
                "Date Finalized"
        };
    }

    public static String[] getFields() {
        return new String[]{
                "billingPeriod",
                "billingID",
                "mtn",
                "pricingCondition",
                "approvedRate",
                "acAmount",
                "dateFinalized" // yyyy-mm-dd
        };
    }

    public static CellProcessor[] getCellProcessors() {
        return new CellProcessor[]{
                new org.supercsv.cellprocessor.Optional(),
                new org.supercsv.cellprocessor.Optional(),
                new org.supercsv.cellprocessor.Optional(),
                new org.supercsv.cellprocessor.Optional(),
                new org.supercsv.cellprocessor.Optional(),
                new org.supercsv.cellprocessor.Optional(),
                new org.supercsv.cellprocessor.Optional()
        };
    }
}
