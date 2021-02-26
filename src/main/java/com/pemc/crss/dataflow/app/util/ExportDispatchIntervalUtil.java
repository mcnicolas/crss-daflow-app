package com.pemc.crss.dataflow.app.util;

import com.pemc.crss.dataflow.app.support.PageableRequest;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.cellprocessor.time.FmtLocalDateTime;

import java.time.format.DateTimeFormatter;

/**
 * Created by jadona on 3/4/21.
 */
public class ExportDispatchIntervalUtil {
    private static DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static String getProcessedLogsFilename(PageableRequest page) {
        String groupId = page.getMapParams().get("groupId");
        return String.format("%s_ProcessedIntervals_", groupId);
    }

    public static String getSkipLogsFilename(PageableRequest page) {
        String groupId = page.getMapParams().get("groupId");
        return String.format("%s_SkippedIntervals_", groupId);
    }

    public static String[] getSkippedHeaders() {
        return new String[]{
                "Dispatch Interval",
                "Resource Name",
                "Region",
                "GESQ",
                "BCQ",
                "ASIE",
                "FEDP",
                "Pricing Condition",
                "Approved Rate",
                "Reason"
        };
    }

    public static String[] getProcessedHeaders() {
        return new String[]{
                "Dispatch Interval",
                "Resource Name",
                "Region",
                "GESQ",
                "BCQ",
                "ASIE",
                "FEDP",
                "Pricing Condition",
                "Approved Rate",
                "AC Amount"
        };
    }

    public static String[] getSkippedFields() {
        return new String[]{
                "dispatchInterval",
                "resourceName",
                "region",
                "gesq",
                "bcq",
                "asie",
                "fedp",
                "pricingCondition",
                "approvedRate",
                "reason"
        };
    }

    public static String[] getProcessedFields() {
        return new String[]{
                "dispatchInterval",
                "resourceName",
                "region",
                "gesq",
                "bcq",
                "asie",
                "fedp",
                "pricingCondition",
                "approvedRate",
                "acAmount"
        };
    }

    public static CellProcessor[] getSkippedCellProcessors() {
        return new CellProcessor[]{
                new FmtLocalDateTime(df),
                new org.supercsv.cellprocessor.Optional(),
                new org.supercsv.cellprocessor.Optional(),
                new org.supercsv.cellprocessor.Optional(),
                new org.supercsv.cellprocessor.Optional(),
                new org.supercsv.cellprocessor.Optional(),
                new org.supercsv.cellprocessor.Optional(),
                new org.supercsv.cellprocessor.Optional(),
                new org.supercsv.cellprocessor.Optional(),
                new org.supercsv.cellprocessor.Optional()
        };
    }

    public static CellProcessor[] getProcessedCellProcessors() {
        return new CellProcessor[]{
                new FmtLocalDateTime(df),
                new org.supercsv.cellprocessor.Optional(),
                new org.supercsv.cellprocessor.Optional(),
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
