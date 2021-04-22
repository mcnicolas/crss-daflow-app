package com.pemc.crss.dataflow.app.dto;

import com.pemc.crss.shared.core.dataflow.repository.ExecutionParamRepository;
import com.pemc.crss.shared.core.dataflow.repository.ViewTxnOutputAddtlCompensationRepository;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Created by jadona on 4/30/21.
 */
@Slf4j
public class FinalizedAC {
    private static DateFormat df = new SimpleDateFormat("yyyy/MM/dd"); // TODO: should I use different format? Like the one in the Angular table
    private static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private String billingPeriod;
    private String billingID;
    private String mtn;
    private String pricingCondition;
    private String approvedRate;
    private String acAmount;
    private String dateFinalized;
    private static ViewTxnOutputAddtlCompensationRepository viewTxnOutputAddtlCompensationRepository;
    private static ExecutionParamRepository executionParamRepository;

    public FinalizedAC(String billingPeriod, String billingID, String mtn, String pricingCondition, String approvedRate, String acAmount, String dateFinalized) {
        this.billingPeriod = billingPeriod;
        this.billingID = billingID;
        this.mtn = mtn;
        this.pricingCondition = pricingCondition;
        this.approvedRate = approvedRate;
        this.acAmount = acAmount;
        this.dateFinalized = dateFinalized;
    }

    public static void setViewTxnOutputAddtlCompensationRepository(ViewTxnOutputAddtlCompensationRepository viewTxnOutputAddtlCompensationRepository) {
        FinalizedAC.viewTxnOutputAddtlCompensationRepository = viewTxnOutputAddtlCompensationRepository;
    }

    public static void setExecutionParamRepository(ExecutionParamRepository executionParamRepository) {
        FinalizedAC.executionParamRepository = executionParamRepository;
    }

    public String getBillingPeriod() {
        return billingPeriod;
    }

    public void setBillingPeriod(String billingPeriod) {
        this.billingPeriod = billingPeriod;
    }

    public String getBillingID() {
        return billingID;
    }

    public void setBillingID(String billingID) {
        this.billingID = billingID;
    }

    public String getMtn() {
        return mtn;
    }

    public void setMtn(String mtn) {
        this.mtn = mtn;
    }

    public String getPricingCondition() {
        return pricingCondition;
    }

    public void setPricingCondition(String pricingCondition) {
        this.pricingCondition = pricingCondition;
    }

    public String getApprovedRate() {
        return approvedRate;
    }

    public void setApprovedRate(String approvedRate) {
        this.approvedRate = approvedRate;
    }

    public String getAcAmount() {
        return acAmount;
    }

    public void setAcAmount(String acAmount) {
        this.acAmount = acAmount;
    }

    public String getDateFinalized() {
        return dateFinalized;
    }

    public void setDateFinalized(String dateFinalized) {
        this.dateFinalized = dateFinalized;
    }

    public static Stream<FinalizedAC> getFinalizedACStream(AddtlCompensationExecutionDto addtlCompensationExecutionDto) {
        log.info("getFinalizedACStream({})", addtlCompensationExecutionDto);

        String startDateStr = df.format(addtlCompensationExecutionDto.getDistinctAddtlCompDto().getStartDate());
        String endDateStr = df.format(addtlCompensationExecutionDto.getDistinctAddtlCompDto().getEndDate());
        String billingPeriod = String.format("%s-%s", startDateStr, endDateStr);
        String pricingCondition = addtlCompensationExecutionDto.getDistinctAddtlCompDto().getPricingCondition();
        String acGroupId = addtlCompensationExecutionDto.getDistinctAddtlCompDto().getGroupId();

        return addtlCompensationExecutionDto.getAddtlCompensationExecDetailsDtos().stream()
                .map(addtlCompensationExecDetailsDto -> fromAddtlCompensationExecDetailsDto(addtlCompensationExecDetailsDto, acGroupId, billingPeriod, pricingCondition));
    }

    private static FinalizedAC fromAddtlCompensationExecDetailsDto(AddtlCompensationExecDetailsDto addtlCompensationExecDetailsDto, String acGroupId, String billingPeriod, String pricingCondition) {
        log.info("fromAddtlCompensationExecDetailsDto({}, {}, {}, {})", acGroupId, billingPeriod, pricingCondition, addtlCompensationExecDetailsDto);

        String mtn = addtlCompensationExecDetailsDto.getMtn();
        String acAmount = Optional.ofNullable(viewTxnOutputAddtlCompensationRepository.getAcAmountByAcGroupIdAndMtn(acGroupId, mtn)).map(acAmt -> getValue(acAmt).toString()).orElse(null);
        String approvedRate = getValue(addtlCompensationExecDetailsDto.getApprovedRate()).toString();
        List<Timestamp> dateFinalizedList = executionParamRepository.getDateFinalizedList(acGroupId);
        Optional<Timestamp> dateFinalizedOpt = Optional.empty();
        if (!dateFinalizedList.isEmpty()) {
            dateFinalizedOpt = Optional.of(dateFinalizedList.get(0));
        }
        String dateFinalized = dateFinalizedOpt.map(dateFinalizedTs -> dateFinalizedTs.toLocalDateTime().toLocalDate().format(formatter)).orElse(null);

        return new FinalizedAC(billingPeriod, addtlCompensationExecDetailsDto.getBillingId(), mtn, pricingCondition, approvedRate, acAmount, dateFinalized);
    }

    // to avoid scientific notation for zero values with different scales (e.g. 0E-11)
    private static BigDecimal getValue(BigDecimal b) {
        boolean isZero = b.compareTo(BigDecimal.ZERO) == 0;

        if (isZero) {
            return BigDecimal.ZERO;
        } else {
            return b;
        }
    }
}
