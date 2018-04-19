CREATE OR REPLACE VIEW dataflow.vw_mtr_issuance AS
  SELECT
    id,
    trading_date,
    start_date,
    end_date,
    msp,
    last_run_date,
    status,
    issue_date
  FROM meterprocess.txn_mtr_msp_issuance;

alter view dataflow.vw_mtr_issuance owner to crss_dataflow;

grant SELECT on meterprocess.txn_mtr_msp_issuance to crss_dataflow;
