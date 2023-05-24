DROP VIEW IF EXISTS dataflow.vw_cfg_billing_id;

CREATE VIEW dataflow.vw_cfg_billing_id AS
   SELECT cfg_billing_id.id,
    cfg_billing_id.created_by,
    cfg_billing_id.created_date,
    cfg_billing_id.modified_by,
    cfg_billing_id.modified_date,
    cfg_billing_id.version,
    cfg_billing_id.billing_id,
    cfg_billing_id.direct_participant,
    cfg_billing_id.effective_end_date,
    cfg_billing_id.effective_start_date,
    cfg_billing_id.renewable,
    cfg_billing_id.tp_address,
    cfg_billing_id.tp_short_name,
    cfg_billing_id.trading_participant,
    cfg_billing_id.virtual_id,
    cfg_billing_id.withholding_tax,
    cfg_billing_id.zero_rated,
    cfg_billing_id.is_default_billing_id,
    cfg_billing_id.tp_membership_type,
    cfg_billing_id.tp_facility_type,
    cfg_billing_id.region,
    cfg_billing_id.invoice_id,
    cfg_billing_id.wht,
    cfg_billing_id.ith,
    cfg_billing_id.genx
   FROM settlement.cfg_billing_id;


ALTER table dataflow.vw_cfg_billing_id OWNER TO crss_dataflow;
grant all on all tables in schema dataflow to crss_settlement;
grant all on all tables in schema settlement to crss_dataflow;
