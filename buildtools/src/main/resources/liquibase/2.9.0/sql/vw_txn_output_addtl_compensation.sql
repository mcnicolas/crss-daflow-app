DROP VIEW IF EXISTS dataflow.vw_txn_output_addtl_compensation;

CREATE VIEW dataflow.vw_txn_output_addtl_compensation AS
   SELECT txn_output_addtl_compensation.id,
    txn_output_addtl_compensation.created_by,
    txn_output_addtl_compensation.created_date,
    txn_output_addtl_compensation.modified_by,
    txn_output_addtl_compensation.modified_date,
    txn_output_addtl_compensation.version,
    txn_output_addtl_compensation.ac_group_id,
    txn_output_addtl_compensation.ac_job_id,
    txn_output_addtl_compensation.approved_rate,
    txn_output_addtl_compensation.billing_id,
    txn_output_addtl_compensation.billing_period_end,
    txn_output_addtl_compensation.billing_period_start,
    txn_output_addtl_compensation.group_id,
    txn_output_addtl_compensation.job_id,
    txn_output_addtl_compensation.pricing_condition,
    txn_output_addtl_compensation.run_date,
    txn_output_addtl_compensation.virtual_id,
    txn_output_addtl_compensation.ac_amount,
    txn_output_addtl_compensation.bcq,
    txn_output_addtl_compensation.dispatch_interval,
    txn_output_addtl_compensation.fedp,
    txn_output_addtl_compensation.gesq,
    txn_output_addtl_compensation.mtn,
    txn_output_addtl_compensation.region,
    txn_output_addtl_compensation.region_group,
    txn_output_addtl_compensation.asie,
    txn_output_addtl_compensation.scheduled_generation,
    txn_output_addtl_compensation.deviation
   FROM settlement.txn_output_addtl_compensation;

ALTER table dataflow.vw_txn_output_addtl_compensation OWNER TO crss_dataflow;
grant all on all tables in schema dataflow to crss_settlement;

