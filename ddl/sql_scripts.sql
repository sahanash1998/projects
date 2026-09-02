USE CATALOG abi_${ENV};

CREATE SCHEMA IF NOT EXISTS gmadw_cmn_views MANAGED LOCATION 's3://abi-data-${ENV}';

CREATE EXTERNAL VOLUME IF NOT EXISTS stage.stage_volume
COMMENT 'Data Source Stage Data'
LOCATION 's3://abi-data-${ENV}/stage_data/data_source/';

GRANT SELECT ON TABLE schema_pub.ACTIVITY TO `dhetl-${ENV}-abi-de-leadusers`;

update gmadw_integration_schema.src_map_dtl set where_clause = "WHERE EXPENSE_DATE >= to_date(last_run_date,'YYYY-MM-DD')" where src_id=25 and objct_id =43;

INSERT INTO gmadw_integration_schema.trgt_map_dtl (src_id, objct_id, col_seq, src_col_nm, trgt_col_nm, col_flag, crtn_dt_tm) VALUES ('25','43','1','"Address Line 1" AS address_line_1','address_line_1','Y',current_timestamp());

Insert into gmadw_integration_schema.stage_to_publish_mapping (src_id,objct_id,stage_schema_name,stage_table_name,stage_column_name,pub_schema_name,pub_table_name,pub_column_name,unique_key,col_seq) 
  VALUES ('25','43','gems_stage','tov','current_timestamp()','gems_pub','tov','load_dt_tm','','64');


CREATE OR REPLACE TABLE gmadw_veevacrm_gems_pub.pub_tov (
  address_line_1 STRING,
  address_line_2 STRING,
    total_amount_of_payment DECIMAL(38,10),
    expense_date DATE,
   load_dt_tm TIMESTAMP
);



CREATE OR REPLACE VIEW gmadw_cmn_views.v_top_sl_list (
  report_id,
  record_id,
  top_sl_tag,
  top_sl_therapeutic_area,
  top_sl_sub_ta,
  link_hcp_id,
  first_name,
  last_name,
  sl_name,
  country_code,
  country_description,
  region,
  proprietary_label,
  guideline_taskforce,
  top_sl_emerging_expert_flag,
  top_sl_year,
  current_top_sl,
  salesforce_id,
  top_sl_country,
  load_dt_tm)
WITH SCHEMA COMPENSATION
AS select distinct
  tsl.report_id
  , tsl.record_id
  , tsl.top_sl_tag
  , tsl.top_sl_therapeutic_area
  , tsl.top_sl_sub_ta
  , hcp.link_hcp_id
  , case when hcp.first_name is null
      then tsl.top_sl_first_name
      else hcp.first_name
      end as first_name
  , case when hcp.last_name is null
      then tsl.top_sl_last_name
      else hcp.last_name
      end as last_name
  , case when hcp.first_name is null
      then tsl.top_sl_first_name
      else hcp.first_name
      end 
      || ' ' || 
      case when hcp.last_name is null
      then tsl.top_sl_last_name
      else hcp.last_name
      end as sl_name
  , upper (tsl.country) as country_code
  , cc.description as country_description
  , cc.region
  , proplab.proprietary_label
  , tsl.guideline_taskforce
  , case when tsl.top_sl_tag is not null
      then 'Top SL'
      else case when emel.emerging_expert is not null
          then emel.emerging_expert
          else 'General SL'
        end
      end as top_sl_emerging_expert_flag
  , tsl.source_workflow_year as top_sl_year
  , case when year (current_date()) = left (tsl.top_sl_tag, 4)
      then 'current_year-top_sl'
      else ''
      end as current_top_sl
  , dsm.salesforce_id
  , dsm.country as top_sl_country,
   current_timestamp() as load_dt_tm
from gmadw_cmn_views.v_top_sl_consolidated tsl
left join gmadw_veevalink_pub.hcp hcp
  ON hcp.link_hcp_id = tsl.top_sl_link
left join gmadw_file_loader.qlik_division_country_codes_and_descriptions_2021_01 cc
  ON cc.code = upper (tsl.country)
left join gmadw_cmn_views.v_top_sl_proprietary_label as proplab
  on proplab.report_id = tsl.report_id
  and proplab.record_id = tsl.record_id
left join 
      (select
      tsl2.report_id
      , tsl2.record_id
      , tsl2.top_sl_link
      , 'Emerging Expert' as emerging_expert
      from gmadw_cmn_views.v_top_sl_consolidated tsl2
      join gmadw_cmn_views.v_top_sl_proprietary_label eme
      on eme.report_id = tsl2.report_id
      and eme.record_id = tsl2.record_id
      and eme.proprietary_label = 'Emerging Expert') emel
    on emel.report_id = tsl.report_id
      and emel.record_id = tsl.record_id
left join gmadw_data_science_pub.vva_link_matchingfile dsm
  on dsm.link_id = tsl.top_sl_link
where (left (tsl.top_sl_tag, 4) is null or left (tsl.top_sl_tag, 4) >= (year (current_date())-1))
order by hcp.link_hcp_id, tsl.record_id
  ;





