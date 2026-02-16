#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# - Delete run_dt=* subfolders so Athena doesn't double-count
# - Create Athena/Glue tables via DDL (no crawlers)
# ============================================================

# ---------- CONFIG ----------
export BUCKET="${BUCKET:-aws-hazard-risk-vigamogh-dev}"
export BRONZE_PREFIX="${BRONZE_PREFIX:-hazard/bronze}"
export DB="${DB:-bronze_hazard_raw}"
export WORKGROUP="${WORKGROUP:-primary}"
export OUT="${OUT:-s3://aws-hazard-risk-vigamogh-dev/athena-results/}"

# ---------- Helpers ----------
athena_qid () {
  local SQL="$1"
  aws athena start-query-execution \
    --work-group "$WORKGROUP" \
    --query-execution-context Database="$DB" \
    --query-string "$SQL" \
    --result-configuration OutputLocation="$OUT" \
    --output text \
    --query 'QueryExecutionId'
}

athena_wait () {
  local QID="$1"
  local STATE
  while true; do
    STATE="$(aws athena get-query-execution --query-execution-id "$QID" --output text --query 'QueryExecution.Status.State')"
    case "$STATE" in
      SUCCEEDED|FAILED|CANCELLED) break ;;
      *) sleep 2 ;;
    esac
  done
  echo "$STATE"
  if [[ "$STATE" != "SUCCEEDED" ]]; then
    aws athena get-query-execution --query-execution-id "$QID" --output json \
      | jq -r '.QueryExecution.Status.StateChangeReason'
    return 1
  fi
}

athena_run () {
  local SQL="$1"
  echo "------------------------------------------------------------"
  echo "$SQL"
  local QID
  QID="$(athena_qid "$SQL")"
  echo "QID=$QID"
  athena_wait "$QID" >/dev/null
}

athena_scalar () {
  local SQL="$1"
  local QID
  QID="$(athena_qid "$SQL")"
  athena_wait "$QID" >/dev/null
  aws athena get-query-results --query-execution-id "$QID" --output json \
    | jq -r '.ResultSet.Rows[].Data[0].VarCharValue' | sed 1d
}

s3_rm_run_dt () {
  local prefix="$1"
  local uri="s3://${BUCKET}/${BRONZE_PREFIX}/${prefix}/"
  echo "==== Deleting run_dt=* under: $uri ===="
  # Deletes ONLY keys under run_dt=.../ (keeps the base CSV sitting at the dataset root)
  aws s3 rm "$uri" --recursive --exclude "*" --include "run_dt=*/**"
}

# ============================================================
# 1) Remove run_dt history folders (Strategy A)
# ============================================================
s3_rm_run_dt "fema/disaster_declarations"
s3_rm_run_dt "fema/housing_assistance_owners"
s3_rm_run_dt "fema/housing_assistance_renters"
s3_rm_run_dt "nri/counties"
s3_rm_run_dt "census/acs5_2022_B01001"
s3_rm_run_dt "census/acs5_2022_B15003"
s3_rm_run_dt "census/acs5_2022_B23025"
s3_rm_run_dt "census/acs5_2022_B19013"
s3_rm_run_dt "census/acs5_2022_B25077"

# ============================================================
# 2) Drop (if exists) the clean Bronze tables we will create
#    (NOAA tables remain as-is and are not touched)
# ============================================================
athena_run "DROP TABLE IF EXISTS ${DB}.disaster_declarations"
athena_run "DROP TABLE IF EXISTS ${DB}.housing_assistance_owners"
athena_run "DROP TABLE IF EXISTS ${DB}.housing_assistance_renters"
athena_run "DROP TABLE IF EXISTS ${DB}.nri_counties"
athena_run "DROP TABLE IF EXISTS ${DB}.acs5_2022_b01001"
athena_run "DROP TABLE IF EXISTS ${DB}.acs5_2022_b15003"
athena_run "DROP TABLE IF EXISTS ${DB}.acs5_2022_b23025"
athena_run "DROP TABLE IF EXISTS ${DB}.acs5_2022_b19013"
athena_run "DROP TABLE IF EXISTS ${DB}.acs5_2022_b25077"

# ============================================================
# 3) Create clean Bronze tables (LOCATION = folder, not file)
#    Uses OpenCSVSerde to handle commas/quotes more safely.
# ============================================================

# --- FEMA: disaster_declarations ---
athena_run "
CREATE EXTERNAL TABLE ${DB}.disaster_declarations (
  femadeclarationstring string,
  disasternumber bigint,
  state string,
  declarationtype string,
  declarationdate string,
  fydeclared bigint,
  incidenttype string,
  declarationtitle string,
  ihprogramdeclared bigint,
  iaprogramdeclared bigint,
  paprogramdeclared bigint,
  hmprogramdeclared bigint,
  incidentbegindate string,
  incidentenddate string,
  disastercloseoutdate string,
  tribalrequest bigint,
  fipsstatecode bigint,
  fipscountycode bigint,
  placecode bigint,
  designatedarea string,
  declarationrequestnumber bigint,
  lastiafilingdate string,
  incidentid bigint,
  region bigint,
  designatedincidenttypes string,
  lastrefresh string,
  hash string,
  id string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar'     = '\"',
  'escapeChar'    = '\\\\'
)
LOCATION 's3://${BUCKET}/${BRONZE_PREFIX}/fema/disaster_declarations/'
TBLPROPERTIES ('skip.header.line.count'='1');
"

# --- FEMA: housing_assistance_owners ---
athena_run "
CREATE EXTERNAL TABLE ${DB}.housing_assistance_owners (
  disasternumber bigint,
  state string,
  county string,
  city string,
  zipcode bigint,
  validregistrations bigint,
  averagefemainspecteddamage double,
  totalinspected bigint,
  totaldamage double,
  nofemainspecteddamage bigint,
  femainspecteddamagebetween1and10000 bigint,
  femainspecteddamagebetween10001and20000 bigint,
  femainspecteddamagebetween20001and30000 bigint,
  femainspecteddamagegreaterthan30000 bigint,
  approvedforfemaassistance bigint,
  totalapprovedihpamount double,
  repairreplaceamount double,
  rentalamount double,
  otherneedsamount double,
  approvedbetween1and10000 bigint,
  approvedbetween10001and25000 bigint,
  approvedbetween25001andmax bigint,
  totalmaxgrants bigint,
  id string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar'     = '\"',
  'escapeChar'    = '\\\\'
)
LOCATION 's3://${BUCKET}/${BRONZE_PREFIX}/fema/housing_assistance_owners/'
TBLPROPERTIES ('skip.header.line.count'='1');
"

# --- FEMA: housing_assistance_renters ---
athena_run "
CREATE EXTERNAL TABLE ${DB}.housing_assistance_renters (
  disasternumber bigint,
  state string,
  county string,
  city string,
  zipcode bigint,
  validregistrations bigint,
  totalinspected bigint,
  totalinspectedwithnodamage bigint,
  totalwithmoderatedamage bigint,
  totalwithmajordamage bigint,
  totalwithsubstantialdamage bigint,
  approvedforfemaassistance bigint,
  totalapprovedihpamount double,
  repairreplaceamount double,
  rentalamount double,
  otherneedsamount double,
  approvedbetween1and10000 bigint,
  approvedbetween10001and25000 bigint,
  approvedbetween25001andmax bigint,
  totalmaxgrants bigint,
  id string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar'     = '\"',
  'escapeChar'    = '\\\\'
)
LOCATION 's3://${BUCKET}/${BRONZE_PREFIX}/fema/housing_assistance_renters/'
TBLPROPERTIES ('skip.header.line.count'='1');
"

# --- NRI: counties ---
athena_run "
CREATE EXTERNAL TABLE ${DB}.nri_counties (
  objectid bigint,
  nri_id string,
  state string,
  stateabbrv string,
  statefips bigint,
  county string,
  countytype string,
  countyfips bigint,
  stcofips bigint,
  population bigint,
  buildvalue bigint,
  agrivalue bigint,
  area double,
  risk_value double,
  risk_score double,
  risk_ratng string,
  risk_spctl double,
  eal_score double,
  eal_ratng string,
  eal_spctl double,
  eal_valt double,
  eal_valb double,
  eal_valp double,
  eal_valpe double,
  eal_vala double,
  alr_valb string,
  alr_valp string,
  alr_vala string,
  alr_npctl double,
  alr_vra_npctl double,
  sovi_score double,
  sovi_ratng string,
  sovi_spctl double,
  resl_score double,
  resl_ratng string,
  resl_spctl double,
  resl_value double,
  crf_value double,
  avln_evnts double,
  avln_afreq double,
  avln_exp_area string,
  avln_expb double,
  avln_expp double,
  avln_exppe double,
  avln_expt double,
  avln_hlrb string,
  avln_hlrp double,
  avln_hlrr string,
  avln_ealb double,
  avln_ealp string,
  avln_ealpe double,
  avln_ealt double,
  avln_eals double,
  avln_ealr string,
  avln_alrb string,
  avln_alrp string,
  avln_alr_npctl double,
  avln_riskv double,
  avln_risks double,
  avln_riskr string,
  cfld_evnts string,
  cfld_afreq double,
  cfld_exp_area double,
  cfld_expb double,
  cfld_expp double,
  cfld_exppe double,
  cfld_expt double,
  cfld_hlrb double,
  cfld_hlrp string,
  cfld_hlrr string,
  cfld_ealb double,
  cfld_ealp string,
  cfld_ealpe double,
  cfld_ealt double,
  cfld_eals double,
  cfld_ealr string,
  cfld_alrb string,
  cfld_alrp string,
  cfld_alr_npctl double,
  cfld_riskv double,
  cfld_risks double,
  cfld_riskr string,
  cwav_evnts string,
  cwav_afreq string,
  cwav_exp_area double,
  cwav_expb double,
  cwav_expp double,
  cwav_exppe double,
  cwav_expa double,
  cwav_expt double,
  cwav_hlrb string,
  cwav_hlrp string,
  cwav_hlra string,
  cwav_hlrr string,
  cwav_ealb string,
  cwav_ealp string,
  cwav_ealpe double,
  cwav_eala double,
  cwav_ealt double,
  cwav_eals double,
  cwav_ealr string,
  cwav_alrb string,
  cwav_alrp string,
  cwav_alra string,
  cwav_alr_npctl double,
  cwav_riskv double,
  cwav_risks double,
  cwav_riskr string,
  drgt_evnts double,
  drgt_afreq double,
  drgt_exp_area double,
  drgt_expa double,
  drgt_expt double,
  drgt_hlra string,
  drgt_hlrr string,
  drgt_eala double,
  drgt_ealt double,
  drgt_eals double,
  drgt_ealr string,
  drgt_alra string,
  drgt_alr_npctl double,
  drgt_riskv double,
  drgt_risks double,
  drgt_riskr string,
  erqk_evnts string,
  erqk_afreq double,
  erqk_exp_area double,
  erqk_expb double,
  erqk_expp bigint,
  erqk_exppe bigint,
  erqk_expt double,
  erqk_hlrb double,
  erqk_hlrp string,
  erqk_hlrr string,
  erqk_ealb double,
  erqk_ealp string,
  erqk_ealpe double,
  erqk_ealt double,
  erqk_eals double,
  erqk_ealr string,
  erqk_alrb string,
  erqk_alrp string,
  erqk_alr_npctl double,
  erqk_riskv double,
  erqk_risks double,
  erqk_riskr string,
  hail_evnts double,
  hail_afreq string,
  hail_exp_area double,
  hail_expb double,
  hail_expp double,
  hail_exppe double,
  hail_expa double,
  hail_expt double,
  hail_hlrb string,
  hail_hlrp string,
  hail_hlra string,
  hail_hlrr string,
  hail_ealb double,
  hail_ealp string,
  hail_ealpe double,
  hail_eala double,
  hail_ealt double,
  hail_eals double,
  hail_ealr string,
  hail_alrb string,
  hail_alrp string,
  hail_alra string,
  hail_alr_npctl double,
  hail_riskv double,
  hail_risks double,
  hail_riskr string,
  hwav_evnts double,
  hwav_afreq double,
  hwav_exp_area double,
  hwav_expb double,
  hwav_expp double,
  hwav_exppe double,
  hwav_expa double,
  hwav_expt double,
  hwav_hlrb string,
  hwav_hlrp string,
  hwav_hlra string,
  hwav_hlrr string,
  hwav_ealb double,
  hwav_ealp double,
  hwav_ealpe double,
  hwav_eala double,
  hwav_ealt double,
  hwav_eals double,
  hwav_ealr string,
  hwav_alrb string,
  hwav_alrp string,
  hwav_alra string,
  hwav_alr_npctl double,
  hwav_riskv double,
  hwav_risks double,
  hwav_riskr string,
  hrcn_evnts double,
  hrcn_afreq double,
  hrcn_exp_area double,
  hrcn_expb double,
  hrcn_expp double,
  hrcn_exppe double,
  hrcn_expa double,
  hrcn_expt double,
  hrcn_hlrb string,
  hrcn_hlrp string,
  hrcn_hlra double,
  hrcn_hlrr string,
  hrcn_ealb double,
  hrcn_ealp string,
  hrcn_ealpe double,
  hrcn_eala double,
  hrcn_ealt double,
  hrcn_eals double,
  hrcn_ealr string,
  hrcn_alrb string,
  hrcn_alrp string,
  hrcn_alra string,
  hrcn_alr_npctl double,
  hrcn_riskv double,
  hrcn_risks double,
  hrcn_riskr string,
  istm_evnts double,
  istm_afreq double,
  istm_exp_area double,
  istm_expb double,
  istm_expp double,
  istm_exppe double,
  istm_expt double,
  istm_hlrb string,
  istm_hlrp string,
  istm_hlrr string,
  istm_ealb double,
  istm_ealp string,
  istm_ealpe double,
  istm_ealt double,
  istm_eals double,
  istm_ealr string,
  istm_alrb string,
  istm_alrp string,
  istm_alr_npctl double,
  istm_riskv double,
  istm_risks double,
  istm_riskr string,
  lnds_evnts string,
  lnds_afreq double,
  lnds_exp_area double,
  lnds_expb double,
  lnds_expp double,
  lnds_exppe double,
  lnds_expt double,
  lnds_hlrb string,
  lnds_hlrp string,
  lnds_hlrr string,
  lnds_ealb double,
  lnds_ealp string,
  lnds_ealpe double,
  lnds_ealt double,
  lnds_eals double,
  lnds_ealr string,
  lnds_alrb string,
  lnds_alrp string,
  lnds_alr_npctl double,
  lnds_riskv double,
  lnds_risks double,
  lnds_riskr string,
  ltng_evnts double,
  ltng_afreq double,
  ltng_exp_area double,
  ltng_expb double,
  ltng_expp double,
  ltng_exppe double,
  ltng_expt double,
  ltng_hlrb string,
  ltng_hlrp string,
  ltng_hlrr string,
  ltng_ealb double,
  ltng_ealp string,
  ltng_ealpe double,
  ltng_ealt double,
  ltng_eals double,
  ltng_ealr string,
  ltng_alrb string,
  ltng_alrp string,
  ltng_alr_npctl double,
  ltng_riskv double,
  ltng_risks double,
  ltng_riskr string,
  ifld_evnts bigint,
  ifld_afreq double,
  ifld_exp_area double,
  ifld_expb double,
  ifld_expp double,
  ifld_exppe double,
  ifld_expa double,
  ifld_expt double,
  ifld_hlrb double,
  ifld_hlrp string,
  ifld_hlra string,
  ifld_hlrr string,
  ifld_ealb double,
  ifld_ealp string,
  ifld_ealpe double,
  ifld_eala double,
  ifld_ealt double,
  ifld_eals double,
  ifld_ealr string,
  ifld_alrb string,
  ifld_alrp string,
  ifld_alra string,
  ifld_alr_npctl double,
  ifld_riskv double,
  ifld_risks double,
  ifld_riskr string,
  swnd_evnts double,
  swnd_afreq string,
  swnd_exp_area double,
  swnd_expb double,
  swnd_expp double,
  swnd_exppe double,
  swnd_expa double,
  swnd_expt double,
  swnd_hlrb string,
  swnd_hlrp string,
  swnd_hlra string,
  swnd_hlrr string,
  swnd_ealb double,
  swnd_ealp string,
  swnd_ealpe string,
  swnd_eala double,
  swnd_ealt double,
  swnd_eals double,
  swnd_ealr string,
  swnd_alrb string,
  swnd_alrp string,
  swnd_alra string,
  swnd_alr_npctl double,
  swnd_riskv double,
  swnd_risks double,
  swnd_riskr string,
  trnd_evnts double,
  trnd_afreq string,
  trnd_exp_area double,
  trnd_expb double,
  trnd_expp double,
  trnd_exppe double,
  trnd_expa double,
  trnd_expt double,
  trnd_hlrb string,
  trnd_hlrp string,
  trnd_hlra string,
  trnd_hlrr string,
  trnd_ealb double,
  trnd_ealp string,
  trnd_ealpe double,
  trnd_eala double,
  trnd_ealt double,
  trnd_eals double,
  trnd_ealr string,
  trnd_alrb string,
  trnd_alrp string,
  trnd_alra string,
  trnd_alr_npctl double,
  trnd_riskv double,
  trnd_risks double,
  trnd_riskr string,
  tsun_evnts double,
  tsun_afreq double,
  tsun_exp_area double,
  tsun_expb double,
  tsun_expp double,
  tsun_exppe double,
  tsun_expt double,
  tsun_hlrb string,
  tsun_hlrp string,
  tsun_hlrr string,
  tsun_ealb double,
  tsun_ealp double,
  tsun_ealpe double,
  tsun_ealt double,
  tsun_eals double,
  tsun_ealr string,
  tsun_alrb string,
  tsun_alrp string,
  tsun_alr_npctl double,
  tsun_riskv double,
  tsun_risks double,
  tsun_riskr string,
  vlcn_evnts double,
  vlcn_afreq double,
  vlcn_exp_area double,
  vlcn_expb double,
  vlcn_expp double,
  vlcn_exppe double,
  vlcn_expt double,
  vlcn_hlrb string,
  vlcn_hlrp string,
  vlcn_hlrr string,
  vlcn_ealb string,
  vlcn_ealp string,
  vlcn_ealpe string,
  vlcn_ealt string,
  vlcn_eals double,
  vlcn_ealr string,
  vlcn_alrb string,
  vlcn_alrp string,
  vlcn_alr_npctl double,
  vlcn_riskv string,
  vlcn_risks double,
  vlcn_riskr string,
  wfir_evnts string,
  wfir_afreq string,
  wfir_exp_area double,
  wfir_expb double,
  wfir_expp double,
  wfir_exppe double,
  wfir_expa double,
  wfir_expt double,
  wfir_hlrb double,
  wfir_hlrp double,
  wfir_hlra double,
  wfir_hlrr string,
  wfir_ealb double,
  wfir_ealp string,
  wfir_ealpe double,
  wfir_eala double,
  wfir_ealt double,
  wfir_eals double,
  wfir_ealr string,
  wfir_alrb string,
  wfir_alrp string,
  wfir_alra string,
  wfir_alr_npctl double,
  wfir_riskv double,
  wfir_risks double,
  wfir_riskr string,
  wntw_evnts double,
  wntw_afreq string,
  wntw_exp_area double,
  wntw_expb double,
  wntw_expp double,
  wntw_exppe double,
  wntw_expa double,
  wntw_expt double,
  wntw_hlrb string,
  wntw_hlrp string,
  wntw_hlra string,
  wntw_hlrr string,
  wntw_ealb double,
  wntw_ealp string,
  wntw_ealpe double,
  wntw_eala string,
  wntw_ealt string,
  wntw_eals double,
  wntw_ealr string,
  wntw_alrb string,
  wntw_alrp string,
  wntw_alra string,
  wntw_alr_npctl double,
  wntw_riskv string,
  wntw_risks double,
  wntw_riskr string,
  nri_ver string,
  shape__area double,
  shape__length double
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar'     = '\"',
  'escapeChar'    = '\\\\'
)
LOCATION 's3://${BUCKET}/${BRONZE_PREFIX}/nri/counties/'
TBLPROPERTIES ('skip.header.line.count'='1');
"

# --- CENSUS ACS tables ---

athena_run "
CREATE EXTERNAL TABLE ${DB}.acs5_2022_b01001 (
  name string,
  b01001_001e bigint, b01001_001ea string, b01001_001m bigint, b01001_001ma string,
  b01001_002e bigint, b01001_002ea string, b01001_002m bigint, b01001_002ma string,
  b01001_003e bigint, b01001_003ea string, b01001_003m bigint, b01001_003ma string,
  b01001_004e bigint, b01001_004ea string, b01001_004m bigint, b01001_004ma string,
  b01001_005e bigint, b01001_005ea string, b01001_005m bigint, b01001_005ma string,
  b01001_006e bigint, b01001_006ea string, b01001_006m bigint, b01001_006ma string,
  b01001_007e bigint, b01001_007ea string, b01001_007m bigint, b01001_007ma string,
  b01001_008e bigint, b01001_008ea string, b01001_008m bigint, b01001_008ma string,
  b01001_009e bigint, b01001_009ea string, b01001_009m bigint, b01001_009ma string,
  b01001_010e bigint, b01001_010ea string, b01001_010m bigint, b01001_010ma string,
  b01001_011e bigint, b01001_011ea string, b01001_011m bigint, b01001_011ma string,
  b01001_012e bigint, b01001_012ea string, b01001_012m bigint, b01001_012ma string,
  b01001_013e bigint, b01001_013ea string, b01001_013m bigint, b01001_013ma string,
  b01001_014e bigint, b01001_014ea string, b01001_014m bigint, b01001_014ma string,
  b01001_015e bigint, b01001_015ea string, b01001_015m bigint, b01001_015ma string,
  b01001_016e bigint, b01001_016ea string, b01001_016m bigint, b01001_016ma string,
  b01001_017e bigint, b01001_017ea string, b01001_017m bigint, b01001_017ma string,
  b01001_018e bigint, b01001_018ea string, b01001_018m bigint, b01001_018ma string,
  b01001_019e bigint, b01001_019ea string, b01001_019m bigint, b01001_019ma string,
  b01001_020e bigint, b01001_020ea string, b01001_020m bigint, b01001_020ma string,
  b01001_021e bigint, b01001_021ea string, b01001_021m bigint, b01001_021ma string,
  b01001_022e bigint, b01001_022ea string, b01001_022m bigint, b01001_022ma string,
  b01001_023e bigint, b01001_023ea string, b01001_023m bigint, b01001_023ma string,
  b01001_024e bigint, b01001_024ea string, b01001_024m bigint, b01001_024ma string,
  b01001_025e bigint, b01001_025ea string, b01001_025m bigint, b01001_025ma string,
  b01001_026e bigint, b01001_026ea string, b01001_026m bigint, b01001_026ma string,
  b01001_027e bigint, b01001_027ea string, b01001_027m bigint, b01001_027ma string,
  b01001_028e bigint, b01001_028ea string, b01001_028m bigint, b01001_028ma string,
  b01001_029e bigint, b01001_029ea string, b01001_029m bigint, b01001_029ma string,
  b01001_030e bigint, b01001_030ea string, b01001_030m bigint, b01001_030ma string,
  b01001_031e bigint, b01001_031ea string, b01001_031m bigint, b01001_031ma string,
  b01001_032e bigint, b01001_032ea string, b01001_032m bigint, b01001_032ma string,
  b01001_033e bigint, b01001_033ea string, b01001_033m bigint, b01001_033ma string,
  b01001_034e bigint, b01001_034ea string, b01001_034m bigint, b01001_034ma string,
  b01001_035e bigint, b01001_035ea string, b01001_035m bigint, b01001_035ma string,
  b01001_036e bigint, b01001_036ea string, b01001_036m bigint, b01001_036ma string,
  b01001_037e bigint, b01001_037ea string, b01001_037m bigint, b01001_037ma string,
  b01001_038e bigint, b01001_038ea string, b01001_038m bigint, b01001_038ma string,
  b01001_039e bigint, b01001_039ea string, b01001_039m bigint, b01001_039ma string,
  b01001_040e bigint, b01001_040ea string, b01001_040m bigint, b01001_040ma string,
  b01001_041e bigint, b01001_041ea string, b01001_041m bigint, b01001_041ma string,
  b01001_042e bigint, b01001_042ea string, b01001_042m bigint, b01001_042ma string,
  b01001_043e bigint, b01001_043ea string, b01001_043m bigint, b01001_043ma string,
  b01001_044e bigint, b01001_044ea string, b01001_044m bigint, b01001_044ma string,
  b01001_045e bigint, b01001_045ea string, b01001_045m bigint, b01001_045ma string,
  b01001_046e bigint, b01001_046ea string, b01001_046m bigint, b01001_046ma string,
  b01001_047e bigint, b01001_047ea string, b01001_047m bigint, b01001_047ma string,
  b01001_048e bigint, b01001_048ea string, b01001_048m bigint, b01001_048ma string,
  b01001_049e bigint, b01001_049ea string, b01001_049m bigint, b01001_049ma string,
  geo_id string,
  state bigint,
  county bigint
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar'=',','quoteChar'='\"','escapeChar'='\\\\')
LOCATION 's3://${BUCKET}/${BRONZE_PREFIX}/census/acs5_2022_B01001/'
TBLPROPERTIES ('skip.header.line.count'='1');
"

athena_run "
CREATE EXTERNAL TABLE ${DB}.acs5_2022_b15003 (
  name string,
  b15003_001e bigint, b15003_001ea string, b15003_001m bigint, b15003_001ma string,
  b15003_002e bigint, b15003_002ea string, b15003_002m bigint, b15003_002ma string,
  b15003_003e bigint, b15003_003ea string, b15003_003m bigint, b15003_003ma string,
  b15003_004e bigint, b15003_004ea string, b15003_004m bigint, b15003_004ma string,
  b15003_005e bigint, b15003_005ea string, b15003_005m bigint, b15003_005ma string,
  b15003_006e bigint, b15003_006ea string, b15003_006m bigint, b15003_006ma string,
  b15003_007e bigint, b15003_007ea string, b15003_007m bigint, b15003_007ma string,
  b15003_008e bigint, b15003_008ea string, b15003_008m bigint, b15003_008ma string,
  b15003_009e bigint, b15003_009ea string, b15003_009m bigint, b15003_009ma string,
  b15003_010e bigint, b15003_010ea string, b15003_010m bigint, b15003_010ma string,
  b15003_011e bigint, b15003_011ea string, b15003_011m bigint, b15003_011ma string,
  b15003_012e bigint, b15003_012ea string, b15003_012m bigint, b15003_012ma string,
  b15003_013e bigint, b15003_013ea string, b15003_013m bigint, b15003_013ma string,
  b15003_014e bigint, b15003_014ea string, b15003_014m bigint, b15003_014ma string,
  b15003_015e bigint, b15003_015ea string, b15003_015m bigint, b15003_015ma string,
  b15003_016e bigint, b15003_016ea string, b15003_016m bigint, b15003_016ma string,
  b15003_017e bigint, b15003_017ea string, b15003_017m bigint, b15003_017ma string,
  b15003_018e bigint, b15003_018ea string, b15003_018m bigint, b15003_018ma string,
  b15003_019e bigint, b15003_019ea string, b15003_019m bigint, b15003_019ma string,
  b15003_020e bigint, b15003_020ea string, b15003_020m bigint, b15003_020ma string,
  b15003_021e bigint, b15003_021ea string, b15003_021m bigint, b15003_021ma string,
  b15003_022e bigint, b15003_022ea string, b15003_022m bigint, b15003_022ma string,
  b15003_023e bigint, b15003_023ea string, b15003_023m bigint, b15003_023ma string,
  b15003_024e bigint, b15003_024ea string, b15003_024m bigint, b15003_024ma string,
  b15003_025e bigint, b15003_025ea string, b15003_025m bigint, b15003_025ma string,
  geo_id string,
  state bigint,
  county bigint
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar'=',','quoteChar'='\"','escapeChar'='\\\\')
LOCATION 's3://${BUCKET}/${BRONZE_PREFIX}/census/acs5_2022_B15003/'
TBLPROPERTIES ('skip.header.line.count'='1');
"

athena_run "
CREATE EXTERNAL TABLE ${DB}.acs5_2022_b23025 (
  name string,
  b23025_001e bigint, b23025_001ea string, b23025_001m bigint, b23025_001ma string,
  b23025_002e bigint, b23025_002ea string, b23025_002m bigint, b23025_002ma string,
  b23025_003e bigint, b23025_003ea string, b23025_003m bigint, b23025_003ma string,
  b23025_004e bigint, b23025_004ea string, b23025_004m bigint, b23025_004ma string,
  b23025_005e bigint, b23025_005ea string, b23025_005m bigint, b23025_005ma string,
  b23025_006e bigint, b23025_006ea string, b23025_006m bigint, b23025_006ma string,
  b23025_007e bigint, b23025_007ea string, b23025_007m bigint, b23025_007ma string,
  geo_id string,
  state bigint,
  county bigint
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar'=',','quoteChar'='\"','escapeChar'='\\\\')
LOCATION 's3://${BUCKET}/${BRONZE_PREFIX}/census/acs5_2022_B23025/'
TBLPROPERTIES ('skip.header.line.count'='1');
"

athena_run "
CREATE EXTERNAL TABLE ${DB}.acs5_2022_b19013 (
  name string,
  b19013_001e bigint,
  state bigint,
  county bigint
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar'=',','quoteChar'='\"','escapeChar'='\\\\')
LOCATION 's3://${BUCKET}/${BRONZE_PREFIX}/census/acs5_2022_B19013/'
TBLPROPERTIES ('skip.header.line.count'='1');
"

athena_run "
CREATE EXTERNAL TABLE ${DB}.acs5_2022_b25077 (
  name string,
  b25077_001e bigint,
  state bigint,
  county bigint
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar'=',','quoteChar'='\"','escapeChar'='\\\\')
LOCATION 's3://${BUCKET}/${BRONZE_PREFIX}/census/acs5_2022_B25077/'
TBLPROPERTIES ('skip.header.line.count'='1');
"

# ============================================================
# 4) Smoke tests
# ============================================================
echo "==================== SMOKE TEST COUNTS ===================="
echo "disaster_declarations:        $(athena_scalar "SELECT COUNT(*) FROM ${DB}.disaster_declarations")"
echo "housing_assistance_owners:    $(athena_scalar "SELECT COUNT(*) FROM ${DB}.housing_assistance_owners")"
echo "housing_assistance_renters:   $(athena_scalar "SELECT COUNT(*) FROM ${DB}.housing_assistance_renters")"
echo "nri_counties:                 $(athena_scalar "SELECT COUNT(*) FROM ${DB}.nri_counties")"
echo "acs5_2022_b01001:             $(athena_scalar "SELECT COUNT(*) FROM ${DB}.acs5_2022_b01001")"
echo "acs5_2022_b15003:             $(athena_scalar "SELECT COUNT(*) FROM ${DB}.acs5_2022_b15003")"
echo "acs5_2022_b23025:             $(athena_scalar "SELECT COUNT(*) FROM ${DB}.acs5_2022_b23025")"
echo "acs5_2022_b19013:             $(athena_scalar "SELECT COUNT(*) FROM ${DB}.acs5_2022_b19013")"
echo "acs5_2022_b25077:             $(athena_scalar "SELECT COUNT(*) FROM ${DB}.acs5_2022_b25077")"

echo "Done."
