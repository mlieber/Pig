-- pig -f mr_cornerstone_source_specific.pig -p SCHEMANAME=mnr -p PROCESS_DATE=20140601 -p YESTERDAY_DATE=20140525 -p TARGET_TABLE1=claim_header -p TARGET_TABLE2=claim_detail
-- -D mapred.job.queue.name=ingest -useHCatalog

 

-- MR_RAW_BUILD / _JOIN_BUILD

-- CLAIMS INCREMENTAL SCRIPT - 
-- Gets raw data and creates Multi-src data, comprised of Claims Header and Claims detail tables.
--

--  REGISTER /homedir/mlieber/R5/build/jar/myudfs.jar;
 
-- LOAD ALL REQUIRED TABLES
/* 

med_claim1 = LOAD 'mnr.A_MED_CLM' using org.apache.hcatalog.pig.HCatLoader();
 med_claim2 = FILTER med_claim1 BY proc_date == '2014-06-22';
-- med_claim2 = FILTER med_claim1 BY proc_date == '2014-03-02';


med_claim_srvc1 =  LOAD 'mnr.A_MED_CLM_SRVC' using org.apache.hcatalog.pig.HCatLoader();
med_claim_srvc2 = FILTER med_claim_srvc1 BY proc_date == '2014-03-02';

med_clm_srvc_prcdr_mod1 = LOAD 'mnr.A_MED_CLM_SRVC_PRCDR_MOD' using org.apache.hcatalog.pig.HCatLoader();
med_clm_srvc_prcdr_mod = FILTER med_clm_srvc_prcdr_mod1 BY proc_date == '2014-03-02';

med_clm_srvc_fin1 = LOAD 'mnr.A_MED_CLM_SRVC_FIN' using org.apache.hcatalog.pig.HCatLoader();
med_clm_srvc_fin2 = FILTER med_clm_srvc_fin1 BY proc_date == '2014-03-02';

med_claim_diag1 = LOAD 'mnr.A_MED_CLM_DIAG' using org.apache.hcatalog.pig.HCatLoader();
med_claim_diag = FILTER med_claim_diag1 BY proc_date == '2014-03-02';

med_clm_srvc_prov1 = LOAD 'mnr.A_MED_CLM_SRVC_PROV' using org.apache.hcatalog.pig.HCatLoader();
med_clm_srvc_prov2 = FILTER med_clm_srvc_prov1 BY proc_date == '2014-03-02';

med_claim_yesterday1 = LOAD 'mnr.claim_header' using org.apache.hcatalog.pig.HCatLoader();
med_claim_yesterday = FILTER med_claim_yesterday1 BY proc_date == 'claims_yesterday';
*/
med_claim1 = LOAD '$SCHEMANAME.A_MED_CLM' using org.apache.hcatalog.pig.HCatLoader();
med_claim2 = FILTER med_claim1 BY proc_date == '${PROCESS_DATE}';
med_claim_srvc1 =  LOAD '$SCHEMANAME.A_MED_CLM_SRVC' using org.apache.hcatalog.pig.HCatLoader();
med_claim_srvc2 = FILTER med_claim_srvc1 BY proc_date == '${PROCESS_DATE}';

med_clm_srvc_prcdr_mod1 = LOAD '$SCHEMANAME.A_MED_CLM_SRVC_PRCDR_MOD' using org.apache.hcatalog.pig.HCatLoader();
med_clm_srvc_prcdr_mod = FILTER med_clm_srvc_prcdr_mod1 BY proc_date == '${PROCESS_DATE}';

med_clm_srvc_fin1 = LOAD '$SCHEMANAME.A_MED_CLM_SRVC_FIN' using org.apache.hcatalog.pig.HCatLoader();
med_clm_srvc_fin2 = FILTER med_clm_srvc_fin1 BY proc_date == '${PROCESS_DATE}';

med_claim_diag1 = LOAD '$SCHEMANAME.A_MED_CLM_DIAG' using org.apache.hcatalog.pig.HCatLoader();
med_claim_diag = FILTER med_claim_diag1 BY proc_date == '${PROCESS_DATE}';

med_clm_srvc_prov1 = LOAD '$SCHEMANAME.A_MED_CLM_SRVC_PROV' using org.apache.hcatalog.pig.HCatLoader();
med_clm_srvc_prov2 = FILTER med_clm_srvc_prov1 BY proc_date == '${PROCESS_DATE}';
 

med_claim_yesterday1 = LOAD '$SCHEMANAME.claim_header' using org.apache.hcatalog.pig.HCatLoader();
med_claim_yesterday = FILTER med_claim_yesterday1 BY proc_date == '${YESTERDAY_DATE}';


-- Format String - Remove leading/trailing spaces
med_claim = FOREACH med_claim2 GENERATE TRIM(a_med_clm_sk) AS a_med_clm_sk,TRIM(med_clm_sk) AS med_clm_sk,TRIM(file_key) AS file_key,
TRIM(cli_id) AS cli_id,TRIM(cli_sk) AS cli_sk,TRIM(clm_id) AS clm_id,TRIM(clm_mod_id) AS clm_mod_id,
TRIM(clm_orig_ref_num) AS clm_orig_ref_num,TRIM(clm_freq_cd) AS clm_freq_cd,clm_stmt_from_dt,
clm_stmt_to_dt,TRIM(orig_clm_id) AS orig_clm_id, TRIM(cs_data_src_sys_cd) AS cs_data_src_sys_cd, src_updt_dttm,
TRIM(clm_mbr_alt_id_val_txt) AS clm_mbr_alt_id_val_txt, clm_admis_dttm, clm_dschrg_dt, diag_rel_grp_cd, clm_facl_cd,  ptnt_dschrg_sts_cd, sub_cli_sk;

med_claim_srvc = FOREACH med_claim_srvc2 GENERATE TRIM(a_med_clm_srvc_sk) AS a_med_clm_srvc_sk,TRIM(a_med_clm_sk) AS a_med_clm_sk,
TRIM(med_clm_srvc_sk) AS med_clm_srvc_sk,TRIM(cli_id) AS cli_id,TRIM(cli_sk) AS cli_sk,
TRIM(clm_id) AS clm_id,TRIM(clm_mod_id) AS clm_mod_id,TRIM(srvc_line_num) AS srvc_line_num,TRIM(nubc_cd) AS nubc_cd,
TRIM((chararray)((int)srvc_unit_cnt)) AS srvc_unit_cnt,clm_srvc_from_dt,clm_srvc_to_dt, src_updt_dttm, TRIM(prcd_cd) AS prcd_cd,
plc_of_srvc_cd;

med_clm_srvc_prov = FOREACH med_clm_srvc_prov2 GENERATE TRIM(clm_prov_enty_npi) AS clm_prov_enty_npi, TRIM(clm_id) AS clm_id,TRIM(a_med_clm_sk) AS a_med_clm_sk, TRIM(a_med_clm_srvc_sk) AS a_med_clm_srvc_sk, TRIM(clm_or_srvc_ind) AS clm_or_srvc_ind, TRIM(prov_enty_type) AS prov_enty_type, TRIM(src_updt_dttm) AS src_updt_dttm;

med_claim_diag = FOREACH med_claim_diag GENERATE TRIM(a_med_clm_sk) AS a_med_clm_sk, TRIM(diag_cd) AS diag_cd, TRIM(diag_qual_typ_cd) AS diag_qual_typ_cd;

med_clm_srvc_prcdr_mod = FOREACH med_clm_srvc_prcdr_mod GENERATE TRIM(prcdr_cd_mod_cd) AS prcdr_cd_mod_cd, TRIM(a_med_clm_srvc_sk) AS a_med_clm_srvc_sk;

med_clm_srvc_fin = FOREACH med_clm_srvc_fin2 GENERATE TRIM(a_med_clm_sk) AS a_med_clm_sk, TRIM(a_med_clm_srvc_sk) AS a_med_clm_srvc_sk,
      TRIM(tot_chrg_amt) AS tot_chrg_amt, TRIM((chararray)((float)clm_trn_amt)) AS clm_trn_amnt, TRIM(srvc_line_item_chrg_amt) AS srvc_line_item_chrg_amt;

-- COMPLEX MAPPING RULES

/* Derive Source System
 "If A_MED_CLM.SUB_CLI_SK=6, then map 'CST-COS-COS' to the target
If A_MED_CLM.SUB_CLI_SK=33, then map 'CST-NIC-NIC' to the target
If A_MED_CLM.SUB_CLI_SK=36, then map 'CST-NIC-PES' to the target
*/
-- Concatenate claim number		
/*
Call {Format String - Remove leading/trailing spaces}
If A_MED_CLM.SUB_CLI_SK = 6, then Concatenate as CO-CLM_ID-CLM_MOD_ID
If A_MED_CLM.SUB_CLI_SK IN (33,36) then Concatenate as NI-CLM_ID-CLM_MOD_ID
*/
sourcesystem_concat = FOREACH med_claim GENERATE 
                (CASE sub_cli_sk 
				WHEN '6' THEN 'CST-COS-COS'
                WHEN '33' THEN 'CST-NIC-NIC' 
                WHEN '36' THEN 'CST-NIC-PES'
                ELSE null 
				END)				AS Source_system1,
				(CASE sub_cli_sk 
				WHEN '6' THEN CONCAT('CO-', CONCAT(cli_id,clm_mod_id))
                WHEN '33' THEN CONCAT('NI-',CONCAT(clm_id,clm_mod_id)) 
                WHEN '36' THEN CONCAT('NI-', CONCAT(clm_id,clm_mod_id)) 
                ELSE null 
				END)				AS Claim_number,
				*;
	
-- Generate unique claim header: Generate a claim header for each unique instance of the [Concatenate Claim_Number] output
concatenate_unique = DISTINCT sourcesystem_concat;

derive_unique_clm_hdr_grp = GROUP concatenate_unique BY (clm_id, clm_mod_id);

-- Generate Unique Latest Claim Header record: Call [Generate Unique Claim Header]
-- If multiple records present, then pick the record with the highest 'A_MED_CLM.SRC_UPDT_DTTM'
derive_unique_clm_hdr_dedup = FOREACH derive_unique_clm_hdr_grp {
                         ordered_data = ORDER concatenate_unique by src_updt_dttm DESC;
                         limit_data = LIMIT ordered_data 1;
                         GENERATE FLATTEN(limit_data);
};


-- DELETION PROCESS FILTERING

--   Sampson process looks for claims with  A_MED_CLM.CLM_FREQ_CD=8 for VOID claims
SPLIT derive_unique_clm_hdr_dedup INTO med_claim_false_claims IF clm_freq_cd=='8', med_claim_true_claims IF clm_freq_cd!='8';
med_claim_replacement = FILTER derive_unique_clm_hdr_dedup BY clm_freq_cd == '7';

		
-- HERE START JOINS


-- Derive claim detail: Join A_MED_CLM and A_MED_CLM_SVC - OUTER?
med_claim_w_med_claim_srvc_join = JOIN derive_unique_clm_hdr_dedup by (a_med_clm_sk), 
                                   med_claim_srvc by (a_med_clm_sk);


-- Derive unique claim detail: Call {Derive Claim Detail} + Generate unique claim detail for each unique instance of A_MED_CLM_SK, A_MED_CLM_SRVC_SK
derive_unique_clm_detail = DISTINCT med_claim_w_med_claim_srvc_join;


-- Generate Unique Latest Claim Detail Record
-- If multiple records present of  {Derive unique Claim Detail}, then pick the record with the highest 'A_MED_CLM_SRVC.SRC_UPDT_DTTM' 

uniq_ltst_clm_detail_grp = GROUP derive_unique_clm_detail BY
     (med_claim_srvc::a_med_clm_sk, 
             med_claim_srvc::a_med_clm_srvc_sk);
uniq_ltst_clm_detail = FOREACH uniq_ltst_clm_detail_grp {
                         ordered_data = ORDER derive_unique_clm_detail by 
						 med_claim_srvc::src_updt_dttm DESC;
                         limit_data = LIMIT ordered_data 1;
                         GENERATE FLATTEN(limit_data);
};

-- Derive Claim Type: If any of the records of {Generate Unique Latest Claim Detail Record} have A_MED_CLM_SRVC.NUBC_CD populated then, map 'I', else map,'P'
-- Calculate/pass CLM_STMT_TO_DT as it's needed for Derive Statement_toDate

derive_claim_type_value_target = FOREACH uniq_ltst_clm_detail GENERATE 
  limit_data::derive_unique_clm_hdr_dedup::limit_data::Source_system1,
   limit_data::derive_unique_clm_hdr_dedup::limit_data::Claim_number,
    limit_data::derive_unique_clm_hdr_dedup::limit_data::a_med_clm_sk,
	 limit_data::derive_unique_clm_hdr_dedup::limit_data::clm_mbr_alt_id_val_txt,
	 limit_data::derive_unique_clm_hdr_dedup::limit_data::diag_rel_grp_cd,
	 limit_data::derive_unique_clm_hdr_dedup::limit_data::ptnt_dschrg_sts_cd,
	 limit_data::derive_unique_clm_hdr_dedup::limit_data::clm_facl_cd,
	 limit_data::derive_unique_clm_hdr_dedup::limit_data::clm_freq_cd,
    limit_data::med_claim_srvc::a_med_clm_srvc_sk,  
	limit_data::med_claim_srvc::srvc_line_num,
	limit_data::med_claim_srvc::clm_srvc_from_dt,
	limit_data::med_claim_srvc::clm_srvc_to_dt,
	limit_data::med_claim_srvc::prcd_cd,
	limit_data::med_claim_srvc::plc_of_srvc_cd,
	limit_data::med_claim_srvc::srvc_unit_cnt,
	FLATTEN((limit_data::med_claim_srvc::nubc_cd is null ?  'P' : 'I' )) AS Derive_Clm_Type,
		limit_data::derive_unique_clm_hdr_dedup::limit_data::clm_stmt_to_dt AS Clm_stm_to_dt,
	
		limit_data::derive_unique_clm_hdr_dedup::limit_data::clm_stmt_from_dt AS Clm_stm_from_dt,
		limit_data::med_claim_srvc::nubc_cd,
		limit_data::derive_unique_clm_hdr_dedup::limit_data::clm_id AS clm_id,
		limit_data::derive_unique_clm_hdr_dedup::limit_data::clm_admis_dttm AS clm_admis_dttm;
derive_claim_type_value_target = DISTINCT derive_claim_type_value_target;
		
-- Derive Statement to Date: If the output of the Derive Claim Type call is, 'I', then map A_MED_CLM.CLM_STMT_TO_DT
-- else, if  output of the above call is, 'P', then call {Generate Unique Latest Claim Detail Record}
-- pick the maximum of A_MED_CLM_SRVC.CLM_SRVC_TO_DT

-- calculate max first then pass it, where condition is evaluated; also calculate min of from date, needed later

max_srvc_todt_grp = GROUP derive_claim_type_value_target BY    (    limit_data::derive_unique_clm_hdr_dedup::limit_data::a_med_clm_sk, 
	 limit_data::med_claim_srvc::a_med_clm_srvc_sk);
	 

max_srvc_todt = FOREACH max_srvc_todt_grp 
{
  ordered_data = ORDER derive_claim_type_value_target BY limit_data::med_claim_srvc::clm_srvc_to_dt desc;
                         limit_data = LIMIT ordered_data 1;
                         GENERATE FLATTEN(limit_data),  
						 FLATTEN(MIN(derive_claim_type_value_target.(
                             limit_data::med_claim_srvc::clm_srvc_from_dt))) as Min_Srvc_from_dt;							 	
};
						  
max_srvc_todt_gp = GROUP max_srvc_todt BY ( limit_data::limit_data::derive_unique_clm_hdr_dedup::limit_data::a_med_clm_sk,
limit_data::limit_data::med_claim_srvc::a_med_clm_srvc_sk);

derive_Statement_toDate1 = FOREACH  max_srvc_todt_gp GENERATE 
                 FLATTEN(max_srvc_todt.( limit_data::limit_data::derive_unique_clm_hdr_dedup::limit_data::a_med_clm_sk)) AS a_med_clm_sk ,
				 FLATTEN(max_srvc_todt.( limit_data::limit_data::derive_unique_clm_hdr_dedup::limit_data::Source_system1)) AS Source_system1 ,
				 FLATTEN(max_srvc_todt.(limit_data::limit_data::derive_unique_clm_hdr_dedup::limit_data::clm_mbr_alt_id_val_txt)) AS clm_mbr_alt_id_val_txt,
				 FLATTEN(max_srvc_todt.(limit_data::limit_data::med_claim_srvc::prcd_cd)) AS prcd_cd,
				  
				  FLATTEN(max_srvc_todt.(limit_data::limit_data::med_claim_srvc::clm_srvc_from_dt)) AS clm_srvc_from_dt,
                 FLATTEN(max_srvc_todt.(limit_data::limit_data::med_claim_srvc::srvc_line_num))			AS	  srvc_line_num,
				 
				FLATTEN(max_srvc_todt.(limit_data::limit_data::derive_unique_clm_hdr_dedup::limit_data::diag_rel_grp_cd)) AS diag_rel_grp_cd,
				FLATTEN(max_srvc_todt.(limit_data::limit_data::derive_unique_clm_hdr_dedup::limit_data::ptnt_dschrg_sts_cd)) AS ptnt_dschrg_sts_cd,
				 FLATTEN(max_srvc_todt.(limit_data::limit_data::derive_unique_clm_hdr_dedup::limit_data::clm_facl_cd)) AS clm_facl_cd,
				 FLATTEN(max_srvc_todt.(limit_data::limit_data::derive_unique_clm_hdr_dedup::limit_data::clm_freq_cd)) AS clm_freq_cd,
				 FLATTEN(max_srvc_todt.( limit_data::limit_data::derive_unique_clm_hdr_dedup::limit_data::Claim_number)) AS Claim_number ,
                 FLATTEN(max_srvc_todt.(limit_data::limit_data::med_claim_srvc::a_med_clm_srvc_sk)) AS a_med_clm_srvc_sk, 
				 FLATTEN(max_srvc_todt.(Derive_Clm_Type)) as Derive_Clm_Type,  FLATTEN(max_srvc_todt.(Clm_stm_to_dt)) as Clm_stm_to_dt, 
				 FLATTEN(max_srvc_todt.(limit_data::limit_data::med_claim_srvc::nubc_cd)) as nubc_cd,
				
				 FLATTEN(max_srvc_todt.(limit_data::limit_data::med_claim_srvc::plc_of_srvc_cd)) as plc_of_srvc_cd,
				 FLATTEN(max_srvc_todt.(limit_data::limit_data::med_claim_srvc::srvc_unit_cnt)) as srvc_unit_cnt,
				 FLATTEN(max_srvc_todt.(limit_data::limit_data::med_claim_srvc::clm_srvc_to_dt)) AS Statement_toDate,
				 FLATTEN(max_srvc_todt.(Min_Srvc_from_dt)) AS Min_Srvc_from_dt,
				 FLATTEN(max_srvc_todt.(Clm_stm_from_dt)) AS Clm_stm_from_dt,
				 FLATTEN(max_srvc_todt.(limit_data::clm_admis_dttm)) AS clm_admis_dttm
				 
				 ;
				 
derive_Statement_toDate12 = FOREACH  derive_Statement_toDate1 GENERATE 
                 a_med_clm_sk ,
                 a_med_clm_srvc_sk, 
				 Source_system1,
				 Claim_number,
				 clm_facl_cd, ptnt_dschrg_sts_cd, clm_mbr_alt_id_val_txt, diag_rel_grp_cd,
				 srvc_unit_cnt, plc_of_srvc_cd, nubc_cd, srvc_line_num, prcd_cd,
				 clm_srvc_from_dt, Statement_toDate AS clm_srvc_to_dt,
				
				 (Derive_Clm_Type == 'I' ? Clm_stm_to_dt :  Statement_toDate) AS Statement_toDate,
				 Derive_Clm_Type, Min_Srvc_from_dt, Clm_stm_from_dt,
                 (Derive_Clm_Type == 'I' ? Clm_stm_from_dt : Min_Srvc_from_dt) AS Statement_from_date,
                 (Derive_Clm_Type == 'I' ? CONCAT(clm_facl_cd, clm_freq_cd) : null) AS Bill_type,
				 
				 clm_admis_dttm;				 
derive_Statement_toDate = DISTINCT derive_Statement_toDate12;

-- Derive Statement from Date: if  output of the Derive Claim type call is, 'I', then map A_MED_CLM.CLM_STMT_FROM_DT
-- Else, if  output of the above call is, 'P', then call {Generate Unique Latest Claim Detail Record}
-- pick the minimum of A_MED_CLM_SRVC.CLM_SRVC_FROM_DT
-- See above, computed in toDate 


med_clm_srvc_prov_1 = FILTER med_clm_srvc_prov BY a_med_clm_srvc_sk == '-1';
rendering_jn = JOIN med_clm_srvc_prov_1 BY (a_med_clm_sk),
                     derive_Statement_toDate BY (a_med_clm_sk);
					          					 

rendering1 = FOREACH rendering_jn GENERATE 
                med_clm_srvc_prov_1::a_med_clm_sk, 
				med_clm_srvc_prov_1::a_med_clm_srvc_sk,
				med_clm_srvc_prov_1::clm_or_srvc_ind,
                derive_Statement_toDate::clm_admis_dttm, 
                derive_Statement_toDate::Claim_number, 
                derive_Statement_toDate::Source_system1, 
				derive_Statement_toDate::Bill_type,
				srvc_unit_cnt, plc_of_srvc_cd, nubc_cd, srvc_line_num, prcd_cd,
				clm_srvc_from_dt, clm_srvc_to_dt,
				derive_Statement_toDate::clm_mbr_alt_id_val_txt, 
				derive_Statement_toDate::diag_rel_grp_cd,
                derive_Statement_toDate::ptnt_dschrg_sts_cd,
				derive_Statement_toDate::clm_facl_cd,
				derive_Statement_toDate::a_med_clm_srvc_sk,
				med_clm_srvc_prov_1::src_updt_dttm,
				derive_Statement_toDate::Derive_Clm_Type,
				derive_Statement_toDate::Statement_toDate,
				derive_Statement_toDate::Statement_from_date,
 				(CASE derive_Statement_toDate::Derive_Clm_Type 
				WHEN 'I' THEN 
				  ((med_clm_srvc_prov_1::clm_or_srvc_ind =='CLM' AND med_clm_srvc_prov_1::prov_enty_type=='82') ? med_clm_srvc_prov_1::clm_prov_enty_npi :
                     ((med_clm_srvc_prov_1::clm_or_srvc_ind =='CLM' AND med_clm_srvc_prov_1::prov_enty_type=='71') ? med_clm_srvc_prov_1::clm_prov_enty_npi : 
					     ((med_clm_srvc_prov_1::clm_or_srvc_ind =='CLM' AND med_clm_srvc_prov_1::prov_enty_type=='85') ? med_clm_srvc_prov_1::clm_prov_enty_npi : ''
					 
					     )
					   )
					 )				  
                WHEN 'P' THEN 
                ((med_clm_srvc_prov_1::clm_or_srvc_ind =='CLM' AND med_clm_srvc_prov_1::prov_enty_type=='82') ? med_clm_srvc_prov_1::clm_prov_enty_npi :
                     ((med_clm_srvc_prov_1::clm_or_srvc_ind =='CLM' AND med_clm_srvc_prov_1::prov_enty_type=='85') ? med_clm_srvc_prov_1::clm_prov_enty_npi : ''

					   )
					 )
					 ELSE null 
				END)				AS Rendering_Provider_NPI;	
				
max_rendering1_grp = GROUP rendering1 BY med_clm_srvc_prov_1::a_med_clm_sk
	;
max_rendering = FOREACH max_rendering1_grp 
{
  ordered_data = ORDER rendering1 BY med_clm_srvc_prov_1::src_updt_dttm desc;
  limit_data = LIMIT ordered_data 1;
  GENERATE FLATTEN(limit_data);
							 	
};						
				
-- Derive Modifier X:
-- Call {Create Service_Line}
-- Join A_MED_CLM_SRVC and A_MED_CLM_SRVC_PRCDR_MOD on A_MED_CLM_SRVC_SK
-- Map A_MED_CLM_SRVC_PRCDR_MOD.PRCDR_CD_MOD_CD to the target
-- If multple records present, then pick the record with the latest A_MED_CLM_SRVC_PRCDR_MOD.SRC_UPDT_DTTM


		
/* Derive Charge Amount: Call {Derive Unique latest Claim service Fin}
Call {Generate Unique Latest Claim Header Record}: unq_claim_hdr
Join A_MED_CLM and A_MED_CLM_SRVC_FIN on A_MED_CLM.A_MED_CLM_SK =A_MED_CLM_SRVC_FIN.A_MED_CLM_SK and A_MED_CLM_SRVC_FIN.A_MED_CLM_SRVC_SK= ‘-1’ 

Map A_MED_CLM_SRVC_FIN.TOT_CHRG_AMT to the target
*/

med_clm_srvc_fin1 = FILTER med_clm_srvc_fin BY a_med_clm_srvc_sk == '-1';
/*
med_claim_w_med_claim_srv_fin_join = JOIN prcdr_rendering_jn by (prcdr_todt::a_med_clm_sk)   , 
                                          med_clm_srvc_fin1 by (a_med_clm_sk);	
*/

med_claim_w_med_claim_srv_fin_join = JOIN max_rendering by (limit_data::med_clm_srvc_prov_1::a_med_clm_sk)   , 
                                          med_clm_srvc_fin1 by (a_med_clm_sk);									  


-- Derive ICD_Diag_Admit: Call {Generate Unique Latest Claim Header Record}, and 
-- If A_MED_CLM_DIAG.DIAG_QUAL_TYP_CD in 'ABJ,BJ', then map A_MED_CLM_DIAG.DIAG_CD to the target

-- If multiple records present, then pick the record with the highest 'A_MED_CLM.SRC_UPDT_DTTM'
-- med_claim_diag
/*
med_claim_diag_grp = GROUP med_claim_diag by (a_med_clm_sk);
med_claim_diag_dedup = FOREACH med_claim_diag_grp {
                         ordered_data = ORDER med_claim_diag by src_updt_dttm DESC;
                         limit_data = LIMIT ordered_data 1;
                         GENERATE FLATTEN(limit_data);
};
*/

-- derive_unique_clm_hdr_dedup
unq_claim_hdr_w_med_claim_diag1 = JOIN med_claim_w_med_claim_srv_fin_join BY
                (limit_data::med_clm_srvc_prov_1::a_med_clm_sk) 
                                   , med_claim_diag by (a_med_clm_sk);
unq_claim_hdr_w_med_claim_diag = DISTINCT unq_claim_hdr_w_med_claim_diag1;

-- REMOVE?
med_claim_diag_grp = GROUP unq_claim_hdr_w_med_claim_diag by ( med_claim_w_med_claim_srv_fin_join::max_rendering::limit_data::med_clm_srvc_prov_1::a_med_clm_sk);
med_claim_diag_dedup = FOREACH med_claim_diag_grp {
                         ordered_data = ORDER unq_claim_hdr_w_med_claim_diag by src_updt_dttm DESC;
                         limit_data = LIMIT ordered_data 1;
                         GENERATE FLATTEN(limit_data);
};


-- Derive ICD_Diag_x
-- If A_MED_CLM_DIAG.DIAG_QUAL_TYP_CD in 'ABf,Bf', then map A_MED_CLM_DIAG.DIAG_CD to the target
-- AND
-- Derive E code x
-- If A_MED_CLM_DIAG.DIAG_QUAL_TYP_CD in 'ABN,BN', then map A_MED_CLM_DIAG.DIAG_CD to the target

-- If multiple records present where A_MED_CLM_DIAG.DIAG_QUAL_TYP_CD in 'ABN,BN', then pick one record randomly and map A_MED_CLM_DIAG.DIAG_CD to the target
-- If no records present with A_MED_CLM_DIAG.DIAG_QUAL_TYP_CD in 'ABN,BN', then map '' to the target
-- Combined with diag1 above

claim_header11 = FOREACH med_claim_diag_dedup GENERATE
limit_data::med_claim_w_med_claim_srv_fin_join::max_rendering::limit_data::derive_Statement_toDate::Source_system1 as Source_system,
	   'MR' AS cc_entity, '' AS Health_plan, '' AS BenefitPlan,
	   limit_data::med_claim_w_med_claim_srv_fin_join::max_rendering::limit_data::derive_Statement_toDate::Claim_number as Claim_number,
	   limit_data::med_claim_w_med_claim_srv_fin_join::max_rendering::limit_data::derive_Statement_toDate::clm_mbr_alt_id_val_txt AS Entity_member_id, 
	   limit_data::med_claim_w_med_claim_srv_fin_join::max_rendering::limit_data::derive_Statement_toDate::clm_mbr_alt_id_val_txt AS HP_member_id, 
	   '' AS Claim_status, limit_data::med_claim_w_med_claim_srv_fin_join::max_rendering::limit_data::derive_Statement_toDate::Derive_Clm_Type as Derive_Clm_Type,
	   limit_data::med_claim_w_med_claim_srv_fin_join::max_rendering::limit_data::derive_Statement_toDate::clm_admis_dttm as Admit_date,
	   limit_data::med_claim_w_med_claim_srv_fin_join::max_rendering::limit_data::derive_Statement_toDate::ptnt_dschrg_sts_cd as Discharge_date,
	   limit_data::med_claim_w_med_claim_srv_fin_join::max_rendering::limit_data::derive_Statement_toDate::Statement_toDate AS Statement_toDate,
   limit_data::med_claim_w_med_claim_srv_fin_join::max_rendering::limit_data::derive_Statement_toDate::Statement_from_date AS Statement_from_date,	
		 Bill_type AS Bill_type, '' AS Inpatient_flag, 
		limit_data::med_claim_w_med_claim_srv_fin_join::max_rendering::limit_data::derive_Statement_toDate::diag_rel_grp_cd AS DRG_Code, 
		 '' AS PPS_Code, 
				 (limit_data::med_claim_diag::diag_qual_typ_cd =='BJ' OR 
				    limit_data::med_claim_diag::diag_qual_typ_cd == 'ABJ' ? 
				    limit_data::med_claim_diag::diag_cd : null) AS ICD_Diag_Admit,
                 (med_claim_diag::diag_qual_typ_cd == 'ABK' OR 
				 med_claim_diag::diag_qual_typ_cd =='BK'
				? med_claim_diag::diag_cd : '') AS ICD_Diag_1, (limit_data::med_claim_diag::diag_qual_typ_cd == 'ABF' OR 
				 limit_data::med_claim_diag::diag_qual_typ_cd =='BF' ? 
				limit_data::med_claim_diag::diag_cd : '')
				AS ICD_Diag_2, 
				(limit_data::med_claim_diag::diag_qual_typ_cd == 'ABF' OR 
				 limit_data::med_claim_diag::diag_qual_typ_cd =='BF' ? 
				limit_data::med_claim_diag::diag_cd : '') AS ICD_Diag_3, 
				(limit_data::med_claim_diag::diag_qual_typ_cd == 'ABF' OR 
				 limit_data::med_claim_diag::diag_qual_typ_cd =='BF' ? 
				limit_data::med_claim_diag::diag_cd : '') AS ICD_Diag_4, (limit_data::med_claim_diag::diag_qual_typ_cd == 'ABF' OR 
				 limit_data::med_claim_diag::diag_qual_typ_cd =='BF' ? 
				limit_data::med_claim_diag::diag_cd : '') AS ICD_Diag_5,
		(limit_data::med_claim_diag::diag_qual_typ_cd == 'ABF' OR 
				 limit_data::med_claim_diag::diag_qual_typ_cd =='BF' ? 
				limit_data::med_claim_diag::diag_cd : '') AS ICD_Diag_6, (limit_data::med_claim_diag::diag_qual_typ_cd == 'ABF' OR 
				 limit_data::med_claim_diag::diag_qual_typ_cd =='BF' ? 
				limit_data::med_claim_diag::diag_cd : '') AS ICD_Diag_7, (limit_data::med_claim_diag::diag_qual_typ_cd == 'ABF' OR 
				 limit_data::med_claim_diag::diag_qual_typ_cd =='BF' ? 
				limit_data::med_claim_diag::diag_cd : '') AS ICD_Diag_8, (limit_data::med_claim_diag::diag_qual_typ_cd == 'ABF' OR 
				 limit_data::med_claim_diag::diag_qual_typ_cd =='BF' ? 
				limit_data::med_claim_diag::diag_cd : '') AS ICD_Diag_9, (limit_data::med_claim_diag::diag_qual_typ_cd == 'ABF' OR 
				 limit_data::med_claim_diag::diag_qual_typ_cd =='BF' ? 
				limit_data::med_claim_diag::diag_cd : '') AS ICD_Diag_10, (limit_data::med_claim_diag::diag_qual_typ_cd == 'ABF' OR 
				 limit_data::med_claim_diag::diag_qual_typ_cd =='BF' ? 
				limit_data::med_claim_diag::diag_cd : '') AS ICD_Diag_11,
		 (limit_data::med_claim_diag::diag_qual_typ_cd == 'ABF' OR 
				 limit_data::med_claim_diag::diag_qual_typ_cd =='BF' ? 
				limit_data::med_claim_diag::diag_cd : '') AS ICD_Diag_12, (limit_data::med_claim_diag::diag_qual_typ_cd == 'ABF' OR 
				 limit_data::med_claim_diag::diag_qual_typ_cd =='BF' ? 
				limit_data::med_claim_diag::diag_cd : '') AS ICD_Diag_13, (limit_data::med_claim_diag::diag_qual_typ_cd == 'ABK' OR 
				 limit_data::med_claim_diag::diag_qual_typ_cd =='BK'
				? limit_data::med_claim_diag::diag_cd : '') AS ICD_Diag_14, (limit_data::med_claim_diag::diag_qual_typ_cd == 'ABF' OR 
				 limit_data::med_claim_diag::diag_qual_typ_cd =='BF' ? 
				limit_data::med_claim_diag::diag_cd : '') AS ICD_Diag_15, (limit_data::med_claim_diag::diag_qual_typ_cd == 'ABF' OR 
				 limit_data::med_claim_diag::diag_qual_typ_cd =='BF' ? 
				limit_data::med_claim_diag::diag_cd : '') AS ICD_Diag_16, 
		(limit_data::med_claim_diag::diag_qual_typ_cd == 'ABF' OR 
				 limit_data::med_claim_diag::diag_qual_typ_cd =='BF' ? 
				limit_data::med_claim_diag::diag_cd : '') AS ICD_Diag_17,(limit_data::med_claim_diag::diag_qual_typ_cd == 'ABF' OR 
				 limit_data::med_claim_diag::diag_qual_typ_cd =='BF' ? 
				limit_data::med_claim_diag::diag_cd : '') AS ICD_Diag_18,(limit_data::med_claim_diag::diag_qual_typ_cd == 'ABF' OR 
				 limit_data::med_claim_diag::diag_qual_typ_cd =='BF' ? 
				limit_data::med_claim_diag::diag_cd : '') AS ICD_Diag_19, (limit_data::med_claim_diag::diag_qual_typ_cd == 'ABF' OR 
				 limit_data::med_claim_diag::diag_qual_typ_cd =='BF' ? 
				limit_data::med_claim_diag::diag_cd : '') AS ICD_Diag_20, (limit_data::med_claim_diag::diag_qual_typ_cd == 'ABF' OR 
				 limit_data::med_claim_diag::diag_qual_typ_cd =='BF' ? 
				limit_data::med_claim_diag::diag_cd : '') AS ICD_Diag_21, (limit_data::med_claim_diag::diag_qual_typ_cd == 'ABF' OR 
				 limit_data::med_claim_diag::diag_qual_typ_cd =='BF' ? 
				limit_data::med_claim_diag::diag_cd : '') AS ICD_Diag_22, (limit_data::med_claim_diag::diag_qual_typ_cd == 'ABF' OR 
				 limit_data::med_claim_diag::diag_qual_typ_cd =='BF' ? 
				limit_data::med_claim_diag::diag_cd : '') AS ICD_Diag_23, (limit_data::med_claim_diag::diag_qual_typ_cd == 'ABF' OR 
				 limit_data::med_claim_diag::diag_qual_typ_cd =='BF' ? 
				limit_data::med_claim_diag::diag_cd : '') AS ICD_Diag_24,(limit_data::med_claim_diag::diag_qual_typ_cd == 'ABF' OR 
				 limit_data::med_claim_diag::diag_qual_typ_cd =='BF' ? 
				limit_data::med_claim_diag::diag_cd : '') AS ICD_Diag_25, (limit_data::med_claim_diag::diag_qual_typ_cd == 'ABF' OR 
				 limit_data::med_claim_diag::diag_qual_typ_cd =='BF' ? 
				limit_data::med_claim_diag::diag_cd : '') AS ICD_Diag_26, (limit_data::med_claim_diag::diag_qual_typ_cd == 'ABF' OR 
				 limit_data::med_claim_diag::diag_qual_typ_cd =='BF' ? 
				limit_data::med_claim_diag::diag_cd : '') AS ICD_Diag_27,
		 (limit_data::med_claim_diag::diag_qual_typ_cd == 'ABF' OR 
				 limit_data::med_claim_diag::diag_qual_typ_cd =='BF' ? 
				limit_data::med_claim_diag::diag_cd : '') AS ICD_Diag_28, (limit_data::med_claim_diag::diag_qual_typ_cd == 'ABF' OR 
				 limit_data::med_claim_diag::diag_qual_typ_cd =='BF' ? 
				limit_data::med_claim_diag::diag_cd : '') AS ICD_Diag_29, (limit_data::med_claim_diag::diag_qual_typ_cd == 'ABF' OR 
				 limit_data::med_claim_diag::diag_qual_typ_cd =='BF' ? 
				limit_data::med_claim_diag::diag_cd : '') AS ICD_Diag_30, (limit_data::med_claim_diag::diag_qual_typ_cd == 'ABF' OR 
				 limit_data::med_claim_diag::diag_qual_typ_cd =='BF' ? 
				limit_data::med_claim_diag::diag_cd : '') AS ICD_Diag_31, (limit_data::med_claim_diag::diag_qual_typ_cd == 'ABF' OR 
				 limit_data::med_claim_diag::diag_qual_typ_cd =='BF' ? 
				limit_data::med_claim_diag::diag_cd : '') AS ICD_Diag_32, (limit_data::med_claim_diag::diag_qual_typ_cd == 'ABF' OR 
				 limit_data::med_claim_diag::diag_qual_typ_cd =='BF' ? 
				limit_data::med_claim_diag::diag_cd : '') AS ICD_Diag_33, (limit_data::med_claim_diag::diag_qual_typ_cd == 'ABF' OR 
				 limit_data::med_claim_diag::diag_qual_typ_cd =='BF' ? 
				limit_data::med_claim_diag::diag_cd : '') AS ICD_Diag_34,
	limit_data::med_claim_w_med_claim_srv_fin_join::max_rendering::limit_data::derive_Statement_toDate::clm_facl_cd as Facility_type,
	limit_data::med_claim_w_med_claim_srv_fin_join::med_clm_srvc_fin1::tot_chrg_amt AS Charge_amount,
	limit_data::med_claim_w_med_claim_srv_fin_join::max_rendering::limit_data::Rendering_Provider_NPI AS Rending_provider_NPI,
	limit_data::med_claim_w_med_claim_srv_fin_join::max_rendering::limit_data::Rendering_Provider_NPI AS Referring_provider_NPI, '' AS Referral_number, '' AS Auth_number, '' AS Type_of_adm, '' AS Admit_src, ptnt_dschrg_sts_cd AS Discharge_status, '' AS Capped_ind, '' AS ICD_Proc1, '' AS ICD_Proc_dte1,
		 '' AS ICD_Proc2, '' AS ICD_Proc_dte2, '' AS ICD_Proc3, '' AS ICD_Proc_dte3, '' AS ICD_Proc4, '' AS ICD_Proc_dte4, '' AS ICD_Proc5,
		 '' AS ICD_Proc_dte5, '' AS ICD_Proc6, '' AS ICD_Proc_dte6,
	  
				(limit_data::med_claim_diag::diag_qual_typ_cd == 'ABN' OR 
				limit_data::med_claim_diag::diag_qual_typ_cd =='BN' ? 
				limit_data::med_claim_diag::diag_cd : '') AS Ecod1, 
				(limit_data::med_claim_diag::diag_qual_typ_cd == 'ABN' OR 
				limit_data::med_claim_diag::diag_qual_typ_cd =='BN' ? 
				limit_data::med_claim_diag::diag_cd : '') AS Ecod2,
				(limit_data::med_claim_diag::diag_qual_typ_cd == 'ABN' OR 
				limit_data::med_claim_diag::diag_qual_typ_cd =='BN' ? 
				limit_data::med_claim_diag::diag_cd : '') AS Ecod3,
				
				 '' AS Soft_del_flg, '' AS DW_inst_dt,
		 '' AS DW_insrt_usr_id, '' AS DW_Updt_dt, '' AS DW_upd_usr_id;

-- Done w/ claim header
-- FOR claim details:

/* test

med_claim_w_med_clm_srvc_prcdr_mod2 = JOIN uniq_ltst_clm_detail BY limit_data::med_claim_srvc::a_med_clm_srvc_sk, 
			               med_clm_srvc_prcdr_mod  BY a_med_clm_srvc_sk   USING 'skewed' ;	   
*/

med_claim_w_med_clm_srvc_prcdr_mod = JOIN med_claim_diag_dedup BY limit_data::med_claim_w_med_claim_srv_fin_join::max_rendering::limit_data::derive_Statement_toDate::a_med_clm_srvc_sk, 
			               med_clm_srvc_prcdr_mod  BY a_med_clm_srvc_sk   USING 'skewed' ;	   
										  
max_med_clm_prcdr_mod_grp = GROUP med_claim_w_med_clm_srvc_prcdr_mod BY (limit_data::med_claim_w_med_claim_srv_fin_join::max_rendering::limit_data::med_clm_srvc_prov::a_med_clm_srvc_sk);
max_prcdr_todt = FOREACH max_med_clm_prcdr_mod_grp {
                         ordered_data = ORDER med_claim_w_med_clm_srvc_prcdr_mod by med_claim_diag_dedup::limit_data::med_claim_w_med_claim_srv_fin_join::max_rendering::limit_data::med_clm_srvc_prov::src_updt_dttm desc;
                         limit_data = LIMIT ordered_data 1;
                         GENERATE FLATTEN(limit_data);
};



claim_detail22 = FOREACH max_prcdr_todt GENERATE 
         limit_data::med_claim_diag_dedup::limit_data::med_claim_w_med_claim_srv_fin_join::max_rendering::limit_data::derive_Statement_toDate::Source_system1 as Source_system , 
         'MR' AS cc_entity, 
         limit_data::med_claim_diag_dedup::limit_data::med_claim_w_med_claim_srv_fin_join::max_rendering::limit_data::derive_Statement_toDate::Claim_number as Claim_number,
         limit_data::med_claim_diag_dedup::limit_data::med_claim_w_med_claim_srv_fin_join::max_rendering::limit_data::derive_Statement_toDate::srvc_line_num AS Service_line, 
		 limit_data::med_claim_diag_dedup::limit_data::med_claim_w_med_claim_srv_fin_join::max_rendering::limit_data::derive_Statement_toDate::nubc_cd AS Revenue_code, 
		 limit_data::med_claim_diag_dedup::limit_data::med_claim_w_med_claim_srv_fin_join::max_rendering::limit_data::derive_Statement_toDate::plc_of_srvc_cd AS Facility_type_code, 
		 limit_data::med_claim_diag_dedup::limit_data::med_claim_w_med_claim_srv_fin_join::max_rendering::limit_data::derive_Statement_toDate::prcd_cd AS Procedure_code, 
		 limit_data::med_clm_srvc_prcdr_mod::prcdr_cd_mod_cd AS Modifier_1,
		 limit_data::med_clm_srvc_prcdr_mod::prcdr_cd_mod_cd AS Modifier_2,
		 limit_data::med_clm_srvc_prcdr_mod::prcdr_cd_mod_cd AS Modifier_3,
		 limit_data::med_clm_srvc_prcdr_mod::prcdr_cd_mod_cd AS Modifier_4,
		 '' AS EmergencyFlag, '' AS NDC_code, '' AS Hospital_related_fg, '' AS Outsidelabs_fg, 
		 med_clm_srvc_fin1::clm_trn_amnt AS Paid_amt, med_clm_srvc_fin1::srvc_line_item_chrg_amt AS Billed_Amount, '' AS Value_amt,
		 limit_data::med_claim_diag_dedup::limit_data::med_claim_w_med_claim_srv_fin_join::max_rendering::limit_data::derive_Statement_toDate::srvc_unit_cnt AS Unit_Count,
		 
		 limit_data::med_claim_diag_dedup::limit_data::med_claim_w_med_claim_srv_fin_join::max_rendering::limit_data::derive_Statement_toDate::clm_srvc_from_dt AS Service_From_Date, 
		limit_data::med_claim_diag_dedup::limit_data::med_claim_w_med_claim_srv_fin_join::max_rendering::limit_data::derive_Statement_toDate::clm_srvc_to_dt AS Service_To_Date, '' AS Soft_del_flg, '' AS Insert_dt, '' AS Insert_usr_id, '' AS Update_usr_id, '' AS Update_dt;
claim_detail2 = DISTINCT claim_detail22;


		  
-- ----------------										
-- DELETION process

--   Sampson process looks for claims with  A_MED_CLM.CLM_FREQ_CD=8
-- deleteflag = FILTER med_claim_w_med_clm_srvc_prcdr_mod BY A_MED_CLM.CLM_FREQ_CD==8;
-- For the claims with CLM_FREQ_CD=8 in the new file, Sampson process gets the original claim number
--  from A_MED_CLM.CLM_ORIG_REF_NUM  and modifier id from A_MED_CLM.CLM_MOD_ID in the new file
-- Sampson process concates the claim number and modifier id from step 3 as  CO_CLM_ID_CLM_MOD_ID for COSMOS record and NI_CLM_ID_CLM_MOD_ID for NICE record
-- > alter table claim_detail drop if exists partition (proc_date = '20140524');

deleteClaims = FOREACH med_claim_false_claims GENERATE 
                
				(CASE sub_cli_sk
                 WHEN '6' THEN CONCAT('CO-',
                    CONCAT(clm_orig_ref_num,CONCAT('-',clm_mod_id)))
				 WHEN '33' THEN CONCAT('NI-',
                    CONCAT(clm_orig_ref_num,CONCAT('-',clm_mod_id)))
				 WHEN '36' THEN CONCAT('NI-', 
                    CONCAT(clm_orig_ref_num,CONCAT('-',clm_mod_id)))
                 ELSE null
                 END )
				 AS Claim_number_to_void, 
				 clm_orig_ref_num, a_med_clm_sk;
--- Replacement rule: For the claims with CLM_FREQ_CD=7 in the new file, Sampson process gets the original claim number from A_MED_CLM.CLM_ORIG_REF_NUM  and modifier id from 
-- A_MED_CLM.CLM_MOD_ID in the new file.

replaceClaims = FOREACH med_claim_replacement GENERATE 
                
				(CASE sub_cli_sk
                 WHEN '6' THEN CONCAT('CO-', 
                    CONCAT(clm_orig_ref_num,CONCAT('-',clm_mod_id)))
				 WHEN '33' THEN CONCAT('NI-', 
                    CONCAT(clm_orig_ref_num,CONCAT('-',clm_mod_id)))
				 WHEN '36' THEN CONCAT('NI-', 
                    CONCAT(clm_orig_ref_num,CONCAT('-',clm_mod_id)))
                 ELSE null
                 END ) AS Claim_number_to_void, clm_orig_ref_num, a_med_clm_sk;				
removeAllClaims = UNION deleteClaims, replaceClaims;				
deleteClaims2 = DISTINCT removeAllClaims;

-- Sampson process validates if the claim number from step 4 is already present in the Sampson data store and deletes it
-- outer join to find all of the valid ids, in which case we get a null; retain those rows and keep the row's claim_number: they are valid from yesterday
-- and Union them with today's data 

construct_claims_to_process = FOREACH med_claim_yesterday GENERATE claim_number;
process_deletion_table = JOIN deleteClaims2 BY Claim_number_to_void RIGHT OUTER,
                 construct_claims_to_process BY claim_number;

filtered_claim_header = FOREACH process_deletion_table GENERATE 
   (deleteClaims2::Claim_number_to_void is null ? construct_claims_to_process::claim_number : null) AS claim_number;                              
   
-- Now join to get all of the fields , in order to Union
full_filtered_claim_header = JOIN filtered_claim_header BY (claim_number),
                                 med_claim_yesterday BY (claim_number);		
full_filtered_claim_header1 = FOREACH full_filtered_claim_header GENERATE
   med_claim_yesterday::source_system..med_claim_yesterday::dw_update_user_id;
						

						
/*
filtered_claim_header = FOREACH to_remove GENERATE
   (construct_claims_to_delete::claim_number != deleteClaims::Claim_number_to_void ? construct_claims_to_delete::claim_number : null);
*/			

-- full_claim_header = UNION ONSCHEMA full_filtered_claim_header, claim_header11;


------------------------ ############## --------------------------------------
-- Multi Source Including SSIS Logic For Both Claim Header And Claim Detail
------------------------ ############## --------------------------------------
-- CLAIM HEADER 

claim_header= FOREACH claim_header11 GENERATE
Source_system AS source_system,
cc_entity AS cc_entity,
'' as health_plan,
'' as benefit_plan,
Claim_number AS claim_number,
Entity_member_id as entity_member_id,
HP_member_id as hp_member_id,
Claim_status as claim_status,
Derive_Clm_Type as claim_type,
((SIZE(Admit_date)==8?CONCAT(CONCAT(CONCAT(SUBSTRING(Admit_date,0,4),'-'),CONCAT(SUBSTRING(Admit_date,4,6),'-')),SUBSTRING(Admit_date,6,8)):Admit_date)) as admit_date,
'' as discharge_date,
(SIZE(Statement_toDate)==8?CONCAT(CONCAT(CONCAT(SUBSTRING(Statement_toDate,0,4),'-'),CONCAT(SUBSTRING(Statement_toDate,4,6),'-')),SUBSTRING(Statement_toDate,6,8)):Statement_toDate) as statement_to_date,
(SIZE(Statement_from_date)==8?CONCAT(CONCAT(CONCAT(SUBSTRING(Statement_from_date,0,4),'-'),CONCAT(SUBSTRING(Statement_from_date,4,6),'-')),SUBSTRING(Statement_from_date,6,8)):Statement_from_date) as statement_from_date,
Bill_type as bill_type,
(Inpatient_flag=='Y'?'1':(Inpatient_flag=='N'?'0':'')) as inpatient_flag,
DRG_Code as drg_code,
'' as pps_code,
ICD_Diag_Admit as icd_diag_admit,
ICD_Diag_1 as icd_diag_1,
ICD_Diag_2 as icd_diag_2,
ICD_Diag_3 as icd_diag_3,
ICD_Diag_4 as icd_diag_4,
ICD_Diag_5 as icd_diag_5,
ICD_Diag_6 as icd_diag_6,
ICD_Diag_7 as icd_diag_7,
ICD_Diag_8 as icd_diag_8,
ICD_Diag_9 as icd_diag_9,
ICD_Diag_10 as icd_diag_10,
ICD_Diag_11 as icd_diag_11,
ICD_Diag_12 as icd_diag_12,
ICD_Diag_13 as icd_diag_13,
ICD_Diag_14 as icd_diag_14,
ICD_Diag_15 as icd_diag_15,
ICD_Diag_16 as icd_diag_16,
ICD_Diag_17 as icd_diag_17,
ICD_Diag_18 as icd_diag_18,
ICD_Diag_19 as icd_diag_19,
ICD_Diag_20 as icd_diag_20,
ICD_Diag_21 as icd_diag_21,
ICD_Diag_22 as icd_diag_22,
ICD_Diag_23 as icd_diag_23,
ICD_Diag_24 as icd_diag_24,
ICD_Diag_25 as icd_diag_25,
'' as icd_diag_26,
'' as icd_diag_27,
'' as icd_diag_28,
'' as icd_diag_29,
'' as icd_diag_30,
'' as icd_diag_31,
'' as icd_diag_32,
'' as icd_diag_33,
'' as icd_diag_34,
Facility_type as facility_type,
Charge_amount as charge_amount,
Rending_provider_NPI as rendering_provider_npi,
Referring_provider_NPI as referring_provider_npi,
Referral_number as referral_number,
Auth_number as authorization_number,
Type_of_adm as type_of_admit,
Admit_src as admit_source,
Discharge_status as discharge_status,
Capped_ind as capped_indicator,
ICD_Proc1 as icd_proc_1,
'' as icd_proc_date_1,
'' as icd_proc_2,
'' as icd_proc_date_2,
'' as icd_proc_3,
'' as icd_proc_date_3,
'' as icd_proc_4,
'' as icd_proc_date_4,
'' as icd_proc_5,
'' as icd_proc_date_5,
'' as icd_proc_6,
'' as icd_proc_date_6,
Ecod1 as e_code_1,
Ecod2 as e_code_2,
Ecod3 as e_code_3,
Soft_del_flg as soft_delete_flag,
DW_inst_dt as dw_insert_date,
DW_insrt_usr_id as dw_insert_user_id,
DW_Updt_dt as dw_update_date,
DW_upd_usr_id as dw_update_user_id;


------------------------ ############## ----------------------------------
-- CLAIM Detail

claim_detail= FOREACH claim_detail2 GENERATE
Source_system as source_system,
'MR' as cc_entity,
Claim_number as claim_number,
Service_line as service_line,
Revenue_code as revenue_code,
Facility_type_code as facility_type_code,
Procedure_code as procedure_code,
Modifier_1 as modifier_1,
Modifier_2 as modifier_2,
Modifier_3 as modifier_3,
Modifier_4 as modifier_4,
'' as emergency_flag,
'' as ndc_code,
'' as hospital_related_flag,
'' as outside_labs_flag, 
Paid_amt as paid_amount,
Billed_Amount as billed_amount,
'' as value_amount,
Unit_Count as unit_count,
(SIZE(Service_From_Date)==8?CONCAT(CONCAT(CONCAT(SUBSTRING(Service_From_Date,0,4),'-'),CONCAT(SUBSTRING(Service_From_Date,4,6),'-')),SUBSTRING(Service_From_Date,6,8)):Service_From_Date) as service_from_date,
(SIZE(Service_To_Date)==8?CONCAT(CONCAT(CONCAT(SUBSTRING(Service_To_Date,0,4),'-'),CONCAT(SUBSTRING(Service_To_Date,4,6),'-')),SUBSTRING(Service_To_Date,6,8)):Service_To_Date) as service_to_date;

/*
(SIZE(cch_erly_srvc_dt)==8?CONCAT(CONCAT(CONCAT(SUBSTRING(cch_erly_srvc_dt,0,4),'-'),CONCAT(SUBSTRING(cch_erly_srvc_dt,4,6),'-')),SUBSTRING(cch_erly_srvc_dt,6,8)):cch_erly_srvc_dt) as statement_from_date,
CONCAT(CONCAT(CONCAT(CONCAT(cch_clm_aud_nbr,cch_sub_aud_nbr),cch_site_cd),cch_cos_clm_head_sys_id),ccss_dtl_ln_nbr) as claim_detail_id; // discuss with Jas, find unique clm_detail_id
*/
-- alter table claim_header drop partition(proc_date='2014-03-02');

------------------------ ############## Multi Source 
-- Storage----------------------------------	
/*
STORE claim_detail into '/hdfs/data/mr/group/ingestgp/organize/structured/core/claim_detail/proc_date=20140616' using PigStorage('|'); 
STORE claim_header into '/hdfs/data/mr/group/ingestgp/organize/structured/core/claim_header/proc_date=20140616' using PigStorage('|'); 
STORE claim_header into '/hdfs/data/mr/user/mlieber/organize/structured/core/claim_header/proc_date=20140601' using PigStorage('|'); 
*/

-- STORE claim_header into '$TARGET_TABLE1' USING org.apache.hcatalog.pig.HCatStorer();
STORE claim_header into '$SCHEMANAME.$TARGET_TABLE1' USING org.apache.hcatalog.pig.HCatStorer('proc_date=$PROCESS_DATE');
STORE claim_detail into '$SCHEMANAME.$TARGET_TABLE2' USING org.apache.hcatalog.pig.HCatStorer('proc_date=$PROCESS_DATE');

-- alter table claim_detail drop partition(proc_date='2014-03-03');


	 

											
										