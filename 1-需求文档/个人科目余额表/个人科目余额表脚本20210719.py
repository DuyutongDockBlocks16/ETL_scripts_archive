# !/usr/bin/env python
# -*- coding: utf-8 -*-

from template.base_sql_task import *


#
# 目前支持：RUNNER_SPARK_SQL和RUNNER_HIVE
#
# 个人科目余额表脚本
sql_runner=RUNNER_HIVE


def get_customized_items():
    """
     if you need some special values in your sql, please define and calculate then here
     to refer it as {YOUR_VAR} in your sql
    """
    today = Time.today()
    TX_PRE_60_DATE = Time.date_sub(date=today, itv=60)
    TX_PRE_365_DATE = Time.date_sub(date=today, itv=365)
    TX_DATA_DATE = Time.date_sub(date=sql_task.get_tx_date(), itv=int(str(sql_task.get_tx_date())[8:10]))
    print("*************数据快照日期为：************")
    print(sql_task.get_tx_date())
    print("*************业务日期为：************")
    print(TX_DATA_DATE)
    print("*************************")
    return locals()


sql_map={
     # ATTENTION:   ！！！！ sql_01  因为系统按字典顺序进行排序，小于10 的一定要写成0加编号，否则会顺序混乱，数据出问题，切记，切记！！！！
     # 建表，表结构保持与月大凭证相同
    
    "sql_02": """
	use dmf_tmp;
    drop table if exists dmf_tmp.tmp_cwmart_month_evidence_detail_s;
	;
    """,
     
     "sql_03": """
    use dmf_tmp;---抽所有集市业务的月大凭证，最后unionall起来
    create table if not exists dmf_tmp.tmp_cwmart_month_evidence_detail_s 
    as
    select
    *
    from odm.odm_fi_tz_dm_gj_general_g_journal_voucher_sync_000001_month_i_i_d 
    where kd_voucher_status ='SUCCESS'---取EBS回传状态为真的数据 
    and status='0'---只取有效的凭证    
    and manual_dt='{TXPREMONTH}'--只取记账时间为上月的数据
    
    union all
    select
    *
    from odm.odm_fi_tz_dm_gj_general_g_journal_voucher_sync_000003_month_i_i_d 
    where kd_voucher_status ='SUCCESS'---取EBS回传状态为真的数据 
    and status='0'---只取有效的凭证    
    and manual_dt='{TXPREMONTH}'--只取记账时间为上月的数据
    
    union all
    select
    *
    from odm.odm_fi_tz_dm_gj_general_g_journal_voucher_sync_000004_month_i_i_d 
    where kd_voucher_status ='SUCCESS'---取EBS回传状态为真的数据 
    and status='0'---只取有效的凭证    
    and manual_dt='{TXPREMONTH}'--只取记账时间为上月的数据
    
    union all
    select
    *
    from odm.odm_fi_tz_dm_gj_general_g_journal_voucher_sync_000005_month_i_i_d 
    where kd_voucher_status ='SUCCESS'---取EBS回传状态为真的数据 
    and status='0'---只取有效的凭证    
    and manual_dt='{TXPREMONTH}'--只取记账时间为上月的数据
    
    union all
    select
    *
    from odm.odm_fi_tz_dm_gj_general_g_journal_voucher_sync_000006_month_i_i_d 
    where kd_voucher_status ='SUCCESS'---取EBS回传状态为真的数据 
    and status='0'---只取有效的凭证    
    and manual_dt='{TXPREMONTH}'--只取记账时间为上月的数据
    
    union all
    select
    *
    from odm.odm_fi_tz_dm_gj_general_g_journal_voucher_sync_000007_month_i_i_d 
    where kd_voucher_status ='SUCCESS'---取EBS回传状态为真的数据 
    and status='0'---只取有效的凭证    
    and manual_dt='{TXPREMONTH}'--只取记账时间为上月的数据
    
    union all
    select
    *
    from odm.odm_fi_tz_dm_gj_general_g_journal_voucher_sync_000008_month_i_i_d 
    where kd_voucher_status ='SUCCESS'---取EBS回传状态为真的数据 
    and status='0'---只取有效的凭证    
    and manual_dt='{TXPREMONTH}'--只取记账时间为上月的数据
    
    union all
    select
    *
    from odm.odm_fi_tz_dm_gj_general_g_journal_voucher_sync_000009_month_i_i_d 
    where kd_voucher_status ='SUCCESS'---取EBS回传状态为真的数据 
    and status='0'---只取有效的凭证    
    and manual_dt='{TXPREMONTH}'--只取记账时间为上月的数据
    
    union all
    select
    *
    from odm.odm_fi_tz_dm_gj_general_g_journal_voucher_sync_000010_month_i_i_d 
    where kd_voucher_status ='SUCCESS'---取EBS回传状态为真的数据 
    and status='0'---只取有效的凭证    
    and manual_dt='{TXPREMONTH}'--只取记账时间为上月的数据
    
    union all
    select
    *
    from odm.odm_fi_tz_dm_gj_general_g_journal_voucher_sync_000011_month_i_i_d 
    where kd_voucher_status ='SUCCESS'---取EBS回传状态为真的数据 
    and status='0'---只取有效的凭证    
    and manual_dt='{TXPREMONTH}'--只取记账时间为上月的数据
    
    union all
    select
    *
    from odm.odm_fi_tz_dm_gj_general_g_journal_voucher_sync_000012_month_i_i_d 
    where kd_voucher_status ='SUCCESS'---取EBS回传状态为真的数据 
    and status='0'---只取有效的凭证    
    and manual_dt='{TXPREMONTH}'--只取记账时间为上月的数据
    
    union all
    select
    *
    from odm.odm_fi_tz_dm_gj_general_g_journal_voucher_sync_000014_month_i_i_d 
    where kd_voucher_status ='SUCCESS'---取EBS回传状态为真的数据 
    and status='0'---只取有效的凭证    
    and manual_dt='{TXPREMONTH}'--只取记账时间为上月的数据
    
    union all
    select
    *
    from odm.odm_fi_tz_dm_gj_general_g_journal_voucher_sync_000015_month_i_i_d 
    where kd_voucher_status ='SUCCESS'---取EBS回传状态为真的数据 
    and status='0'---只取有效的凭证    
    and manual_dt='{TXPREMONTH}'--只取记账时间为上月的数据
    
    union all
    select
    *
    from odm.odm_fi_tz_dm_gj_general_g_journal_voucher_sync_000016_month_i_i_d 
    where kd_voucher_status ='SUCCESS'---取EBS回传状态为真的数据 
    and status='0'---只取有效的凭证    
    and manual_dt='{TXPREMONTH}'--只取记账时间为上月的数据
    
    union all
    select
    *
    from odm.odm_fi_tz_dm_gj_general_g_journal_voucher_sync_000017_month_i_i_d 
    where kd_voucher_status ='SUCCESS'---取EBS回传状态为真的数据 
    and status='0'---只取有效的凭证    
    and manual_dt='{TXPREMONTH}'--只取记账时间为上月的数据
    
    union all
    select
    *
    from odm.odm_fi_tz_dm_gj_general_g_journal_voucher_sync_000019_month_i_i_d 
    where kd_voucher_status ='SUCCESS'---取EBS回传状态为真的数据 
    and status='0'---只取有效的凭证    
    and manual_dt='{TXPREMONTH}'--只取记账时间为上月的数据
    
    union all
    select
    *
    from odm.odm_fi_tz_dm_gj_general_g_journal_voucher_sync_000020_month_i_i_d 
    where kd_voucher_status ='SUCCESS'---取EBS回传状态为真的数据 
    and status='0'---只取有效的凭证    
    and manual_dt='{TXPREMONTH}'--只取记账时间为上月的数据
    
    union all
    select
    *
    from odm.odm_fi_tz_dm_gj_general_g_journal_voucher_sync_000021_month_i_i_d 
    where kd_voucher_status ='SUCCESS'---取EBS回传状态为真的数据 
    and status='0'---只取有效的凭证    
    and manual_dt='{TXPREMONTH}'--只取记账时间为上月的数据
    
    union all
    select
    *
    from odm.odm_fi_tz_dm_gj_general_g_journal_voucher_sync_000022_month_i_i_d 
    where kd_voucher_status ='SUCCESS'---取EBS回传状态为真的数据 
    and status='0'---只取有效的凭证    
    and manual_dt='{TXPREMONTH}'--只取记账时间为上月的数据
    
    union all
    select
    *
    from odm.odm_fi_tz_dm_gj_general_g_journal_voucher_sync_000023_month_i_i_d 
    where kd_voucher_status ='SUCCESS'---取EBS回传状态为真的数据 
    and status='0'---只取有效的凭证    
    and manual_dt='{TXPREMONTH}'--只取记账时间为上月的数据
    
    union all
    select
    *
    from odm.odm_fi_tz_dm_gj_general_g_journal_voucher_sync_000024_month_i_i_d 
    where kd_voucher_status ='SUCCESS'---取EBS回传状态为真的数据 
    and status='0'---只取有效的凭证    
    and manual_dt='{TXPREMONTH}'--只取记账时间为上月的数据
    
    union all
    select
    *
    from odm.odm_fi_tz_dm_gj_general_g_journal_voucher_sync_000028_month_i_i_d 
    where kd_voucher_status ='SUCCESS'---取EBS回传状态为真的数据 
    and status='0'---只取有效的凭证    
    and manual_dt='{TXPREMONTH}'--只取记账时间为上月的数据
    
    union all
    select
    *
    from odm.odm_fi_tz_dm_gj_general_g_journal_voucher_sync_000029_month_i_i_d 
    where kd_voucher_status ='SUCCESS'---取EBS回传状态为真的数据 
    and status='0'---只取有效的凭证    
    and manual_dt='{TXPREMONTH}'--只取记账时间为上月的数据
    
    union all
    select
    *
    from odm.odm_fi_tz_dm_gj_general_g_journal_voucher_sync_000030_month_i_i_d 
    where kd_voucher_status ='SUCCESS'---取EBS回传状态为真的数据 
    and status='0'---只取有效的凭证    
    and manual_dt='{TXPREMONTH}'--只取记账时间为上月的数据
    
    union all
    select
    *
    from odm.odm_fi_tz_dm_gj_general_g_journal_voucher_sync_000031_month_i_i_d 
    where kd_voucher_status ='SUCCESS'---取EBS回传状态为真的数据 
    and status='0'---只取有效的凭证    
    and manual_dt='{TXPREMONTH}'--只取记账时间为上月的数据
    
    union all
    select
    *
    from odm.odm_fi_tz_dm_gj_general_g_journal_voucher_sync_000032_month_i_i_d 
    where kd_voucher_status ='SUCCESS'---取EBS回传状态为真的数据 
    and status='0'---只取有效的凭证    
    and manual_dt='{TXPREMONTH}'--只取记账时间为上月的数据
    
    union all
    select
    *
    from odm.odm_fi_hs_dm_gj_general_g_journal_voucher_sync_000035_month_i_i_d 
    where kd_voucher_status ='SUCCESS'---取EBS回传状态为真的数据 
    and status='0'---只取有效的凭证    
    and manual_dt='{TXPREMONTH}'--只取记账时间为上月的数据
    
    union all
    select
    *
    from odm.odm_fi_hs_dm_gj_general_g_journal_voucher_sync_000036_month_i_i_d 
    where kd_voucher_status ='SUCCESS'---取EBS回传状态为真的数据 
    and status='0'---只取有效的凭证    
    and manual_dt='{TXPREMONTH}'--只取记账时间为上月的数据
    
    union all
    select
    *
    from odm.odm_fi_hs_dm_gj_general_g_journal_voucher_sync_000037_month_i_i_d 
    where kd_voucher_status ='SUCCESS'---取EBS回传状态为真的数据 
    and status='0'---只取有效的凭证    
    and manual_dt='{TXPREMONTH}'--只取记账时间为上月的数据
    
    union all
    select
    *
    from odm.odm_fi_hs_dm_gj_general_g_journal_voucher_sync_000038_month_i_i_d 
    where kd_voucher_status ='SUCCESS'---取EBS回传状态为真的数据 
    and status='0'---只取有效的凭证    
    and manual_dt='{TXPREMONTH}'--只取记账时间为上月的数据
    
    union all
    select
    *
    from odm.odm_fi_hs_dm_gj_general_g_journal_voucher_sync_000039_month_i_i_d 
    where kd_voucher_status ='SUCCESS'---取EBS回传状态为真的数据 
    and status='0'---只取有效的凭证    
    and manual_dt='{TXPREMONTH}'--只取记账时间为上月的数据
    
    union all
    select
    *
    from odm.odm_fi_hs_dm_gj_general_g_journal_voucher_sync_000041_month_i_i_d 
    where kd_voucher_status ='SUCCESS'---取EBS回传状态为真的数据 
    and status='0'---只取有效的凭证    
    and manual_dt='{TXPREMONTH}'--只取记账时间为上月的数据
    
    union all
    select
    *
    from odm.odm_fi_hs_dm_gj_general_g_journal_voucher_sync_000042_month_i_i_d 
    where kd_voucher_status ='SUCCESS'---取EBS回传状态为真的数据 
    and status='0'---只取有效的凭证    
    and manual_dt='{TXPREMONTH}'--只取记账时间为上月的数据
    ;
    """,
    
    "sql_04": """
    use dmf_tmp;
    drop table if exists dmf_tmp.tmp_cwmart_month_evidence_personmerchant_detail_s ;
    ;
    """,
        
     "sql_05": """
    use dmf_tmp;
     create table if not exists dmf_tmp.tmp_cwmart_month_evidence_personmerchant_detail_s
     as
        select 
        trim(voucher.ebs_company_code)      as ebs_company_code,  --EBS公司编码
        trim(voucher.subject_no)            as subject_no   ,     --EBS科目编码      
        case when (voucher.aux_subject_1_type='3' or voucher.aux_subject_1_type='2#2') then trim(aux_subject_1)
             when (voucher.aux_subject_2_type='3' or voucher.aux_subject_1_type='2#2') then trim(aux_subject_2)
             when (voucher.aux_subject_3_type='3' or voucher.aux_subject_1_type='2#2') then trim(aux_subject_3)
        else '' end                         as merchant_id   , --客商id,取辅助核算3和2#2这俩都是客商的意思
        trim(voucher.jdfin_code)            as ebs_biz_code  , --EBS业务线编码（虽然叫金蝶code）
        case when voucher.aux_subject_1_type='4' then trim(aux_subject_1)
             when voucher.aux_subject_2_type='4' then trim(aux_subject_2)
             when voucher.aux_subject_3_type='4' then trim(aux_subject_3)
        else '' end                         as ebs_proj_code , ---可能出现在任何一个辅助核算中，用此方法循环查找
        voucher.direction_em                as direction_em,   --借贷方向 0借1贷
        case 
        when direction_em='0' and positive_or_negative='0' then abs(voucher.trans_amt)
        when direction_em='0' and positive_or_negative='1' then 0.0-abs(voucher.trans_amt)
        when direction_em='0' and positive_or_negative='2' then voucher.trans_amt   
        when direction_em='0' and positive_or_negative='3' then 0.0-voucher.trans_amt        
        else 0.0     end                    as jf_amt,       --借方金额，这个逻辑来源于建华
        case 
        when direction_em='1' and positive_or_negative='0' then abs(voucher.trans_amt)
        when direction_em='1' and positive_or_negative='1' then 0.0-abs(voucher.trans_amt)
        when direction_em='1' and positive_or_negative='2' then voucher.trans_amt   
        when direction_em='1' and positive_or_negative='3' then 0.0-voucher.trans_amt 
        else 0.0    end                     as df_amt,       --贷方金额
        voucher.manual_dt                   as manual_dt,    --记账时间
        voucher.currency                    as currency      --币种
        from 
        (select * from dmf_tmp.tmp_cwmart_month_evidence_detail_s) voucher--算那天要哪天
        inner join 
        (
        select distinct vendor_site_code,dt from
            odm.odm_fi_fin_subject_merchant_s_d 
            where 1=1
            and yn_flag='1' 
            and company_name = '个人（汇总）'--只要为个人汇总客商的数据
        ) merchant
        on 
        voucher.dt=merchant.dt and voucher.ebs_vendor_side_code=merchant.vendor_site_code ---主数据可能会修改个人汇总的客商编码，这里做动态取数
    ;
    """,    
    
    "sql_06": """
    use dmf_tmp;
    drop table if exists dmf_tmp.tmp_cwmart_month_evidence_personmerchant_detail_002_s ;
    ;
    """,
    
     "sql_07": """
    use dmf_tmp;
    create table if not exists dmf_tmp.tmp_cwmart_month_evidence_personmerchant_detail_002_s 
    as
    select 
    '{TXPREMONTH}'              as periodname      ,    ---期间
    aaa.ebs_company_code        as ebs_company_code,    ---EBS公司编码
    aaa.subject_no              as subject_no,          ---EBS科目编码
    aaa.merchant_id             as merchant_id,         ---客商id
    aaa.ebs_biz_code            as ebs_biz_code,        ---EBS业务线编码
    aaa.ebs_proj_code           as ebs_proj_code,       ---EBS项目编码
    aaa.jf_amt                  as jf_amt,              ---借方金额(原币)
    aaa.df_amt                  as df_amt,              ---贷方金额(原币)
    aaa.manual_dt               as manual_dt,           ---记账时间(年月格式202106)
    concat(substr(aaa.manual_dt,0,4),'-',substr(aaa.manual_dt,5,2),'-01') 
                                as currency_date,       ---汇率时间(年月日格式2021-06-01)，这个逻辑来源于建华
    aaa.currency                as currency,            ---币种编码
    curinfo.currency_brief_code as currency_brief_code, ---币种标准名
    trim(bwb.base_currency_code) as base_currency_code, ---本位币标准名
    mercode.merchant_code       as merchant_code        ---客商编码
    from (select * from dmf_tmp.tmp_cwmart_month_evidence_personmerchant_detail_s)aaa   
    left join
    (
    select id,merchant_code from
    odm.odm_fi_fin_subject_merchant_s_d 
    where 1=1
    and yn_flag='1' 
    and dt =default.sysdate(-1)  --主数据的历史dt脏数据太多了，重跑的时候用最新版本的映射
    )mercode
    on aaa.merchant_id=mercode.id ---按最新dt客商编码 
    left join 
    (
    select currency_brief_code,jde_currency_code from odm.odm_fi_zj_bd_currency_info_i_d 
    where status='1') curinfo
    on aaa.currency=curinfo.jde_currency_code
    left join --需要根据知道每笔凭证对应的公司本位币是多少
    (
       select
       a.coa_co_code,a.base_currency_code
       from(
       select
       coa_co_code,base_currency_code
       ,row_number() over(partition by coa_co_code order by incremental_date desc) as rn
       from 
       odm.odm_fi_ebs_cux_jdtmas_gl_balances_v_i_d) a --从科目余额表里取出公司编码对应的公司本位币
       where rn=1
       group by coa_co_code,base_currency_code
    )  bwb
    on aaa.ebs_company_code = bwb.coa_co_code 
    ;
    """,       

    "sql_08": """
    use dmf_tmp;
    drop table if exists dmf_tmp.tmp_cwmart_month_evidence_personmerchant_detail_003_s ;
    ;
    """,
    
    "sql_09": """
    use dmf_tmp;
    create table if not exists dmf_tmp.tmp_cwmart_month_evidence_personmerchant_detail_003_s 
    as
    select
    '{TXPREMONTH}'                  as periodname,          ---期间
    voucher.ebs_company_code        as ebs_company_code,    ---EBS公司编码
    voucher.subject_no              as subject_no,          ---EBS科目编码
    voucher.ebs_biz_code            as ebs_biz_code,        ---EBS业务线编码
    voucher.ebs_proj_code           as ebs_proj_code,       ---EBS项目编码
    voucher.jf_amt                  as jf_amt,              ---借方金额(原币)
    voucher.df_amt                  as df_amt,              ---贷方金额(原币)
    voucher.jf_amt*(case when currency.conversion_rate is null then 1.0 else currency.conversion_rate end)
                                    as jf_amt_base,         ---借方金额(本位币)
    voucher.df_amt*(case when currency.conversion_rate is null then 1.0 else currency.conversion_rate end)
                                    as df_amt_base ,        ---贷方金额(本位币)
    voucher.manual_dt               as manual_dt,           ---记账时间(年月格式202106)
    voucher.currency_date           as currency_date,       ---汇率时间(年月日格式2021-06-01)
    voucher.currency_brief_code     as currency_brief_code, ---币种标准名
    voucher.base_currency_code      as base_currency_code,  ---本位币标准名
    voucher.merchant_code           as merchant_code        ---客商编码
    from (select * from dmf_tmp.tmp_cwmart_month_evidence_personmerchant_detail_002_s ) voucher
    left join 
    (
        select 
        substr(conversion_date,1,10) as conversion_date,
        trim(from_currency) as from_currency,
        trim(to_currency) as to_currency,
        conversion_rate as conversion_rate
        from odm.ODM_FI_EBS_CUX_JDTMAS_DAILY_RATES_V_I_D
        where substr(conversion_date,1,10) like '%-01' --因为汇率日期都是01结尾的，所以在此做过滤减少数据量
    ) currency
    on  voucher.currency_date=currency.conversion_date 
    and voucher.currency_brief_code=currency.from_currency
    and voucher.base_currency_code=currency.to_currency
    
    ;
    """,  
    
    "sql_10": """
    use dmf_tmp;
    drop table if exists dmf_tmp.tmp_cwmart_month_evidence_personmerchant_sum ;
    ;
    """,
    
    
    "sql_11": """
    use dmf_tmp;
    create table if not exists dmf_tmp.tmp_cwmart_month_evidence_personmerchant_sum
    as 
    select 
    periodname,
    ebs_company_code,
    subject_no,
    ebs_biz_code,
    ebs_proj_code,
    manual_dt,
    currency_date,
    currency_brief_code,
    base_currency_code,
    merchant_code,
    sum(jf_amt)      as  period_net_dr    ,--借方原币期间发生额
    sum(df_amt)      as  period_net_cr    ,--贷方原币期间发生额
    sum(jf_amt_base) as  period_net_dr_beq,--借方本位币期间发生额
    sum(df_amt_base) as  period_net_cr_beq --贷方本位币期间发生额
    from (select * from dmf_tmp.tmp_cwmart_month_evidence_personmerchant_detail_003_s ) aaa
    group by 
    periodname,
    ebs_company_code,
    subject_no,
    ebs_biz_code,
    ebs_proj_code,
    manual_dt,
    currency_date,
    currency_brief_code,
    base_currency_code,
    merchant_code
    ;
    """,
    
    "sql_12": """
    use dmf_tmp;
    alter table dmf_rpt.dmfrpt_eas_sf_pers_subj_bal_tab_amt_i_m drop if exists partition(dt = '{TX_DATA_DATE}')
    ;
    """,    
    
    "sql_13": """
    use dmf_tmp;
    insert into table dmf_rpt.dmfrpt_eas_sf_pers_subj_bal_tab_amt_i_m partition(dt = '{TX_DATA_DATE}')
    select * 
    from(
    select
    '{TXPREMONTH}'              as periodname,
    voucher.ebs_company_code    as company_code,
    ''                          as company_name,
    voucher.base_currency_code  as base_currency_code,
    voucher.currency_brief_code as currency,
    voucher.subject_no          as subject_code,
    ''                          as subject_name,
    voucher.merchant_code       as merchant_code,
    ''                          as merchant_name,
    voucher.ebs_biz_code        as bizline_code,
    ''                          as bizline_name,
    voucher.ebs_proj_code       as project_code,
    ''                          as project_name,
    coalesce(preperiod.end_balance,0.0)      as begin_balance,--期初余额（原币）上期期末作为本期期初
    coalesce(preperiod.end_balance_beq,0.0)  as begin_balance_beq,--期初余额（本位币）
    coalesce(voucher.period_net_dr,0.0)      as period_net_dr,--借方原币期间发生额
    coalesce(voucher.period_net_cr,0.0)      as period_net_cr,--贷方原币期间发生额
    coalesce(voucher.period_net_dr_beq,0.0)  as period_net_dr_beq,--借方本位币期间发生额
    coalesce(voucher.period_net_cr_beq,0.0)  as period_net_cr_beq,--贷方本位币期间发生额
    coalesce(preperiod.year_sum_dr,0.0)+coalesce(voucher.period_net_dr,0.0)
                                             as year_sum_dr,--本年累计借方（原币）
    coalesce(preperiod.year_sum_cr,0.0)+coalesce(voucher.period_net_cr,0.0)  
                                             as year_sum_cr,--本年累计贷方（原币）
    coalesce(preperiod.year_sum_dr_beq,0.0)+coalesce(voucher.period_net_dr_beq,0.0) 
                                             as year_sum_dr_beq,--本年累计借方（本位币）
    coalesce(preperiod.year_sum_cr_beq,0.0)+coalesce(voucher.period_net_cr_beq,0.0) 
                                             as year_sum_cr_beq,--本年累计借方（本位币）
    coalesce(preperiod.end_balance,0.0)+coalesce(voucher.period_net_dr,0.0)-coalesce(voucher.period_net_cr,0.0)
                                             as end_balance ,--期末余额（原币）
    coalesce(preperiod.end_balance_beq,0.0)+coalesce(voucher.period_net_dr_beq,0.0)-coalesce(voucher.period_net_cr_beq,0.0)
                                             as end_balance_beq --期末余额（本位币）                                         
    from dmf_tmp.tmp_cwmart_month_evidence_personmerchant_sum voucher
    FULL OUTER JOIN (
        select * from dmf_rpt.dmfrpt_eas_sf_pers_subj_bal_tab_amt_i_m 
        where
        case when substr('{TXPREMONTH}',5,6) = '12' then period_name=concat('{TXPREYEAR}','11')--如果算12月数据的时候，取11月数据需要特殊处理
             when substr('{TXPREMONTH}',5,6) = '01' then 1=2--如果是算1月的数据，则没有上期的数据，直接全部过滤
             else period_name=substr('{TXPREMONTH}'-1,0,6)  end ---取上个期间的期末，当这个期间的期初
        ) preperiod
    on  voucher.ebs_company_code=preperiod.company_code
    and voucher.subject_no=preperiod.subject_code
    and voucher.ebs_biz_code=preperiod.bizline_code
    and voucher.ebs_proj_code=preperiod.project_code
    and voucher.currency_brief_code=preperiod.currency
    and voucher.base_currency_code=preperiod.base_currency_code
    and voucher.merchant_code=preperiod.merchant_code   
    ) res
    where 1=1
    and (
         abs(res.begin_balance)>0.001 or
         abs(res.begin_balance_beq)>0.001 or
         abs(res.period_net_dr)>0.001 or
         abs(res.period_net_cr)>0.001 or
         abs(res.period_net_dr_beq)>0.001 or
         abs(res.period_net_cr_beq)>0.001 or
         abs(res.end_balance)>0.001 or
         abs(res.end_balance_beq)>0.001 or
         abs(res.year_sum_dr)>0.001 or
         abs(res.year_sum_cr)>0.001 or
         abs(res.year_sum_dr_beq)>0.001 or
         abs(res.year_sum_cr_beq)>0.001 
            )
    ;
    """,    


}


    

# 以下部分无需改动，除非作业有特殊要求
sql_task = SqlTask()
sql_task.set_sql_runner(sql_runner)
sql_task.set_customized_items(get_customized_items())
return_code = sql_task.execute_sqls(sql_map)
exit(return_code)