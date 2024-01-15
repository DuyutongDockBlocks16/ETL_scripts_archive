# !/usr/bin/env python
# -*- coding: utf-8 -*-

from template.base_sql_task import *


#
# 目前支持：RUNNER_SPARK_SQL和RUNNER_HIVE
#
sql_runner=RUNNER_HIVE


def get_customized_items():
    """
     if you need some special values in your sql, please define and calculate then here
     to refer it as {YOUR_VAR} in your sql
    """
    today = Time.today()
    TX_PRE_60_DATE = Time.date_sub(date=today, itv=60)
    TX_PRE_365_DATE = Time.date_sub(date=today, itv=365)
    return locals()


sql_map={

     # ATTENTION:   ！！！！ sql_01  因为系统按字典顺序进行排序，小于10 的一定要写成0加编号，否则会顺序混乱，数据出问题，切记，切记！！！！
    "sql_01": """
	use dmf_tmp;
    drop table if exists dmf_tmp.tmp_jcw_dmfbi_profits_pay_qyzf_corp_week_rpt_s_d_detail_001_dyt;
    """,
    
    "sql_02": """
    use dmf_tmp;
    create table if not exists dmf_tmp.tmp_jcw_dmfbi_profits_pay_qyzf_corp_week_rpt_s_d_detail_001_dyt
    as 
    
    select 
    stats_dt, --业务日期
    case 
    when prtnr_sht_name = '深度' then '深度' 
    else '其他' end as plat_name,--平台名称
    substring(stats_dt,1,7) yearmonth,
    substring(stats_dt,1,4) year
    ,sum(tx_succ_amt+0.0)-sum(tx_refund_amt+0.0) as gmv ---gmv
    ,sum(dpst_income_amt+0.0) as income ---收入
    ,sum(dpst_income_amt+0.0)/1.06 as gaap_income ---GAAP收入
    from sdm.sdm_f02_epay_all_prod_indx_sum_i_d 
    where 1=1
    and stats_dt>='2020-01-01'
    and stats_dt<='{TX_DATE}'
    and dim_code='03'
    and stats_prd= '当日'
    group by 1,2
    
    """,
    
    "sql_03": """
	use dmf_tmp;
    drop table if exists dmf_tmp.tmp_jcw_dmfbi_profits_pay_qyzf_corp_week_rpt_s_d_sum_001_dyt_shendu;
    """,
    
    "sql_04": """
    use dmf_tmp;
    create table if not exists dmf_tmp.tmp_jcw_dmfbi_profits_pay_qyzf_corp_week_rpt_s_d_sum_001_dyt_shendu
    as 
    
    select 
    stats_dt,
    plat_name,
    yearmonth,
    year,
    0.0 as index_value,---gmv
    0.0 as mtd_index_value,
    0.0 as ytd_index_value,
    'JDF.02' as index_code,
    '交易额' as index_name
    from dmf_tmp.tmp_jcw_dmfbi_profits_pay_qyzf_corp_week_rpt_s_d_detail_001_dyt 
    where plat_name='深度'
    
    union all 
    
    select 
    stats_dt,
    plat_name,
    yearmonth,
    year,
    income as index_value,---gmv
    sum(income) over(partition by yearmonth,plat_name order by stats_dt) as mtd_index_value,
    sum(income) over(partition by year,plat_name order by stats_dt) as ytd_index_value,
    'JDF.08' as index_code,
    '含税收入' as index_name
    from dmf_tmp.tmp_jcw_dmfbi_profits_pay_qyzf_corp_week_rpt_s_d_detail_001_dyt 
    where plat_name='深度'
    
    union all 
    
    select 
    stats_dt,
    plat_name,
    yearmonth,
    year,
    gaap_income as index_value,---gmv
    sum(gaap_income) over(partition by yearmonth,plat_name order by stats_dt) as mtd_index_value,
    sum(gaap_income) over(partition by year,plat_name order by stats_dt) as ytd_index_value,
    'JDF.09' as index_code,
    '不含税收入' as index_name
    from dmf_tmp.tmp_jcw_dmfbi_profits_pay_qyzf_corp_week_rpt_s_d_detail_001_dyt 
    where plat_name='深度'
    
    union all 
    
    select 
    stats_dt,
    plat_name,
    yearmonth,
    year,
    gaap_income as index_value,---gmv
    sum(gaap_income) over(partition by yearmonth,plat_name order by stats_dt) as mtd_index_value,
    sum(gaap_income) over(partition by year,plat_name order by stats_dt) as ytd_index_value,
    'JDF.32' as index_code,
    '不含税收入（还原出表）' as index_name
    from dmf_tmp.tmp_jcw_dmfbi_profits_pay_qyzf_corp_week_rpt_s_d_detail_001_dyt 
    where plat_name='深度'
    
    """,
    
    "sql_05": """
	use dmf_tmp;
    drop table if exists dmf_tmp.tmp_jcw_dmfbi_profits_pay_qyzf_corp_week_rpt_s_d_sum_001_dyt_qita;
    """,
    
    "sql_06": """
    use dmf_tmp;
    create table if not exists dmf_tmp.tmp_jcw_dmfbi_profits_pay_qyzf_corp_week_rpt_s_d_sum_001_dyt_qita
    as 
    
    select 
    stats_dt,
    plat_name,
    yearmonth,
    year,
    gmv as index_value,---gmv
    sum(gmv) over(partition by yearmonth,plat_name order by stats_dt) as mtd_index_value,
    sum(gmv) over(partition by year,plat_name order by stats_dt) as ytd_index_value,
    'JDF.02' as index_code,
    '交易额' as index_name
    from dmf_tmp.tmp_jcw_dmfbi_profits_pay_qyzf_corp_week_rpt_s_d_detail_001_dyt 
    where plat_name='其他'
    
    union all 
    
    select 
    stats_dt,
    plat_name,
    yearmonth,
    year,
    income as index_value,---gmv
    sum(income) over(partition by yearmonth,plat_name order by stats_dt) as mtd_index_value,
    sum(income) over(partition by year,plat_name order by stats_dt) as ytd_index_value,
    'JDF.08' as index_code,
    '含税收入' as index_name
    from dmf_tmp.tmp_jcw_dmfbi_profits_pay_qyzf_corp_week_rpt_s_d_detail_001_dyt 
    where plat_name='其他'
    
    union all 
    
    select 
    stats_dt,
    plat_name,
    yearmonth,
    year,
    gaap_income as index_value,---gmv
    sum(gaap_income) over(partition by yearmonth,plat_name order by stats_dt) as mtd_index_value,
    sum(gaap_income) over(partition by year,plat_name order by stats_dt) as ytd_index_value,
    'JDF.09' as index_code,
    '不含税收入' as index_name
    from dmf_tmp.tmp_jcw_dmfbi_profits_pay_qyzf_corp_week_rpt_s_d_detail_001_dyt 
    where plat_name='其他'
    
    union all 
    
    select 
    stats_dt,
    plat_name,
    yearmonth,
    year,
    gaap_income as index_value,---gmv
    sum(gaap_income) over(partition by yearmonth,plat_name order by stats_dt) as mtd_index_value,
    sum(gaap_income) over(partition by year,plat_name order by stats_dt) as ytd_index_value,
    'JDF.32' as index_code,
    '不含税收入（还原出表）' as index_name
    from dmf_tmp.tmp_jcw_dmfbi_profits_pay_qyzf_corp_week_rpt_s_d_detail_001_dyt 
    where plat_name='其他'
    
    """,
    
     "sql_07": """
	use dmf_tmp;
    drop table if exists dmf_tmp.tmp_jcw_dmfbi_profits_pay_qyzf_corp_week_rpt_s_d_sum_002_dyt;
    """,
    
    "sql_08": """
    use dmf_tmp;
    create table if not exists dmf_tmp.tmp_jcw_dmfbi_profits_pay_qyzf_corp_week_rpt_s_d_sum_002_dyt
    as 
    
    select
    *,
    '企业支付' as biz_type,
    '金融行业' as inds_mark,
    '企业支付产品' as project_lvl2,
    '企业支付产品' as project_lvl3,
    '金融科技群-支付' as hangye,
    '健康业务' as is_health,
    'YWX.992.08.50' as bizline_code,
    '支付业务部-企业支付' as bizline_name
    from
    dmf_tmp.tmp_jcw_dmfbi_profits_pay_qyzf_corp_week_rpt_s_d_sum_001_dyt_shendu
    
    union all
    
    select
    *,
    '企业支付' as biz_type,
    '' as inds_mark,
    '企业支付产品' as project_lvl2,
    '企业支付产品' as project_lvl3,
    '金融科技群-支付' as hangye,
    '健康业务' as is_health,
    'YWX.992.08.50' as bizline_code,
    '支付业务部-企业支付' as bizline_name
    from
    dmf_tmp.tmp_jcw_dmfbi_profits_pay_qyzf_corp_week_rpt_s_d_sum_001_dyt_qita
   
    
    """,
    
     "sql_09": """
	use dmf_tmp;
    drop table if exists dmf_tmp.tmp_jcw_dmfbi_profits_pay_qyzf_corp_week_rpt_s_d_sum_003_dyt;
    """,
    
    "sql_10": """
    use dmf_tmp;
    create table if not exists dmf_tmp.tmp_jcw_dmfbi_profits_pay_qyzf_corp_week_rpt_s_d_sum_003_dyt
    as 
    
    select
    aaa.*,
    biz_dept.bizdept
    from dmf_tmp.tmp_jcw_dmfbi_profits_pay_qyzf_corp_week_rpt_s_d_sum_002_dyt aaa
    left join dmf_add.dmfadd_add_pay_thd_pty_map_bizdept_a_d biz_dept on aaa.hangye=biz_dept.hangye  

    """,
    
    "sql_11": """
    use dmf_bi;
    alter table dmf_bi.dmfbi_profits_pay_qyzf_corp_week_rpt_s_d drop if exists partition(dt = '{TX_DATE}');
    """,
    
    "sql_12": """
    use dmf_bi;
    insert into dmf_bi.dmfbi_profits_pay_qyzf_corp_week_rpt_s_d partition (dt = '{TX_DATE}')
    select 
    stats_dt,
    plat_name,
    yearmonth,
    year,
    index_value,
    mtd_index_value,
    ytd_index_value,
    index_code,
    index_name,
    biz_type,
    inds_mark,
    project_lvl2,
    project_lvl3,
    hangye,
    is_health,
    bizline_code,
    bizline_name,
    bizdept
    from dmf_tmp.tmp_jcw_dmfbi_profits_pay_qyzf_corp_week_rpt_s_d_sum_003_dyt 
    """,
    
    "sql_13": """
    use dmf_bi;
    alter table dmf_bi.dmfbi_profits_pay_qyzf_corp_week_rpt_detail_s_d drop if exists partition(dt = '{TX_DATE}');
    """,
    
    "sql_14": """
    use dmf_bi;
    insert into dmf_bi.dmfbi_profits_pay_qyzf_corp_week_rpt_detail_s_d partition (dt = '{TX_DATE}')
    select 
    stats_dt,
    stats_prd,
    prod_name,
    prtnr_biz_type,
    prtnr_name,
    prtnr_sht_name,
    prtnr_inds_type,
    prtnr_belg_dept,
    tx_succ_cnt,
    tx_refund_cnt,
    tx_succ_amt,
    tx_refund_amt,
    day_avg_dpst_amt,
    curmth_dpst_fee_rate,
    dpst_income_amt,
    serv_fee_income_amt,
    wyzx_income_amt,
    tx_succ_amt-tx_refund_amt as gmv ---gmv
    from sdm.sdm_f02_epay_all_prod_indx_sum_i_d 
    where stats_dt>='2020-01-01'
    and stats_dt<='{TX_DATE}'
    and dim_code='03'
    """,
    
}


# 以下部分无需改动，除非作业有特殊要求
sql_task = SqlTask()
sql_task.set_sql_runner(sql_runner)
sql_task.set_customized_items(get_customized_items())
return_code = sql_task.execute_sqls(sql_map)
exit(return_code)