# !/usr/bin/env python
# -*- coding: utf-8 -*-

from template.base_sql_task import *


#目前支持：RUNNER_SPARK_SQL和RUNNER_HIVE
#Creater:linxianglong1
#Creation Time:20200730
#Description:dmfalm_alm_rpt_01_pss_acct_tab_s_d  集团占款表
#不包含收入



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

-- 新建集团占款临时表
drop table if exists dmf_tmp.tmp_dmfalm_alm_rpt_01_pss_acct_tab_s_d;
create table if not exists dmf_tmp.tmp_dmfalm_alm_rpt_01_pss_acct_tab_s_d as
select * from dmf_alm.dmfalm_alm_rpt_01_pss_acct_tab_s_d where 1 = 2;

-- 资债表数据抽取到集团占款表中
insert into dmf_tmp.tmp_dmfalm_alm_rpt_01_pss_acct_tab_s_d
select 
    data_time,
    no_bear_interest_acct,
    rmb_cannib,
    grp_advance_fee,
    buyback,
    recv_cap_ocup,
    other_frgn_cur_cannib,
    frgn_cur_cannib,
    etl_date,
    data_type,
    0 as ep_sum_nr_amt,
    0 as dbt_loan_bal,
    0 as xbt_loan_bal,
	0 as xbt_180_bal,
    dt 
from dmf_alm.dmfalm_alm_rpt_01_group_pss_acct_new_s_d 
where dt = '{TX_DATE}'
;
 """,

  "sql_02": """
use dmf_tmp;

-- 企业白条
insert into dmf_tmp.tmp_dmfalm_alm_rpt_01_pss_acct_tab_s_d
select  
     '{TX_DATE}' as data_time,
     0 as no_bear_interest_acct,
     0 as rmb_cannib,
     0 as grp_advance_fee,
     0 as buyback,
     0 as recv_cap_ocup,
     0 as other_frgn_cur_cannib,
     0 as frgn_cur_cannib,
     '' as etl_date,
     '1' as data_type,
     sum_nr_amt  as ep_sum_nr_amt, --企业白条占用
     0 as dbt_loan_bal,
     0 as xbt_loan_bal,
	 0 as xbt_180_bal,
     data_dt as dt 
 from
 (
     select
         data_dt,
         cast(sum_nr_amt as double) as sum_nr_amt
     from dmf_ada.dmfada_sj_i9_sf_loan_balance_daily_i_d --老金采债项信息表(新版)
     where data_dt = '{TX_DATE}'
 ) a
;
 """,
 
  "sql_03": """
use dmf_tmp;

-- 个人大白条
insert into dmf_tmp.tmp_dmfalm_alm_rpt_01_pss_acct_tab_s_d
select  
    '{TX_DATE}' as data_time,
    0 as no_bear_interest_acct,
    0 as rmb_cannib,
    0 as grp_advance_fee,
    0 as buyback,
    0 as recv_cap_ocup,
    0 as other_frgn_cur_cannib,
    0 as frgn_cur_cannib,
    '' as etl_date,
    '1' as data_type,
    0  as ep_sum_nr_amt,
    loan_bal as dbt_loan_bal,
    0 as xbt_loan_bal,
	0 as xbt_180_bal,
    dt 
from dmf_ada.dmfada_sj_adrpt_xfjr_dbt_balance_day_s_d --个人白条每日余额-大小白整合版
where sys_code ='大白条' 
    and (nvl(trim(agreetment), '') = '' OR trim(agreetment) = 'N') 
    and dt ='{TX_DATE}'
;
 """,
 
   "sql_04": """
use dmf_tmp;

-- 小白条
insert into dmf_tmp.tmp_dmfalm_alm_rpt_01_pss_acct_tab_s_d
select  
    '{TX_DATE}' as data_time,
    0 as no_bear_interest_acct,
    0 as rmb_cannib,
    0 as grp_advance_fee,
    0 as buyback,
    0 as recv_cap_ocup,
    0 as other_frgn_cur_cannib,
    0 as frgn_cur_cannib,
    '' as etl_date,
    '1' as data_type,
    0  as ep_sum_nr_amt,
    0 as dbt_loan_bal,
    loan_bal as xbt_loan_bal,
	0 as xbt_180_bal,
    dt 
from dmf_ada.dmfada_sj_adrpt_xfjr_dbt_balance_day_s_d
where sys_code = '小白条' 
    and (nvl(trim(agreetment), '') = '' OR trim(agreetment) = 'N')  
    and dt = '{TX_DATE}'
;
 """,
 
"sql_05":"""
use dmf_tmp;
drop table dmf_tmp.tmp_dmfalm_alm_rpt_01_pss_acct_tab_overdue;
create table dmf_tmp.tmp_dmfalm_alm_rpt_01_pss_acct_tab_overdue as 
select t1.*
from (select *
	from dmf_ada.dmfada_adrpt_xbt_shouldpayamount_01_s_m t
    where t.dt = '{TX_DATE}' 
     and (
      (t.bizcode = 1 and t.merchantcode not in ('200023','200031'))
      or t.bizcode in (12, 16, 18)
      or (t.bizcode = 4 and t.merchantcode = 10001)
     )
    ) t1
where (nvl(trim(t1.specplan_code), '') = '' OR trim(t1.specplan_code) = 'N')
;
""",
 
 "sql_06": """
use dmf_tmp;

-- 小白 180+未入池资产
insert into dmf_tmp.tmp_dmfalm_alm_rpt_01_pss_acct_tab_s_d
select  
    '{TX_DATE}' as data_time,
    0 as no_bear_interest_acct,
    0 as rmb_cannib,
    0 as grp_advance_fee,
    0 as buyback,
    0 as recv_cap_ocup,
    0 as other_frgn_cur_cannib,
    0 as frgn_cur_cannib,
    '' as etl_date,
    '1' as data_type,
    0  as ep_sum_nr_amt,
    0 as dbt_loan_bal,
    0 as xbt_loan_bal,
	shouldpayamount as xbt_180_bal,
    dt 
from dmf_tmp.tmp_dmfalm_alm_rpt_01_pss_acct_tab_overdue t1
left join (select loanid
                ,case when writeofftime is not null then '已核销' else '未核销' end as writeoffstatus 
            from dmf_adm.dmfadm_admd_fi_xb_writeoff_i_d 
	        where dt <= '{TX_DATE}' 
	        group by loanid
			        ,case when writeofftime is not null then '已核销' else '未核销' end) t2
 on t1.loanid = t2.loanid
where t2.writeoffstatus = '已核销'
;
 """,
    
  "sql_07": """
use dmf_alm;
insert overwrite table dmf_alm.dmfalm_alm_rpt_01_pss_acct_tab_s_d PARTITION (dt = '{TX_DATE}') 
select 
    data_time,
    sum(no_bear_interest_acct) as no_bear_interest_acct ,
    sum(rmb_cannib) as rmb_cannib,
    sum(grp_advance_fee) as grp_advance_fee ,
    sum(buyback) as buyback ,
    sum(recv_cap_ocup) as recv_cap_ocup,
    sum(other_frgn_cur_cannib) as other_frgn_cur_cannib ,
    sum(frgn_cur_cannib) as frgn_cur_cannib ,
    default.sysdate() as etl_date,
    '1' as data_type,
    sum(ep_sum_nr_amt) as ep_sum_nr_amt , 
    sum(dbt_loan_bal) as dbt_loan_bal, 
    sum(xbt_loan_bal) as xbt_loan_bal,
	sum(xbt_180_bal) as xbt_180_bal
from dmf_tmp.tmp_dmfalm_alm_rpt_01_pss_acct_tab_s_d
group by data_time;
    """,
}


# 以下部分无需改动，除非作业有特殊要求
sql_task = SqlTask()
sql_task.set_sql_runner(sql_runner)
sql_task.set_customized_items(get_customized_items())
return_code = sql_task.execute_sqls(sql_map)
exit(return_code)