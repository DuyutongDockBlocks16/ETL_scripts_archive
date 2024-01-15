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
    TX_PRE_366_DATE = Time.date_sub(date=today, itv=366)
    TX_DATA_DATE = Time.date_sub(date=sql_task.get_tx_date(), itv=int(str(sql_task.get_tx_date())[8:10]))
    print("*************数据快照日期为：************")
    print(sql_task.get_tx_date())
    print("*************业务日期为：************")
    print(TX_DATA_DATE)
    print("*************最远业务日期为：************")
    print(TX_PRE_366_DATE)
    print("*************************")
    return locals()


sql_map={
     # ATTENTION:   ！！！！ sql_01  因为系统按字典顺序进行排序，小于10 的一定要写成0加编号，否则会顺序混乱，数据出问题，切记，切记！！！！
     # 建表，表结构保持与月大凭证相同
    
    "sql_01": """
	use dmf_tmp;
    drop table if exists dmf_tmp.tmp_jcw_dmfbi_profits_pay_qyzf_corp_week_rpt_s_d_dyt_detail_001;
	;
    """,
     
    "sql_02": """
    use dmf_tmp;---首先加工明细层，为列传行做准备
    create table if not exists dmf_tmp.tmp_jcw_dmfbi_profits_pay_qyzf_corp_week_rpt_s_d_dyt_detail_001 
    as 
        select 
        case when t.policy_type='8000' then '鼎鼎寿险' 
             when t.policy_type='7000' then '鼎鼎其他团财险' 
             when t.policy_type='4000' then '鼎鼎车险' 
             else t.policy_type end as policy_type,
        case when t.agent_id in ('JD1100000026517','JD3100000013043','JD1100000023946') then '东家'
             else '鼎鼎' end as remark,
        substr(t.created_time,1,10) as da_te,
        sum(t.prem_total_money) as gmv,
        sum(t.std_prem) as income,
        sum(t.std_prem)/1.06 as gaap
        from
        (
            select *,row_number ()over(partition by uuid order by modified_time desc)as rn from
            idm.idm_f02_insu_ddbd_policy_i_d
            where policy_year='1'
        ) t
        where t.rn=1
        and substr(t.created_time,1,10)>'2021-01-01'
        and status<>'-1'
        group by 1,2,3

    ;
    """,
    
    "sql_03": """
	use dmf_tmp;
    drop table if exists dmf_tmp.tmp_lifeinsu_agency_perform_rep_detail_002;
	;
    """,
    
    "sql_04": """
    use dmf_tmp;---首先加工明细层，为列传行做准备
    create table if not exists dmf_tmp.tmp_jcw_dmfbi_profits_pay_qyzf_corp_week_rpt_s_d_dyt_detail_002
    as  
    
    select
    da_te,
    policy_type,
    remark,
    gmv            as index_value,
    '交易额'       as index_name,
    'JDF.02'       as index_code
    from dmf_tmp.tmp_jcw_dmfbi_profits_pay_qyzf_corp_week_rpt_s_d_dyt_detail_001 
    
    union all
    
    select
    da_te,
    policy_type,
    remark,
    gaap           as index_value,
    '不含税收入'   as index_name,
    'JDF.09'       as index_code
    from dmf_tmp.tmp_jcw_dmfbi_profits_pay_qyzf_corp_week_rpt_s_d_dyt_detail_001 
    
    union all
    
    select
    da_te,
    policy_type,
    remark,
    gaap           as index_value,
    '不含税收入（还原出表）'   as index_name,
    'JDF.32'       as index_code
    from dmf_tmp.tmp_jcw_dmfbi_profits_pay_qyzf_corp_week_rpt_s_d_dyt_detail_001 
    
    union all
    
    select
    da_te,
    policy_type,
    remark,
    income         as index_value,
    '含税收入'     as index_name,
    'JDF.08'       as index_code
    from dmf_tmp.tmp_jcw_dmfbi_profits_pay_qyzf_corp_week_rpt_s_d_dyt_detail_001 
    
    ;
    """,
    
    "sql_05": """
	use dmf_tmp;
    drop table if exists dmf_tmp.tmp_jcw_dmfbi_profits_pay_qyzf_corp_week_rpt_s_d_dyt_detail_002;
	;
    """,
    
    "sql_06": """
    use dmf_tmp;---首先加工明细层，为列传行做准备
    create table if not exists dmf_tmp.tmp_jcw_dmfbi_profits_pay_qyzf_corp_week_rpt_s_d_dyt_detail_002 
    as  
    
    select
    da_te,
    remark,
    index_value,
    index_name,
    case 
    when policy_type = '鼎鼎其他团财险' then 'YWX.992.16.03'
    when policy_type = '鼎鼎车险' then 'YWX.992.16.02'
    when policy_type = '鼎鼎寿险' then 'YWX.992.16.01'
    else '业务线异常' end as buiz_line_code,
    index_code,
    '健康业务' as is_health,
    '0001-0002-0001-0011' as bizdept,
    policy_type
    from dmf_tmp.tmp_lifeinsu_agency_perform_rep_detail_002
    
    
    ;
    """,
    
    "sql_98": """
    use dmf_tmp;
    alter table dmf_bi.dmfbi_profits_lifeinsu_agency_perform_rep_s_d drop if exists partition(dt = '{TX_DATE}')
    ;
    """,    
    
    "sql_99": """
    use dmf_tmp;
    insert into table dmf_bi.dmfbi_profits_lifeinsu_agency_perform_rep_s_d partition(dt = '{TX_DATE}')
    select 
    *
    from dmf_tmp.tmp_lifeinsu_agency_perform_rep_sum_001 
    ;
    """,    

}


    

# 以下部分无需改动，除非作业有特殊要求
sql_task = SqlTask()
sql_task.set_sql_runner(sql_runner)
sql_task.set_customized_items(get_customized_items())
return_code = sql_task.execute_sqls(sql_map)
exit(return_code)