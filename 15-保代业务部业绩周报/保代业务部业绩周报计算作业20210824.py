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
    
    "sql_01": """
	use dmf_tmp;
    drop table if exists dmf_tmp.tmp_lifeinsu_agency_perform_rep_detail_001;
	;
    """,
     
    "sql_02": """
    use dmf_tmp;---首先加工明细层，为列传行做准备
    create table if not exists dmf_tmp.tmp_lifeinsu_agency_perform_rep_detail_001 
    as 
    select 
    substr(created_time,1,10) as da_te,
    remark
    ,sum(first_year_premium) as GMV----GMV
    ,sum(standard_premium)/1.06 as GAAP---GAAP
    from
    (
        select *,
        case when agent_id in ('JD1100000026517','JD3100000013043','JD1100000023946') then '东家'
             else '鼎鼎' end as remark
        ,row_number ()over(partition by uuid order by created_time asc)as rn 
        from odm.odm_insu_ddbd_policy_i_d
        where policy_type='8000'
        and policy_year='1'
        and dt<='{TX_DATE}'
    )
    where rn=1
    group by substr(created_time,1,10),remark
    ;
    """,
    
    "sql_03": """
	use dmf_tmp;
    drop table if exists dmf_tmp.tmp_lifeinsu_agency_perform_rep_detail_002;
	;
    """,
    
    "sql_04": """
    use dmf_tmp;---首先加工明细层，为列传行做准备
    create table if not exists dmf_tmp.tmp_lifeinsu_agency_perform_rep_detail_002 
    as  
    
    select
    da_te,
    remark,
    GMV         as index_value,
    '交易额'       as index_name,
    from dmf_tmp.tmp_lifeinsu_agency_perform_rep_detail_001 
    
    union all
    
    select
    da_te,
    remark,
    GAAP        as index_value,
    '不含税收入'      as index_name
    from dmf_tmp.tmp_lifeinsu_agency_perform_rep_detail_001 
    
    ;
    """,
    
    "sql_05": """
	use dmf_tmp;
    drop table if exists dmf_tmp.tmp_lifeinsu_agency_perform_rep_sum_001;
	;
    """,
    
    "sql_06": """
    use dmf_tmp;---首先加工明细层，为列传行做准备
    create table if not exists dmf_tmp.tmp_lifeinsu_agency_perform_rep_sum_001 
    as  
    
    select
    da_te,
    remark,
    index_value,
    index_name,
    'YWX.992.16.01' as buiz_line_code,
    case when index_name = '交易额'          then 'JDF.02'
         when index_name = '不含税收入'      then 'JDF.09' 
    else '科目异常' end as buiz_subj_code,
    '健康' as is_health

    from dmf_tmp.tmp_lifeinsu_agency_perform_rep_detail_002
    
    
    ;
    """,
    
    "sql_07": """
    use dmf_tmp;
    alter table dmf_bi.dmfbi_profits_lifeinsu_agency_perform_rep_s_d drop if exists partition(dt = '{TX_DATE}')
    ;
    """,    
    
    "sql_08": """
    use dmf_tmp;
    insert into table dmf_rpt.dmfbi_profits_lifeinsu_agency_perform_rep_s_d partition(dt = '{TX_DATE}')
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