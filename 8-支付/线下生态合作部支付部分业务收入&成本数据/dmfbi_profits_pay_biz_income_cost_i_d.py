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
    TX_DATA_DATE = Time.date_sub(date=sql_task.get_tx_date(), itv=int(str(sql_task.get_tx_date())[8:10]))
    return locals()


sql_map={

     # ATTENTION:   ！！！！ sql_01  因为系统按字典顺序进行排序，小于10 的一定要写成0加编号，否则会顺序混乱，数据出问题，切记，切记！！！！
    "sql_01": """
use dmf_ada;
alter table dmf_bi.dmfbi_profits_pay_biz_income_cost_i_d drop if exists partition(dt = '{today}');
    """,

    "sql_02": """
use dmf_ada;
insert into dmf_bi.dmfbi_profits_pay_biz_income_cost_i_d partition(dt = '{today}') 
    select 
    day
    ,bd as user_num
    ,'代理侧bd' as bd_category
    ,'支付' as first_product
    ,'通道收入' as value_name
    ,'1' as value_category
    ,sum(income)as value 
    from 
    dmc_bc.dmcbc_bc_sd_trade_s_d
    where dt='{TX_DATE}'
    and company='京东'
    and refund_flag=0
    and day>='2020-06-01'
    group by day,bd
    
union

    select day
    ,bd as user_num
    ,'代理侧bd' as bd_category
    ,'支付' as first_product
    ,'通道成本' as value_name
    ,'0' as value_category
    ,sum(income)-sum(case when fee is not null then ord_amount*1/10000 end)as value 
    from 
    dmc_bc.dmcbc_bc_sd_trade_s_d
    where dt='{TX_DATE}'
    and company='京东'
    and refund_flag=0
    and day>='2020-06-01'
    group by day,bd
    
union 
    
    select 
    day
    ,bd as user_num
    ,'代理侧bd' as bd_category
    ,'支付' as first_product
    ,'校园收入' as value_name
    ,'1' as value_category
    ,sum(case when fee is null then ord_amount end)*2/1000 as value 
    from 
    (
    select * from dmc_bc.dmcbc_bc_sd_trade_s_d
    where dt='{TX_DATE}'
    and company='京东'
    and refund_flag=0
    and day>='2020-06-01'
    )a 
    join 
    dmc_add.dmcadd_add_chcn_hmd_xxst_campus_project_a_d b 
    on a.merchant_no=b.merchant_no 
    group by day,bd
    
union
    
    select day
    ,bd as user_num
    ,'代理侧bd' as bd_category
    ,'支付' as first_product
    ,'校园成本' as value_name
    ,'0' as value_category
    ,sum(case when fee is null then ord_amount end)*2/1000 as value 
    from 
    (
    select * from dmc_bc.dmcbc_bc_sd_trade_s_d
    where dt='{TX_DATE}'
    and company='京东'
    and refund_flag=0
    and day>='2020-06-01'
    )a 
    join 
    dmc_add.dmcadd_add_chcn_hmd_xxst_campus_project_a_d b 
    on a.merchant_no=b.merchant_no 
    group by day,bd
    
union 
    
    select day
    ,user_num
    ,'直营侧bd' as bd_category
    ,'支付' as first_product
    ,'通道收入' as income_category
    ,'1' as value_category,
    sum(income) as value
    from 
    dmc_bc.dmcbc_bc_sd_trade_s_d
    where dt='{TX_DATE}'
    and company != '京东'
    and refund_flag=0
    and day>='2020-06-01'
    group by day,user_num
    
 union  
    
    select day
    ,user_num
    ,'直营侧bd' as bd_category
    ,'支付' as first_product
    ,'校园收入' as value_name
    ,'1' as value_category
    ,sum(case when fee is null then ord_amount end)*2/1000 as value 
    from 
    (
    select * from dmc_bc.dmcbc_bc_sd_trade_s_d
    where dt='{TX_DATE}'
    and company!='京东'
    and refund_flag=0
    and day>='2020-06-01'
    )a 
    left join 
    dmc_add.dmcadd_add_chcn_hmd_xxst_campus_project_a_d b 
    on a.merchant_no=b.merchant_no 
    group by day,user_num
    
union

    select day
    ,user_num
    ,'直营侧bd' as bd_category
    ,'支付' as first_product
    ,'校园成本' as value_name
    ,'0' as value_category
    ,sum(case when fee is null then ord_amount end)*2/1000 as value 
    from 
    (
    select * from dmc_bc.dmcbc_bc_sd_trade_s_d
    where dt='{TX_DATE}'
    and company!='京东'
    and refund_flag=0
    and day>='2020-06-01'
    )a 
    left join 
    dmc_add.dmcadd_add_chcn_hmd_xxst_campus_project_a_d b 
    on a.merchant_no=b.merchant_no 
    group by day,user_num
    ;
    """,
}


# 以下部分无需改动，除非作业有特殊要求
sql_task = SqlTask()
sql_task.set_sql_runner(sql_runner)
sql_task.set_customized_items(get_customized_items())
return_code = sql_task.execute_sqls(sql_map)
exit(return_code)