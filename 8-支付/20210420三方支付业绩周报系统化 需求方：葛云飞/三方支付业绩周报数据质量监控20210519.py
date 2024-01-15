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
    use dmf_bi;
    insert overwrite table dmf_bi.dmfbi_profits_fi_pay_data_qty_thd_a_d 
    select 
    da_te
    ,trade_name
    ,product_id
    ,pay_tools
    ,sum(order_amt)
    ,sum(trade_amt)
    ,sum(mer_fee)
    ,sum(bank_fee)
    ,sum(trade_num)
    ,mer_big_type
    ,mer_sma_type
    ,prod_type
    ,product_2
    ,product_3
    ,hangyenew
    ,health_type_new
    from
    dmf_bi.dmfbi_profits_pay_pay_inds_msg_thd_pty_week_a_d
    where isnull(product_2)
    or isnull(product_3)
    or isnull(hangyenew)
    or isnull(health_type_new)
    group by
    da_te
    ,trade_name
    ,product_id
    ,pay_tools
    ,mer_big_type
    ,mer_sma_type
    ,prod_type
    ,product_2
    ,product_3
    ,hangyenew
    ,health_type_new
    """,
}


# 以下部分无需改动，除非作业有特殊要求
sql_task = SqlTask()
sql_task.set_sql_runner(sql_runner)
sql_task.set_customized_items(get_customized_items())
return_code = sql_task.execute_sqls(sql_map)
exit(return_code)