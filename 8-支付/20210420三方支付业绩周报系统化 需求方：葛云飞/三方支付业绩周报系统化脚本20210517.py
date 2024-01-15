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
insert overwrite table dmf_bi.dmfbi_profits_pay_pay_inds_msg_thd_pty_week_a_d 
select
  dt as da_te,
  trade_name,
  product_id,
  mer_id,
  pay_tools,
  sum(order_amt) as order_amt,
  sum(trade_amt) as trade_amt,
  sum(mer_fee) as mer_fee,
  sum(bank_fee) as bank_fee,
  sum(trade_num) as trade_num,
  mer_big_type,
  mer_sma_type,
  prod_type, 
  '',
  '',
  '',
  '' 
from
  dmf_bc.dmfbc_oar_bc_pay_sis_fin_rpt_i_d
where
  dt >= '2019-01-01'
  and dept_nm = '支付业务部'
group by
  dt,
  mer_id,
  trade_name,
  product_id,
  pay_tools,
  mer_big_type,
  mer_sma_type,
  prod_type 
  
  
    """,

}


# 以下部分无需改动，除非作业有特殊要求
sql_task = SqlTask()
sql_task.set_sql_runner(sql_runner)
sql_task.set_customized_items(get_customized_items())
return_code = sql_task.execute_sqls(sql_map)
exit(return_code)