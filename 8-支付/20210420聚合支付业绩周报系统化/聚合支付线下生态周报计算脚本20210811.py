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
    insert overwrite table dmf_bi.dmfbi_profits_offline_pay_week_a_d 
    select
'week' date_type,
day,
case when agent_no 
in ('111936383',
'111609736',
'111962744',
'114688474',
'111012619',
'113721662',
'111109537',
'112361306',
'114431368',
'114930253',
'116150985')
then '生态' else '线下' end as hangye,
sum(income) as income,
sum(ord_amount)as ord_amount
from 
(select *
,case when fee is not null then income when region like '华北-代理' and sys_type='合利宝' and fee is null then ord_amount*0.00280476412824299
      when region='大客户-商超拓展' and sys_type='合利宝' and fee is null then ord_amount*9/10000
     else 0 end as maoli
from dmc_bc.dmcbc_bc_sd_trade_s_d
where 1=1
and day>='2020-01-01' 
and refund_flag=0
and merchant_second_no not in('110225410008','110225410016','111434658002','111317672003','110225410010','111097131013') --去掉京邦达
and dt='{TX_DATE}') a
where merchant_no not in 
(select merchant_no from dmf_bi.dmfbi_profits_pay_equip_mer_id_a_d
group by 1)-----去除聚合支付（设备）表中的商户号 
group by 1,2,3
    """,
}


# 以下部分无需改动，除非作业有特殊要求
sql_task = SqlTask()
sql_task.set_sql_runner(sql_runner)
sql_task.set_customized_items(get_customized_items())
return_code = sql_task.execute_sqls(sql_map)
exit(return_code)