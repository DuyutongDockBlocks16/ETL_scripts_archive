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
    insert overwrite table dmf_bi.dmfbi_profits_fi_pay_un_equip_week_a_d 
select day,weekday,cate,sum(total_gmv) as gmv,sum(total_shouxufei) as shoruu, sum(orders_num) as num 
from
(
SELECT
date_add(to_date(day),(6-datediff(to_date(day),'2016-01-01')%7)) as weekday,
day,
cate,
merchant_no1,
sum(gmv) as total_gmv,
sum(shouxufei) as total_shouxufei,
sum(orders) as orders_num,
sum(active_orders_day) as is_huoyue
from 
(
	select day,case
	when company='哆啦宝' then '哆啦宝'
	when sys_type='哆啦宝' then '哆啦宝-京东'
	when sys_type='乐惠' then '乐惠'
	else '区域' end as cate,
	case when sys_type='哆啦宝' then shop_no else merchant_no end as merchant_no1,
	sum(ord_amount) as gmv,
	sum(COALESCE(fee-cost,0)) as shouxufei,
	sum(case when ord_amount>=1 then 1 else 0 end)as active_orders_day,
	count(distinct order_id1) as orders
	FROM(
		select day,company,sys_type,shop_no,merchant_no,ord_amount,fee,cost,order_id1
		from dmc_bc.dmcbc_bc_sd_trade_s_d
		where dt='{TX_PREDATE}'
		and day>='2021-01-01'
		and region not in ('其他')
		and refund_flag=0
		and merchant_second_no not in ( '110225410008','110225410016')
		group by day,company,sys_type,shop_no,merchant_no,ord_amount,fee,cost,order_id1) a 
		group BY day,
case
	when company='哆啦宝' then '哆啦宝'
	when sys_type='哆啦宝' then '哆啦宝-京东'
	when sys_type='乐惠' then '乐惠'
	else '区域' end,
case 
	when sys_type='哆啦宝' then shop_no 
	else merchant_no end 
)t
group BY
date_add(to_date(day),(6-datediff(to_date(day),'2016-01-01')%7)),
day,
cate,
merchant_no1
) a
group by weekday,cate,day

 """,
}


# 以下部分无需改动，除非作业有特殊要求
sql_task = SqlTask()
sql_task.set_sql_runner(sql_runner)
sql_task.set_customized_items(get_customized_items())
return_code = sql_task.execute_sqls(sql_map)
exit(return_code)