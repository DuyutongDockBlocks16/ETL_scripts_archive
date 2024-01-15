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
    drop table if exists dmf_tmp.pay_thd_pty_sum_001_dyt;
    """,
    
     "sql_02": """
    use dmf_tmp;
    create table if not exists dmf_tmp.pay_thd_pty_sum_001_dyt
    as 
    
    select da_te,
    substring(da_te,1,7) yearmonth,
    substring(da_te,1,4) year,
    product_2,
    product_3,
    hangyenew,
    health_type_new,
    bizline_code,
    bizline_name,
    bizdept,
    bz1,---这个字段是渠道/供应链标签
    sum(order_amt) order_amt,
    sum(mer_fee) mer_fee 
    from dmf_bi.dmfbi_profits_pay_pay_inds_msg_thd_pty_week_a_d
    where mer_sma_type <> '牌照收单'---这个需求方不看，来自于张惠
    and product_2 <> '协同产品'---这个需求方不看，来自于张惠
    group by 
    da_te,
    product_2,
    product_3,
    hangyenew,
    health_type_new,
    bizline_code,
    bizline_name,
    bizdept,
    bz1
    
    """,
    
    "sql_03": """
	use dmf_tmp;
    drop table if exists dmf_tmp.pay_thd_pty_sum_002_dyt;
    """,
    
    
    "sql_04": """
    use dmf_tmp;
    create table if not exists dmf_tmp.pay_thd_pty_sum_002_dyt
    as
    
    select---交易额
    da_te,
    product_2,
    product_3,
    hangyenew,
    health_type_new,
    bizline_code,
    bizline_name,
    bizdept,
    bz1,
    '交易额' as index_name,  
    order_amt as index_value,
    sum(order_amt) over(partition by yearmonth,product_2,product_3,hangyenew,health_type_new,bizline_code,bizline_name,bizdept,bz1 order by da_te ) as mtd_index_value,
    sum(order_amt) over(partition by year,product_2,product_3,hangyenew,health_type_new,bizline_code,bizline_name,bizdept,bz1 order by da_te ) as ytd_index_value,
    'JDF.02' as index_code
    from dmf_tmp.pay_thd_pty_sum_001_dyt
    
  
    union all 
  
  
    select---含税收入
    da_te,
    product_2,
    product_3,
    hangyenew,
    health_type_new,
    bizline_code,
    bizline_name,
    bizdept,
    bz1,
    '含税收入' as index_name,
    mer_fee as index_value,
    sum(mer_fee) over(partition by yearmonth,product_2,product_3,hangyenew,health_type_new,bizline_code,bizline_name,bizdept,bz1 order by da_te ) as mtd_index_value,
    sum(mer_fee) over(partition by year,product_2,product_3,hangyenew,health_type_new,bizline_code,bizline_name,bizdept,bz1 order by da_te ) as ytd_index_value,
    'JDF.08' as index_code
    from dmf_tmp.pay_thd_pty_sum_001_dyt
  
  
    union all 

  
    select---不含税收入
    da_te,
    product_2,
    product_3,
    hangyenew,
    health_type_new,
    bizline_code,
    bizline_name,
    bizdept,
    bz1,
    '不含税收入' as index_name,
    mer_fee/1.06 as index_value,
    (sum(mer_fee) over(partition by yearmonth,product_2,product_3,hangyenew,health_type_new,bizline_code,bizline_name,bizdept,bz1 order by da_te ))/1.06 as mtd_index_value,
    (sum(mer_fee) over(partition by year,product_2,product_3,hangyenew,health_type_new,bizline_code,bizline_name,bizdept,bz1 order by da_te ))/1.06 as ytd_index_value,
    'JDF.09' as index_code
    from dmf_tmp.pay_thd_pty_sum_001_dyt
    
    union all 

    select---不含税收入（还原出表）
    da_te,
    product_2,
    product_3,
    hangyenew,
    health_type_new,
    bizline_code,
    bizline_name,
    bizdept,
    bz1,
    '不含税收入（还原出表）' as index_name,
    mer_fee/1.06 as index_value,
    (sum(mer_fee) over(partition by yearmonth,product_2,product_3,hangyenew,health_type_new,bizline_code,bizline_name,bizdept,bz1 order by da_te ))/1.06 as mtd_index_value,
    (sum(mer_fee) over(partition by year,product_2,product_3,hangyenew,health_type_new,bizline_code,bizline_name,bizdept,bz1 order by da_te ))/1.06 as ytd_index_value,
    'JDF.32' as index_code
    from dmf_tmp.pay_thd_pty_sum_001_dyt
    
    
    """,
    
    "sql_05": """
    use dmf_bi;
    insert overwrite table dmf_bi.dmfbi_profits_pay_pay_sum_pay_week_a_d 
    select 
    da_te,
    product_2,
    product_3,
    hangyenew,
    health_type_new,
    index_name,
    index_value,
    mtd_index_value,
    ytd_index_value,
    '',
    '',
    '',
    '',
    bizline_code,
    bizline_name,
    bizdept,
    index_code,
    bz1---这个字段是渠道/供应链标签
    from dmf_tmp.pay_thd_pty_sum_002_dyt 
    """,
    
    "sql_06": """
    use dmf_bi;
    alter table dmf_bi.dmfbi_profits_pay_pay_sum_pay_week_s_d  drop if exists partition(dt = '{TX_DATE}');
    """,
    
    "sql_07": """
    use dmf_bi;
    insert into dmf_bi.dmfbi_profits_pay_pay_sum_pay_week_s_d  partition(dt = '{TX_DATE}')
    select 
    da_te,
    product_2,
    product_3,
    hangyenew,
    health_type_new,
    index_name,
    index_value,
    mtd_index_value,
    ytd_index_value,
    '',
    '',
    '',
    '',
    bizline_code,
    bizline_name,
    bizdept,
    index_code,
    bz1---这个字段是渠道/供应链标签
    from dmf_tmp.pay_thd_pty_sum_002_dyt 
    """,
}


# 以下部分无需改动，除非作业有特殊要求
sql_task = SqlTask()
sql_task.set_sql_runner(sql_runner)
sql_task.set_customized_items(get_customized_items())
return_code = sql_task.execute_sqls(sql_map)
exit(return_code)