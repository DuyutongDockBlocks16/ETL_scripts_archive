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
    insert overwrite table dmf_tmp.dmcbc_bc_pay_pay_rpt_i_d_1_new_zh
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
  prod_type
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
    
    
    "sql_02": """
    use dmf_tmp;
    insert overwrite table dmf_tmp.dmcbc_bc_pay_pay_rpt_i_d_2_new_zh
    select
    a.*,
    case when a.prod_type='线上-通道业务-POP代付有收入' then '金融科技群-京东生态' else b.hangye end as hangye,
    b.health_type
    from
    dmf_tmp.dmcbc_bc_pay_pay_rpt_i_d_1_new_zh a
    left join dmf_add.dmfadd_add_fi_no_pay_mht_acct_inds_inds_a_d b on a.mer_id = b.mer_id
    """,
    
    
    "sql_03": """
    use dmf_tmp;
    insert overwrite table dmf_tmp.dmcbc_bc_pay_pay_rpt_i_d_3_new_zh
    select
    a.*,
    case when a.mer_id='110247176001' then '京东收银产品' else b.produc_1 end as product_2,----------------会员码产品取个单独标识
    case when a.mer_id='110247176001' then '会员码产品' else b.produc_2 end as product_3,---------------同上
    case
    when a.hangye is not null then a.hangye
    else c.hangye
    end as hangyenew,
    case
    when a.health_type is not null then a.health_type
    else b.health_type
    end as health_type_new
    from
    (
    select
    *,
    concat(
    nvl(trade_name, ''),
    nvl(product_id, ''),
    nvl(pay_tools, ''),
    nvl(mer_big_type, ''),
    nvl(mer_sma_type, ''),
    nvl(prod_type, '')
    ) as glzd
    from dmf_tmp.dmcbc_bc_pay_pay_rpt_i_d_2_new_zh
    ) a
    left join dmf_add.dmfadd_add_fi_prod_pay_inds_msg_a_d b on a.glzd = b.glzd
    left join dmf_add.dmfadd_add_fi_pay_inds_inds_rltv_tab_a_d c on a.glzd = c.glzd 
    """,
    
    
    "sql_04": """
    use dmf_bi;
    insert overwrite table dmf_bi.dmfbi_profits_pay_pay_inds_msg_thd_pty_week_a_d 
    select                       
    da_te,
    trade_name,
    product_id,
    '' as mer_id,
    pay_tools,
    sum(order_amt) as order_amt,
    sum(trade_amt) as trade_amt,
    sum(mer_fee) as mer_fee,
    sum(bank_fee) as bank_fee,
    sum(trade_num) as trade_num,
    mer_big_type,
    mer_sma_type,
    prod_type,
    '' as bz1,
    '' as bz2,
    '' as sz1,
    '' as sz2,
    product_2,
    product_3,
    hangyenew,
    health_type_new,
    row_number () over () as rn
    from dmf_tmp.dmcbc_bc_pay_pay_rpt_i_d_3_new_zh
    where
    da_te >= '2019-01-01'
    group by
    da_te,
    trade_name,
    product_id,
    pay_tools,
    mer_big_type,
    mer_sma_type,
    prod_type,
    product_2,
    product_3,
    hangyenew,
    health_type_new
    """,
    
     "sql_05": """
    use dmf_bi;
    insert overwrite table dmf_bi.dmfbi_profits_pay_mdl_thd_pty_week_a_d   ---应一个不知道是干啥的业务方要求把中间表转入正式表
    select                       
    *
    from dmf_tmp.dmcbc_bc_pay_pay_rpt_i_d_3_new_zh
    """,
    
}


# 以下部分无需改动，除非作业有特殊要求
sql_task = SqlTask()
sql_task.set_sql_runner(sql_runner)
sql_task.set_customized_items(get_customized_items())
return_code = sql_task.execute_sqls(sql_map)
exit(return_code)