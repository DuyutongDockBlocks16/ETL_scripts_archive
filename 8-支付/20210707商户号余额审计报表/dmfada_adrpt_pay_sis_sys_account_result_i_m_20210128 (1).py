# !/usr/bin/env python
# -*- coding: utf-8 -*-

from template.base_sql_task import *


#
# 目前支持：RUNNER_SPARK_SQL和RUNNER_HIVE
#
sql_runner=RUNNER_STINGER
##sql_runner=RUNNER_HIVE
##sql_runner=RUNNER_SPARK_SQL


def get_customized_items():
    """
     if you need some special values in your sql, please define and calculate then here
     to refer it as {YOUR_VAR} in your sql
    """
    today = Time.today()
    TX_PRE_60_DATE = Time.date_sub(date=today, itv=60)
    TX_PRE_365_DATE = Time.date_sub(date=today, itv=365)
    #3个月前的第一日
    TX_PRE_4MONTHFIRSTDATE = Time.date_format(Time.year_month_sub(itv=4)+'01',sep='')
    return locals()


sql_map={

     # ATTENTION:   ！！！！ sql_01  因为系统按字典顺序进行排序，小于10 的一定要写成0加编号，否则会顺序混乱，数据出问题，切记，切记！！！！
    "sql_01": """
use dmf_ada;
INSERT overwrite table dmf_ada.dmfada_adrpt_fi_account_mer_daily_i_m partition (dt = '{TX_DATE}') 
SELECT    '{TX_PREMONTHLASTDATE}' AS trade_date,
          b.owner_no, 
          a.customer_id, 
          a.account_no, 
          substr(a.account_no, 21) AS account_type, 
          c.sub_type_desc, 
          a.balance 
FROM      ( 
            select customer_id,account_no,balance,account_date
              from 
                 (select customer_id,account_no,balance,account_date,row_number() over(partition by id order by modified_date desc) rn
                        from dmf_bc.dmfbc_mix_odm_fi_pay_account_mer_daily_report_000_i_d
                        where dt <= '{TX_DATE}'
                 ) t1
             where rn = 1 and account_date = '{TXPREMONTHLASTDATE}' and balance != '0'
          ) a 
JOIN 
          (select sub_type_desc,account_type  
            from 
               (select sub_type_desc,account_type,row_number() over(partition by id order by modified_date desc) rn
                      from dmf_bc.dmfbc_mix_odm_fi_single_account_type_mapping_i_d
                      where dt <= '{TX_DATE}'
               ) t11
            where rn = 1 and sub_type_desc IN ( '商户基本账户', '商户欠款账户', '商户待清算账户')
          ) c 
ON        substr(a.account_no, 21) = c.account_type 
LEFT JOIN 
          (
            select new_account_no,owner_no  
              from 
                 (select new_account_no,owner_no ,row_number() over(partition by new_account_no order by modified_date desc) rn
                        from dmf_bc.dmfbc_mix_odm_fi_zf_mer_account_info_i_d
                        where dt <= '{TX_DATE}'
                 ) t111
             where rn = 1
          ) b 
ON        a.account_no = b.new_account_no
;
    """,
}


# 以下部分无需改动，除非作业有特殊要求
sql_task = SqlTask()
sql_task.set_sql_runner(sql_runner)
sql_task.set_customized_items(get_customized_items())
return_code = sql_task.execute_sqls(sql_map)
exit(return_code)

