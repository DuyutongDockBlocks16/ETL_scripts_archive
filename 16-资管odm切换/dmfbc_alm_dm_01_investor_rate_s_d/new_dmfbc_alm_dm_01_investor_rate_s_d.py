# !/usr/bin/env python
# -*- coding: utf-8 -*-

from template.base_sql_task import *


#
# 目前支持：RUNNER_SPARK_SQL和RUNNER_HIVE
#  Creater        :zlc
#  Creation Time  :2019-02-04
#  Description    :dmfbc_oar_bc_cash_loan_syl_info 资管系统资方利率表
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
     insert overwrite table dmf_bc.dmfbc_alm_dm_01_investor_rate_s_d  partition (dt='{TX_DATE}')
     select distinct         
                             from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss' ) AS etl_dt
                             ,investor_id
                             ,Investor_type
                             ,case cooperation when '1' then '按比分成' 
                                   when '2' then '固定收益' 
                                   when '3' then '按日计息' 
                                   when '4' then '贷款余额固收' 
                                   when '5' then '风险卖断' 
                                   else '其他' end as cooperation   --合作方式
                             ,case when dmf_bc.dmdictdesc('zgs_plat_type',t1.plat_id)=1 then '大白'  --映射值1代表平台属于大白，映射值2代表平台属于小白,3代表平台属于金条，4代表平台属于京农贷
                                   when dmf_bc.dmdictdesc('zgs_plat_type',t1.plat_id)=2 then '小白'
                                   when dmf_bc.dmdictdesc('zgs_plat_type',t1.plat_id)=3 then '金条'
                                   when dmf_bc.dmdictdesc('zgs_plat_type',t1.plat_id)=4 then '京农贷'
                                   else '其他' end as business_plat_line
                             ,get_json_object(config,'$.cooperationVo.fixedRate') as fixed_income_rate  --固定收益率 r1
                             ,get_json_object(config,'$.cooperationVo.ratioRate') as ratio_rate    --按笔分成比例
                             ,get_json_object(extra_config_json,'$.profitModelConfig.riskSellingFixedRate') as risk_selling_fixed_rate --固收利率
     from
     (select * from 
     ----odm.ODM_FI_PLAT_INVESTOR_S_D 
     odm.ODM_AM_ABS_PLAT_INVESTOR_S_D
     where dt = '{TX_DATE}') t1
     join
     (select * from 
     -----odm.ODM_FI_LEDGER_CONFIG_SOLUTION_S_D 
     odm.ODM_AM_ABS_LEDGER_CONFIG_SOLUTION_S_D 
     where dt ='{TX_DATE}') t2
     on t1.investor_id =t2.inveStor_alias
     ;
    """,

}


# 以下部分无需改动，除非作业有特殊要求
sql_task = SqlTask()
sql_task.set_sql_runner(sql_runner)
sql_task.set_customized_items(get_customized_items())
return_code = sql_task.execute_sqls(sql_map)
exit(return_code) 