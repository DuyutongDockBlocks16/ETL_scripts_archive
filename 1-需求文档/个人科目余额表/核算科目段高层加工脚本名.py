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
    "sql_12": """
    use dmf_tmp;
    alter table dmf_rpt.dmfrpt_eas_subj_accti_machg_plus_spst_s_d drop if exists partition(dt = '{TX_DATE}')
    ;
    """,    
    
    "sql_13": """
    use dmf_tmp;
    insert into table dmf_rpt.dmfrpt_eas_subj_accti_machg_plus_spst_s_d partition(dt = '{TX_DATE}')
    select  
    etl_dt	,
rt_meta	,
id	,
code	,
name	,
account_type	,
accrual_direction	,
parent_code	,
is_leaf	,
status	,
source_created_time	,
source_creator	,
source_modified_time	,
source_modifier	,
creator	,
created_time	,
modifier	,
modified_time	,
deleted	,
start_dt1	,
end_dt	,
source_id	,
accrualdirection	,
ac_flg	,
acnotice_flg	,
asst_id	,
asst_number	,
asst_name	,
cfd_id	,
cfd_number	,
cfd_name	,
cfc_id	,
cfc_number	,
cfc_name	,
is_changecurrency	,
report_project_rl	,
source_start_date	,
source_end_date	,
need_vendor_flag	
    from
    odm.odm_fi_mdm_ebs_subject_new_i_d
    ;
    """,    


}


    

# 以下部分无需改动，除非作业有特殊要求
sql_task = SqlTask()
sql_task.set_sql_runner(sql_runner)
sql_task.set_customized_items(get_customized_items())
return_code = sql_task.execute_sqls(sql_map)
exit(return_code)