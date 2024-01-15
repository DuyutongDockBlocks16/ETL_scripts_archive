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
    alter table dmf_rpt.dmfrpt_eas_sf_supp_vendor_plus_i_d drop if exists partition(dt = '{TX_DATE}')
    ;
    """,    
    
    "sql_13": """
    use dmf_tmp;
    insert into table dmf_rpt.dmfrpt_eas_sf_supp_vendor_plus_i_d partition(dt = '{TX_DATE}')
    select  
    etl_dt	,
    rt_meta	,
    id	,
    merchant_code	,
    company_name	,
    company_type	,
    license_type	,
    license_number	,
    license_file	,
    organization_code_file	,
    company_country	,
    company_address	,
    legal_representative	,
    registered_capital_currency	,
    registered_capital	,
    company_begin_date	,
    company_expiry_date	,
    run_business	,
    superior_company_code	,
    taxpayer_type	,
    taxpayer_qualification_file	,
    taxpayer_begin_time	,
    taxpayer_identity	,
    taxpayer_reg_certificate_file	,
    opening_bank	,
    account_number	,
    account_number_tm	,
    account_number_join	,
    opening_permit_file	,
    contacts	,
    contacts_tm	,
    contacts_join	,
    contacts_tel	,
    contacts_tel_tm	,
    contacts_tel_join	,
    contacts_mail	,
    contacts_mail_tm	,
    contacts_mail_join	,
    remarks	,
    creator	,
    create_date	,
    editor	,
    modified_date	,
    status	,
    audit_status	,
    approved_by	,
    examine_by_app	,
    approved_date	,
    is_certification	,
    source	,
    yn_flag	,
    license_number_aks	,
    contacts_aks	,
    contacts_tel_aks	,
    account_number_aks	,
    company_name_aks	,
    extend_param	,
    old_names	,
    related_company_type	,
    push_status	,
    vendor_site_code	,
    erp	,
    ou
    from
    odm.odm_fi_fin_subject_merchant_s_d
    where dt = '{TX_DATE}'
    ;
    """,    


}


    

# 以下部分无需改动，除非作业有特殊要求
sql_task = SqlTask()
sql_task.set_sql_runner(sql_runner)
sql_task.set_customized_items(get_customized_items())
return_code = sql_task.execute_sqls(sql_map)
exit(return_code)