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
     # 建表，表结构保持与月大凭证相同
    
    "sql_12": """
    use dmf_tmp;
    alter table dmf_rpt.dmfrpt_eas_sf_pers_subj_bal_tab_amt_i_m drop if exists partition(dt = '2021-05-31')
    ;
    """,    
    
    "sql_13": """
    use dmf_tmp;
    insert into table dmf_rpt.dmfrpt_eas_sf_pers_subj_bal_tab_amt_i_m partition(dt = '2021-05-31')
    select 
    '202105' as period_name,
    company.ebs_code as company_code,
    '' as company_name,
    curr.currency_brief_code as base_currency_code,
    currtwo.currency_brief_code as currency,
    accview.ebs_code as subject_code,
    '' as subject_name,
    balance.supplierNum as merchant_code,
    '' as merchant_name,
    bizline.ebs_code as bizline_code,
    '' as bizline_name,
    proj.ebs_pro_code as project_code,
    '' as project_name,
    0.0 as begin_balance,
    0.0 as begin_balance_beq,
    0.0 as period_net_dr,
    0.0 as period_net_cr,
    0.0 as period_net_dr_beq,
    0.0 as period_net_cr_beq,
    balance.fyeardebitfor as year_sum_dr,
    balance.fyearcreditfor as year_sum_cr,
    balance.fyeardebitlocal as year_sum_dr_beq,
    balance.fyearcreditlocal as year_sum_cr_beq,
    balance.endbalancefor as end_balance,
    balance.endbalance as end_balance_beq,
    provider.id
    ------
    from(
    select
        concat(bal.periodnumber/100) as periodnumber,
        bal.comNum as comNum,
        bal.company as company,
        bal.bcName as bcName,
        bal.cyName as cyName,
        bal.acctnum as acctnum,
        bal.accName as accName,
        bal.assnum as assnum,
        bal.supplier as supplier,
        bal.fyeardebitfor as fyeardebitfor,
        bal.fyearcreditfor as fyearcreditfor,
        bal.fyeardebitlocal as fyeardebitlocal,
        bal.fyearcreditlocal as fyearcreditlocal,
        substring_index(substring_index(bal.assnum,'_',-4),'_',1) as supplierNum,--客商编码
        bal.linename as linename,
        substring_index(substring_index(bal.assnum,'_',-3),'_',1) as lineNum,--业务线编码
        bal.xmname as xmname,
        substring_index(bal.assnum,'_',-1) as xmNumber,--项目编码
        bal.endbalance as endbalance,
        bal.endbalancefor as endbalancefor
    from
    DMF_BC.DMFBC_EAS_JD_GL_BALANCES_S_D bal
        where 1=1
        and periodnumber=202105
        and dt='2021-06-15'
        and cyname !='(综合本位币)'
    ) balance
    left join dmf_add.dmfadd_add_fi_map_comp_eas_ebs_a_d company on balance.comNum=company.jindie_code
    left join dmf_add.dmfadd_add_fi_map_line_subj_eas_ebs_a_d accview on  balance.acctnum=accview.jindie_code
    inner join ( select * from odm.odm_fi_fin_subject_merchant_s_d 
            where dt = '{TX_DATE}' 
            and 
            ((yn_flag='1' 
            and vendor_site_code='A041667820001') or (merchant_code='KS000669'))) provider on  balance.supplierNum=provider.merchant_code
    left join dmf_add.dmfadd_add_map_bizline_eas_ebs_a_d bizline on balance.lineNum=bizline.jindie_code
    left join dmf_add.dmfadd_add_eas_ebs_proj_map_a_d proj on balance.xmNumber=proj.jindie_pro_code
    left join (select * from odm.odm_fi_zj_bd_currency_info_i_d where jde_currency_code like 'BB%' ) curr on balance.bcName =  curr.currency_name
    left join (select * from odm.odm_fi_zj_bd_currency_info_i_d where jde_currency_code like 'BB%' ) currtwo on balance.cyName =  currtwo.currency_name
    ;
    """,    


}


    

# 以下部分无需改动，除非作业有特殊要求
sql_task = SqlTask()
sql_task.set_sql_runner(sql_runner)
sql_task.set_customized_items(get_customized_items())
return_code = sql_task.execute_sqls(sql_map)
exit(return_code)