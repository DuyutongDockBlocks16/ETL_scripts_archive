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
    insert OVERWRITE table dmf_bc.dmfbc_mrpt_pay_ebs_fi_push_to_sk_i_d PARTITION (dt='{TX_DATE}')
    select 
	'1',---数据来源
	'网银在线（北京）科技有限公司',--公司名称
	'8106',--公司编码
	subject_manage_code,--管理科目编码
	subject_manage_name,--管理科目名称
	business_manage_code,--预算（管理）业务线编码
	business_manage_name,--预算（管理）业务线名称
	'0001-0002-0001-0001-0001-0001',--预算部门编码
	'支付事业部',--预算部门名称
	'',
    '',
    '',
	bizline.ebs_code,---核算业务线编码（格式：YWX.992.08.06）这里用线下补录表做了映射
	bizline.ebs_bizline_name,---核算业务线名称（格式：YWX.992.08.06）这里用线下补录表做了映射
	'',----department_account_code对的这个字段是空的
    '',
	3,
    '',
    '',
    '',
    push.manage_report_amount,
    '',
    '',
    '',
    push.voucher_date,
    '',
    '',
    '',
    '',
	provider.vendor_site_code ,---供应商编码用主数据的映射
    provider.company_name,---供应商名称用主数据的映射
    '',
    '',
    '',
    '',
    '',
    push.entry_mark,
    push.supple_mark,
    'CNY',
    '人民币元',
    '',
    '',
    '',
    '',
    '',
    '',
    '',
    '',
    push.business_date,
    push.voucher_type_code,
    push.voucher_type_name,
    '',
    '',
    '',
    '',
    '',
    '{TX_DATE}',
    default.getmd5('1','8106','X0000019',push.subject_manage_code,push.business_manage_code,push.business_manage_name,push.business_date,'{TX_DATE}',push.supplier_name,push.voucher_type_code) as uuid,
    FROM_UNIXTIME(UNIX_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss'),
    'cwmart',
    FROM_UNIXTIME(UNIX_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss'),
    '',
    0,
    cast(substr(voucher_date,1,6) as int)
	from
	dmf_bc.dmfbc_mrpt_pay_fi_push_to_sk_i_d push
    left join ( select * from odm.odm_fi_fin_subject_merchant_s_d where dt = '{TX_DATE}' and yn_flag='1') provider on  push.supplier_code=provider.merchant_code
	left join dmf_add.dmfadd_add_map_bizline_eas_ebs_a_d bizline on replace(push.business_account_code,'YWX.','YWX-')=bizline.jindie_code
	where push.dt='{TX_DATE}'
    """,
}


# 以下部分无需改动，除非作业有特殊要求
sql_task = SqlTask()
sql_task.set_sql_runner(sql_runner)
sql_task.set_customized_items(get_customized_items())
return_code = sql_task.execute_sqls(sql_map)
exit(return_code)