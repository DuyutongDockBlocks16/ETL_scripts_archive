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
set mapred.job.name=job_sdm_zjxt_zfyw_jjsdm_transaction_i_01;
set mapred.max.split.size=64000000;
set hive.stats.column.autogather=false;
use dmf_gj;

alter table dmf_gj.dmfgj_gj_pay_fi_hs_transaction_detail_i_d drop partition(dt = '{TX_DATE}' ,data_source_em='a07_zfyw_jjsdm');
alter table dmf_gj.dmfgj_gj_pay_fi_hs_transaction_detail_i_d add partition(dt = '{TX_DATE}' ,data_source_em='a07_zfyw_jjsdm');
""",

"sql_02": """
set mapred.job.name=job_sdm_zjxt_zfyw_jjsdm_transaction_i_02;
set mapred.max.split.size=64000000;
set hive.stats.column.autogather=false;
use dmf_tmp;

drop table dmf_tmp.tmp_sdm_zjxt_zfyw_jjsdm_sdm_gj_transaction_i;
create table dmf_tmp.tmp_sdm_zjxt_zfyw_jjsdm_sdm_gj_transaction_i
as
select
CONCAT_WS('_', new_source_table, 'a07_zfyw_jjsdm', trans_type, writeoff_status,joinkey,'{TX_DATE}') AS origin_id,
'TNR001' as serial_no,
dmf_bc.dmdictdesc('BUSI_TYPE_3_1','303500','{TXDATE}') as biz_type,
dmf_bc.dmdictdesc('BUSI_TYPE_3_2','303500','{TXDATE}') as biz_line,
'303500' as product_no,
trans_type,
'1002500' as company_no1,
'' as company_no2,
'' as company_no3,
'' as company_no4,
'1498621' as company_no5, --客商-无
case when dmf_bc.dmdictdesc('zjxt_zfyw_jjsdm_trans_type',bankcode,batchid,txtype) in ('0','2') then bankcode else '' end as borrow_bank_acct,
case when dmf_bc.dmdictdesc('zjxt_zfyw_jjsdm_trans_type',bankcode,batchid,txtype) in ('1','3') then bankcode else '' end as loan_bank_acct,
tradedate as trans_dt,
txamount as trans_amt,
banksn as trans_no,
'' as pay_no,
'' as loan_no,
'' as plan_no,
'' as customer_no,
'' as merchant_no,
'' as section_no,
'' as pay_enum,
case when txtype='-1' then '0'  --支出 借方
     when txtype='1' then '1'   --收入 贷方
     else ''
end as direction,
'' as loan_type,
create_dt as create_dt,
orderid as order_no,
'' as project_no,
'' as spare_no,
'' as vir_merchant,
'' as company_no6,
'' as company_no7,
'' as company_no8,
'' as company_no9,
'' as currency,
'' as has_tax,
'' as tax
,new_source_table     as source_table    --'来源表'       add by xiaojia 20210305 新增字段
,source_table_dt                         --'来源表分区（冲销数据需获取被冲销记录对应分区）'  add by xiaojia 20210305 新增字段
,cast(null as string) as repay_no        --'还款单号',    add by xiaojia 20210305 新增字段
,cast(null as string) as refund_no       --'退款单号',    add by xiaojia 20210305 新增字段
,cast(null as string) as bill_no         --'账单号',      add by xiaojia 20210305 新增字段
,cast(null as string) as sett_id         --'结算单号',    add by xiaojia 20210305 新增字段
,cast(null as string) as fee_id          --'费用单号',    add by xiaojia 20210305 新增字段
,cast(null as string) as fee_type        --'费用类型',    add by xiaojia 20210305 新增字段
,cast(null as string) as sett_scenes     --'结算场景',    add by xiaojia 20210305 新增字段
,cast(null as string) as sett_biz_type   --'业务类型',    add by xiaojia 20210305 新增字段
,writeoff_status                         --'冲销标识 NORMAL-正常,WRITEOFF-被冲销'
,cast(null as string) as product_id      -- '产品id',     add by xiaojia 20210305 新增字段
,cast(null as string) as sku_id          -- '品类id'      add by xiaojia 20210305 新增字段
from (
    select *
        ,'dmfbc_gj_pay_jjsdm_transaction_i_d' as new_source_table
        ,dmf_bc.dmdictdesc('zjxt_zfyw_jjsdm_trans_type',bankcode,batchid,txtype)  as trans_type
        , case when data_type = '1' then 'NORMAL' else 'WRITEOFF' end as writeoff_status
from dmf_bc.dmfbc_gj_pay_jjsdm_transaction_i_d a1
where  dt = '{TX_DATE}'
  and  valid = '1') x  --0 无效 1 有效
  ;
""",

"sql_03": """
set mapred.job.name=job_sdm_zjxt_zfyw_jjsdm_transaction_i_03;
set mapred.max.split.size=64000000;
set hive.stats.column.autogather=false;
use dmf_gj;

INSERT OVERWRITE TABLE dmf_gj.dmfgj_gj_pay_fi_hs_transaction_detail_i_d PARTITION (dt = '{TX_DATE}' ,data_source_em='a07_zfyw_jjsdm')
SELECT
origin_id,
upper(concat_ws('_',serial_no, '{TX_DATE}', 'a07_zfyw_jjsdm', default.getmd5(origin_id))) as serial_no,
biz_type,
biz_line,
product_no,
trans_type,
company_no1,
company_no2,
company_no3,
company_no4,
company_no5,
borrow_bank_acct,
loan_bank_acct,
trans_dt,
trans_amt,
trans_no,
pay_no,
loan_no,
plan_no,
customer_no,
merchant_no,
section_no,
pay_enum,
direction,
loan_type,
create_dt,
order_no,
project_no,
spare_no,
vir_merchant,
company_no6,
company_no7,
company_no8,
company_no9,
currency,
has_tax,
tax
,source_table    --'来源表'       add by xiaojia 20210114 新增字段
,source_table_dt --'来源表分区（冲销数据需获取被冲销记录对应分区）'  add by xiaojia 20210114 新增字段
,repay_no        --'还款单号',    add by xiaojia 20210114 新增字段
,refund_no       --'退款单号',    add by xiaojia 20210114 新增字段
,bill_no         --'账单号',      add by xiaojia 20210114 新增字段
,sett_id         --'结算单号',    add by xiaojia 20210114 新增字段
,fee_id          --'费用单号',    add by xiaojia 20210114 新增字段
,fee_type        --'费用类型',    add by xiaojia 20210114 新增字段
,sett_scenes     --'结算场景',    add by xiaojia 20210114 新增字段
,sett_biz_type   --'业务类型',    add by xiaojia 20210114 新增字段
,writeoff_status --'冲销标识 NORMAL-正常,WRITEOFF-被冲销'
,product_id      -- '产品id',     add by xiaojia 20210304 新增字段
,sku_id          -- '品类id'      add by xiaojia 20210304 新增字段
FROM dmf_tmp.tmp_sdm_zjxt_zfyw_jjsdm_sdm_gj_transaction_i
WHERE nvl(trans_amt,0) != 0
  AND nvl(trans_type,'')  !=  ''
;
""",

}
# 以下部分无需改动，除非作业有特殊要求
sql_task = SqlTask()
sql_task.set_sql_runner(sql_runner)
sql_task.set_customized_items(get_customized_items())
return_code = sql_task.execute_sqls(sql_map)
exit(return_code)