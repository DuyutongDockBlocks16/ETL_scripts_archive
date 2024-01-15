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
    print("*************计算账龄数据快照日期为：************")
    print(sql_task.get_tx_date())
    print("*************计算账龄业务日期为：************")
    print(TX_DATA_DATE)
    print("*************基础数据为作业运行时T-1日的数据快照，业务数据为T-1日的上个月月末时点及之前的数据，比如2020-10-07，计算的业务数据为2020-09-30的账龄************")
    return locals()


sql_map={
     # ATTENTION:   ！！！！ sql_01  因为系统按字典顺序进行排序，小于10 的一定要写成0加编号，否则会顺序混乱，数据出问题，切记，切记！！！！
    "sql_01": """
	use dmf_tmp;
    alter table dmf_tmp.tmp_cwmart_month_evidence_detail_s drop if exists partition(dt = '{TX_DATA_DATE}')
	;
    """,
     
     "sql_02": """
    use dmf_tmp;---抽所有集市业务的月大凭证，最后unionall起来
    insert into dmf_tmp.tmp_cwmart_month_evidence_detail_s partition(dt = '{TX_DATA_DATE}')
    
    select
    *
    from odm.odm_fi_tz_dm_gj_general_g_journal_voucher_sync_000004_month_i_i_d 
    where kd_voucher_status ='SUCCESS'---取EBS回传状态为真的数据 
    and status='0'---只取有效的凭证    
    and manual_dt='{TXPREMONTH}'--只取记账时间为上月的数据
    ;
    """,
    
    "sql_03": """
    use dmf_tmp;
    alter table dmf_tmp.tmp_cwmart_month_evidence_personmerchant_detail_s drop if exists partition(dt = '{TX_DATA_DATE}')
    ;
    """,
        
     "sql_04": """
    use dmf_tmp;---抽所有集市业务的月大凭证，最后unionall起来
    insert into dmf_tmp.tmp_cwmart_month_evidence_personmerchant_detail_s partition(dt = '{TX_DATA_DATE}')
        select 
        trim(voucher.ebs_company_code)      as ebs_company_code,  --EBS公司编码
        trim(voucher.subject_no)            as subject_no   ,     --EBS科目编码      
        case when voucher.aux_subject_1_type='3' then trim(aux_subject_1)
             when voucher.aux_subject_2_type='3' then trim(aux_subject_2)
             when voucher.aux_subject_3_type='3' then trim(aux_subject_3)
        else '' end                         as merchant_id   , --客商id
        trim(voucher.jdfin_code)            as ebs_biz_code  , --EBS业务线编码（虽然叫金蝶code）
        case when voucher.aux_subject_1_type='4' then trim(aux_subject_1)
             when voucher.aux_subject_2_type='4' then trim(aux_subject_2)
             when voucher.aux_subject_3_type='4' then trim(aux_subject_3)
        else '' end                         as ebs_proj_code , ---可能出现在任何一个辅助核算中，用此方法循环查找
        voucher.direction_em                as direction_em,   --借贷方向 0借1贷
        case 
        when direction_em='0' and positive_or_negative='0' then abs(voucher.trans_amt)
        when direction_em='0' and positive_or_negative='1' then 0.0-abs(voucher.trans_amt)
        when direction_em='0' and positive_or_negative='2' then voucher.trans_amt   
        when direction_em='0' and positive_or_negative='3' then 0.0-voucher.trans_amt        
        else 0.0     end                    as jf_amt,       --借方金额
        case 
        when direction_em='1' and positive_or_negative='0' then abs(voucher.trans_amt)
        when direction_em='1' and positive_or_negative='1' then 0.0-abs(voucher.trans_amt)
        when direction_em='1' and positive_or_negative='2' then voucher.trans_amt   
        when direction_em='1' and positive_or_negative='3' then 0.0-voucher.trans_amt 
        else 0.0    end                     as df_amt,       --贷方金额
        voucher.manual_dt                   as manual_dt,    --记账时间
        voucher.currency                    as currency      --币种
        from 
        (select * from dmf_tmp.tmp_cwmart_month_evidence_detail_s where dt = '{TX_DATA_DATE}') voucher--算那天要哪天
        inner join 
        (
        select distinct vendor_site_code,dt from
            odm.odm_fi_fin_subject_merchant_s_d 
            where 1=1
            and yn_flag='1' 
            and company_name = '个人（汇总）'--只要为个人汇总客商的数据
        ) merchant
        on 
        voucher.dt=merchant.dt and voucher.ebs_vendor_side_code=merchant.vendor_site_code ---主数据可能会修改个人汇总的客商编码 
    ;
    """,    
    
    "sql_05": """
    use dmf_tmp;
    alter table dmf_tmp.tmp_cwmart_month_evidence_personmerchant_detail_002_s drop if exists partition(dt = '{TX_DATA_DATE}')
    ;
    """,
    
     "sql_06": """
    use dmf_tmp;---抽所有集市业务的月大凭证，最后unionall起来
    insert into dmf_tmp.tmp_cwmart_month_evidence_personmerchant_detail_002_s partition(dt = '{TX_DATA_DATE}')
    select 
    aaa.ebs_company_code        as ebs_company_code,    ---EBS公司编码
    aaa.subject_no              as subject_no,          ---EBS科目编码
    aaa.merchant_id             as merchant_id,         ---客商id
    aaa.ebs_biz_code            as ebs_biz_code,        ---EBS业务线编码
    aaa.ebs_proj_code           as ebs_proj_code,       ---EBS项目编码
    aaa.jf_amt                  as jf_amt,              ---借方金额
    aaa.df_amt                  as df_amt,              ---贷方金额
    aaa.manual_dt               as manual_dt,           ---记账时间(年月格式202106)
    aaa.currency                as currency,            ---币种编码
    curinfo.currency_brief_code as currency_brief_code, ---币种标准名
    mercode.merchant_code       as merchant_code ,      ---客商编码
    trim(bwb.base_currency_code)      as base_currency_code   ---公司本位币
    from dmf_tmp.tmp_cwmart_month_evidence_personmerchant_detail_s aaa   
    left join
    (
    select id,merchant_code from
    odm.odm_fi_fin_subject_merchant_s_d 
    where 1=1
    and yn_flag='1' 
    and dt ='{TX_DATE}' 
    )mercode
    on aaa.merchant_id=mercode.id ---按最新dt客商编码 
    left join 
    (
    select currency_brief_code,jde_currency_code from odm.odm_fi_zj_bd_currency_info_i_d 
    where status='1') curinfo
    on aaa.currency=curinfo.jde_currency_code
    left join --这步还是汇总后再映射好一些
    (
       select
       a.coa_co_code,a.base_currency_code
       from(
       select
       coa_co_code,base_currency_code
       ,row_number() over(partition by coa_co_code order by incremental_date desc) as rn
       from 
       odm.odm_fi_ebs_cux_jdtmas_gl_balances_v_i_d) a --从科目余额表里取出公司编码对应的公司本位币
       where rn=1
       group by coa_co_code,base_currency_code
    )  bwb
    on e.ebs_company_code = bwb.coa_co_code
    ;
    """,   
    
    
    
}

# 以下部分无需改动，除非作业有特殊要求
sql_task = SqlTask()
sql_task.set_sql_runner(sql_runner)
sql_task.set_customized_items(get_customized_items())
return_code = sql_task.execute_sqls(sql_map)
exit(return_code)