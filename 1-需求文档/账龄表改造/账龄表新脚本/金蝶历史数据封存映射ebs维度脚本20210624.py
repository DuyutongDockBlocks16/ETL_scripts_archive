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
     #用于封存金蝶历史凭证数据的脚本，撰写日期：2021年6月5日，仅用于将原账龄表脚本中加工的金蝶凭证明细数据进行封存
    "sql_01": """
    use dmf_ada;
    insert overwrite table dmf_ada.dmfada_adrpt_h_data_sttag_h_age_a_d
    ---此版本改造了客商名称取数的逻辑，原本从原金蝶凭证中取数，现在改为从主数据取数
    select
    voucher.id	                    as id,	--	id
    voucher.vouchernum	            as vouchernum,	--	凭证编号
    voucher.bookeddate	            as bookeddate,	--	凭证日期
    company.ebs_code	            as ebs_company_id,	--	公司fid
    company.ebs_code	            as ebs_company_code,	--	公司编码
    company.company_code            as comname,	--	公司名称
    voucher.accviewgroup	        as accviewgroup,	--	会计科目组合
    accview.ebs_name   	            as accviewlongnum,	--	会计科目
    accview.ebs_code	            as ebs_accview_code,	--	科目编码
    voucher.accviewfdc	            as accviewfdc,	--	科目方向
    voucher.costorgfid	            as costorgfid,	--	成本中心fid
    voucher.costorgname	            as costorgname,	--	成本中心
    voucher.costorgnumber	        as costorgnumber,	--	成本中心编码
    voucher.dbbank	                as dbbank,	--	开户行
    voucher.bankaccount	            as bankaccount,	--	银行账户
    provider.vendor_site_code	    as ebs_provider_id,	--	供应商fid
    provider.vendor_site_code	    as ebs_provider_code,	--	供应商编码
    provider.company_name	        as providername,	--	供应商，20210604修改了取数逻辑
    bizline.ebs_bizline_name        as bizline,	--	业务线
    voucher.projectname	            as projectname,	--	项目
    voucher.invoicetype	            as invoicetype,	--	发票类型
    voucher.entryseq	            as entryseq,	--	行号
    voucher.voucheredesc	        as voucheredesc,	--	分录摘要
    voucher.assentrydesc	        as assentrydesc,	--	辅助账摘要
    voucher.currency	            as currency,	--	币种
    voucher.jforiamt	            as jforiamt,	--	借方金额
    voucher.dforiamt	            as dforiamt,	--	贷方金额
    voucher.jflocalamt	            as jflocalamt,	--	借方金额(本位币)
    voucher.dflocalamt	            as dflocalamt,	--	贷方金额(本位币)
    voucher.sourcesys	            as sourcesys,	--	来源
    voucher.sourcetype	            as sourcetype,	--	类别
    voucher.creatorname	            as creatorname,	--	创建人
    voucher.createtime	            as createtime,	--	创建时间
    voucher.voucherstatus	        as voucherstatus,	--	凭证状态
    voucher.voucherdesc	            as voucherdesc,	--	参考信息
    voucher.attachments	            as attachments,	--	附件张数
    voucher.bizdate	                as bizdate,	--	业务日期
    voucher.lastupdatetime	        as lastupdatetime,	--	最后修改时间
    voucher.vouchertype	            as vouchertype,	--	凭证类型
    bizline.ebs_code	            as ebs_bizline_code,	--	业务线代码
    voucher.xzname	                as xzname,	--	性质
    voucher.accbizlinenum	        as accbizlinenum,	--	科目与业务线代码
    voucher.gencompanyfid	        as gencompanyfid,	--	调整公司fid
    voucher.gencompanynum	        as gencompanynum,	--	调整公司代码
    voucher.gencompanyname	        as gencompanyname,	--	调整公司
    voucher.remark	                as remark,	--	备注
    voucher.userid	                as userid,    --创建人id
    voucher.dt                      as dt --数据日期
    FROM dmf_ada.dmfada_adrpt_fi_h_vouch_sttag_a_d voucher --来自金蝶凭证封存
    left join dmf_add.dmfadd_add_fi_map_comp_eas_ebs_a_d company on voucher.comnum=company.jindie_code
    left join dmf_add.dmfadd_add_fi_map_line_subj_eas_ebs_a_d accview on  voucher.accviewnum=accview.jindie_code
    left join ( select * from odm.odm_fi_fin_subject_merchant_s_d where dt = '{TX_DATE}' and yn_flag='1') provider on  voucher.providernum=provider.merchant_code
    left join dmf_add.dmfadd_add_map_bizline_eas_ebs_a_d bizline on voucher.bizlinenum=bizline.jindie_code
    ;
    """,
}


# 以下部分无需改动，除非作业有特殊要求
sql_task = SqlTask()
sql_task.set_sql_runner(sql_runner)
sql_task.set_customized_items(get_customized_items())
return_code = sql_task.execute_sqls(sql_map)
exit(return_code)