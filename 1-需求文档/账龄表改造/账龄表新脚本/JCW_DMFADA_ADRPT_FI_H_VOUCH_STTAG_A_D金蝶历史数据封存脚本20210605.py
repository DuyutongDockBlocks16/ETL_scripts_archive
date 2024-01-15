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
    insert overwrite table dmf_ada.dmfada_adrpt_fi_h_vouch_sttag_a_d
    select
    id , -- 凭证编号
    vouchernum , -- 凭证日期
    bookeddate , -- 公司fid
    comfid , -- 公司编码
    comnum , -- 公司名称
    comname , -- 会计科目组合
    accviewgroup , -- 会计科目
    accviewlongnum , -- 科目编码
    accviewnum , -- 科目方向
    accviewfdc , -- 成本中心fid
    costorgfid , -- 成本中心
    costorgname , -- 成本中心编码
    costorgnumber , -- 开户行
    dbbank , -- 银行账户
    bankaccount , -- 供应商fid
    providerfid , -- 供应商编码
    providernum , -- 供应商
    providername , -- 业务线
    bizline ,-- 项目
    projectname , -- 发票类型
    invoicetype , -- 行号
    entryseq , -- 分录摘要
    voucheredesc , -- 辅助账摘要
    assentrydesc , -- 币种
    currency , -- 借方金额
    jforiamt , -- 贷方金额
    dforiamt , -- 借方金额(本位币)
    jflocalamt , -- 贷方金额(本位币)
    dflocalamt , -- 来源
    sourcesys , -- 类别
    sourcetype , -- 创建人
    creatorname , -- 创建时间
    createtime , -- 凭证状态
    voucherstatus , -- 参考信息
    voucherdesc , -- 附件张数
    attachments , -- 业务日期
    bizdate , -- 最后修改时间
    lastupdatetime , -- 凭证类型
    vouchertype , -- 业务线代码
    bizlinenum , -- 性质
    xzname , -- 科目与业务线代码
    accbizlinenum , -- 调整公司fid
    gencompanyfid , -- 调整公司代码
    gencompanynum , -- 调整公司
    gencompanyname , -- 备注
    remark , -- 报表创建者id  
    userid , -- 数据日期分区
    dt  
    FROM dmf_tmp.bp_cwaduit_coas_acct_period_evidence_d_s --为快照表，每个分区都保存一个截止到当前dt全量凭证的快照
    WHERE dt in ('2021-05-31','2021-04-30','2021-03-31','2021-02-28','2021-01-31' ) --由于源表分区在2021年1月1日之前的分区混乱，此处只取2021年分区的数据
    ;
    """,
}


# 以下部分无需改动，除非作业有特殊要求
sql_task = SqlTask()
sql_task.set_sql_runner(sql_runner)
sql_task.set_customized_items(get_customized_items())
return_code = sql_task.execute_sqls(sql_map)
exit(return_code)