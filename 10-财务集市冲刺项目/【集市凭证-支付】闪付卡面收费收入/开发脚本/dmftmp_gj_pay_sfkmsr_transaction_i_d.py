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
set mapred.job.name=job_gdm_zfyw_sfkmsr_transaction_i_01;
set mapred.max.split.size=64000000;
set hive.stats.column.autogather=false;
use dmf_tmp;

drop TABLE if exists dmf_tmp.dmftmp_gj_pay_sfkmsr_transaction_i_d;
create TABLE if not exists dmf_tmp.dmftmp_gj_pay_sfkmsr_transaction_i_d as
SELECT     '0' AS logic_delete_flag
          ,'0' AS mq_delete_flag
          ,id                         --唯一键
          ,banksn                     --流水号
          ,orderid                    --订单号
          ,String(bankcode)           --银行账户
          ,batchid                    --资金帐分类
          ,String(txtype)             --借贷方向
          ,txamount                   --交易金额
          ,txbalance                  --余额
          ,balance                    --每笔收支后的余额
          ,substr(nvl(tradedate,''),1,19) as tradedate --交易时间 银行交易日期 处理时间带.0的问题
          ,txtime                     --交易时间
          ,String(currencytype)       --币种
          ,String(reftype)            --业务类型
          ,String(checkstatus)        --对账状态：0-未对账 1-已达 2-未达
          ,String(isinit)	            --是否是期初录入：0-不是 1-是
          ,String(trade_type)         --交易类型
          ,trade_code                 --交易代码
          ,valid                      --有效标志
		  ,'odm_fi_zjz_fss_balance_bank_account_20190701_i_d' as source_table
FROM  odm.odm_fi_zjz_fss_balance_bank_account_20190701_i_d
WHERE  dt = '{TX_DATE}'
       AND bankcode='6852'
       AND txtype ='1'
       AND trade_type in ('1','30')
       AND valid = 1
  ;
""",

"sql_02": """
set mapred.job.name=job_gdm_zfyw_sfkmsr_transaction_i_02;
set mapred.max.split.size=64000000;
set hive.stats.column.autogather=false;
use dmf_tmp;

DROP TABLE IF EXISTS dmf_tmp.dmftmp_gj_pay_sfkmsr_transaction_i_d_01;
CREATE TABLE IF NOT EXISTS dmf_tmp.dmftmp_gj_pay_sfkmsr_transaction_i_d_01
AS
--当日增量临时流水
SELECT     dmf_bc.getmd5(t.id) AS joinkey
          ,dmf_bc.getmd5(logic_delete_flag
                         ,mq_delete_flag
                         ,id
                         ,banksn
                         ,orderid
                         ,bankcode
                         ,batchid
                         ,txtype
                         ,txamount
                         ,txbalance
                         ,balance
                         ,tradedate
                         ,txtime
                         ,currencytype
                         ,reftype
                         ,checkstatus
                         ,isinit
                         ,trade_type
                         ,trade_code
                         ,valid
                         ) AS change_code
          ,logic_delete_flag
          ,mq_delete_flag
          ,id
          ,banksn
          ,orderid
          ,bankcode
          ,batchid
          ,txtype
          ,txamount
          ,txbalance
          ,balance
          ,tradedate
          ,txtime
          ,currencytype
          ,reftype
          ,checkstatus
          ,isinit
          ,trade_type
          ,trade_code
          ,valid
		  ,source_table
FROM  dmf_tmp.dmftmp_gj_pay_sfkmsr_transaction_i_d t
;
""",

"sql_03": """
set mapred.job.name=job_gdm_zfyw_sfkmsr_transaction_i_03;
set mapred.max.split.size=64000000;
set hive.stats.column.autogather=false;
use dmf_tmp;

DROP TABLE IF EXISTS dmf_tmp.tmp_his_zfyw_sfkmsr_transaction_i;
CREATE TABLE IF NOT EXISTS dmf_tmp.tmp_his_zfyw_sfkmsr_transaction_i
AS
--历史流水全量有效数据
SELECT * FROM (
  SELECT     joinkey
            ,dmf_bc.getmd5(logic_delete_flag
                         ,mq_delete_flag
                         ,id
                         ,banksn
                         ,orderid
                         ,bankcode
                         ,batchid
                         ,txtype
                         ,txamount
                         ,txbalance
                         ,balance
                         ,substr(nvl(tradedate,''),1,19)
                         ,txtime
                         ,currencytype
                         ,reftype
                         ,checkstatus
                         ,isinit
                         ,trade_type
                         ,trade_code
                         ,valid
                         ) AS change_code --重新生成change_code，为了处理tradedate带.0的问题
            ,logic_delete_flag
            ,mq_delete_flag
            ,id
            ,banksn
            ,orderid
            ,bankcode
            ,batchid
            ,txtype
            ,txamount
            ,txbalance
            ,balance
            ,substr(nvl(tradedate,''),1,19) as tradedate --处理时间带.0的问题
            ,txtime
            ,currencytype
            ,reftype
            ,checkstatus
            ,isinit
            ,trade_type
            ,trade_code
            ,create_dt
            ,data_source_em
            ,valid
            ,dt
            ,data_type
            ,ROW_NUMBER() OVER(PARTITION BY joinkey ORDER BY dt DESC) AS rn
			,source_table
  FROM  dmf_bc.dmfbc_gj_pay_sfkmsr_transaction_i_d
  WHERE dt < '{TX_DATE}'  AND data_type = 1
) a
WHERE rn = 1
;
""",

"sql_04": """
set mapred.job.name=job_gdm_zfyw_sfkmsr_transaction_i_04;
set mapred.max.split.size=64000000;
set hive.stats.column.autogather=false;
use dmf_bc;

ALTER TABLE dmf_bc.dmfbc_gj_pay_sfkmsr_transaction_i_d DROP PARTITION (dt = '{TX_DATE}' ,data_type = 1);
ALTER TABLE dmf_bc.dmfbc_gj_pay_sfkmsr_transaction_i_d DROP PARTITION (dt = '{TX_DATE}' ,data_type = 2);
ALTER TABLE dmf_bc.dmfbc_gj_pay_sfkmsr_transaction_i_d ADD PARTITION (dt = '{TX_DATE}' ,data_type = 1);
ALTER TABLE dmf_bc.dmfbc_gj_pay_sfkmsr_transaction_i_d ADD PARTITION (dt = '{TX_DATE}' ,data_type = 2);
""",

"sql_05": """
set mapred.job.name=job_gdm_zfyw_sfkmsr_transaction_i_05;
set mapred.max.split.size=64000000;
set hive.stats.column.autogather=false;
use dmf_bc;

INSERT OVERWRITE TABLE dmf_bc.dmfbc_gj_pay_sfkmsr_transaction_i_d PARTITION (dt = '{TX_DATE}' ,data_type = 1)
--比较T-1日增量流水与T-2日之前历史流水，若数据发生变化则以T-1日报表数据为准
SELECT     C.joinkey
          ,C.change_code
          ,C.logic_delete_flag
          ,C.mq_delete_flag
          ,C.id
          ,C.banksn
          ,C.orderid
          ,C.bankcode
          ,C.batchid
          ,C.txtype
          ,C.txamount
          ,C.txbalance
          ,C.balance
          ,C.tradedate
          ,C.txtime
          ,C.currencytype
          ,C.reftype
          ,C.checkstatus
          ,C.isinit
          ,C.trade_type
          ,C.trade_code
          ,FROM_UNIXTIME(UNIX_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss') AS create_dt
          ,'a07' AS data_source_em
          ,C.valid
		  ,'{TX_DATE}' AS source_table_dt
          ,C.source_table
FROM  dmf_tmp.tmp_his_zfyw_sfkmsr_transaction_i H
FULL OUTER JOIN  dmf_tmp.dmftmp_gj_pay_sfkmsr_transaction_i_d_01 C
ON  H.joinkey = C.joinkey
WHERE (H.change_code <> C.change_code) OR (H.joinkey IS NULL);
""",

"sql_06": """
set mapred.job.name=job_gdm_zfyw_sfkmsr_transaction_i_06;
set mapred.max.split.size=64000000;
set hive.stats.column.autogather=false;
use dmf_bc;

INSERT OVERWRITE TABLE dmf_bc.dmfbc_gj_pay_sfkmsr_transaction_i_d PARTITION (dt = '{TX_DATE}' ,data_type = 2)
--比较T-1日增量流水与T-2日之前历史流水，若数据发生变化则冲销T-2日之前最新记录
SELECT     H.joinkey
          ,H.change_code
          ,H.logic_delete_flag
          ,H.mq_delete_flag
          ,H.id
          ,H.banksn
          ,H.orderid
          ,H.bankcode
          ,H.batchid
          ,H.txtype
          ,-H.txamount
          ,H.txbalance
          ,H.balance
          ,H.tradedate
          ,H.txtime
          ,H.currencytype
          ,H.reftype
          ,H.checkstatus
          ,H.isinit
          ,H.trade_type
          ,H.trade_code
          ,FROM_UNIXTIME(UNIX_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss') AS create_dt
          ,'a07' AS data_source_em
          ,H.valid
		  ,H.dt AS source_table_dt
          ,H.source_table
FROM  dmf_tmp.tmp_his_zfyw_sfkmsr_transaction_i H
FULL OUTER JOIN  dmf_tmp.dmftmp_gj_pay_sfkmsr_transaction_i_d_01 C
ON  H.joinkey = C.joinkey
WHERE H.change_code <> C.change_code;
""",

}
# 以下部分无需改动，除非作业有特殊要求
sql_task = SqlTask()
sql_task.set_sql_runner(sql_runner)
sql_task.set_customized_items(get_customized_items())
return_code = sql_task.execute_sqls(sql_map)
exit(return_code)