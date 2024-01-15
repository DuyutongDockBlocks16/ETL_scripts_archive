# !/usr/bin/env python
# -*- coding: utf-8 -*-

from template.base_sql_task import *


#目前支持：RUNNER_SPARK_SQL和RUNNER_HIVE
#Creater:wanglixin16
#Creation Time:20200331
#Description:dmfalm_alm_rpt_01_group_pss_acct_new_s_d  集团占款报表(新)
#不包含收入



sql_runner=RUNNER_HIVE


def get_customized_items():
    """
     if you need some special values in your sql, please define and calculate then here
     to refer it as {YOUR_VAR} in your sql
    """
    today = Time.today()
    TX_PRE_40_DATE = Time.date_sub(date=today, itv=40)
    TX_PRE_39_DATE = Time.date_sub(date=today, itv=39)
    TX_PRE_41_DATE = Time.date_sub(date=today, itv=41)
    TX_PRE_60_DATE = Time.date_sub(date=today, itv=60)
    TX_PRE_365_DATE = Time.date_sub(date=today, itv=365)
    return locals()


sql_map={

     # ATTENTION:   ！！！！ sql_01  因为系统按字典顺序进行排序，小于10 的一定要写成0加编号，否则会顺序混乱，数据出问题，切记，切记！！！！
  "sql_01": """
use dmf_tmp;

--20201231分区有问题,跟业务方确认20210101没有做任何变动,可以使用20210101的数据来跑20201231的数据
drop table if exists dmf_tmp.tmp_dmfalm_alm_rpt_01_group_pss_acct_new_s_d_bd_exchange_rate;
create table if not exists dmf_tmp.tmp_dmfalm_alm_rpt_01_group_pss_acct_new_s_d_bd_exchange_rate as
select *
from odm.odm_fi_bd_exchange_rate_s_d
where dt <> '2020-12-31'
    and dt = '{TX_DATE}'
union all
select
    etl_dt,rt_meta,id,foreign_currency_id,foreign_currency_name,base_currency_id,
    base_currency_name,rate,rate_type,rate_date,memo,newly,status,creator_pin,creator,
    create_date,last_update_user_pin,last_update_user,last_update_date,foreign_currency_code,
    base_currency_code,'2020-12-31' as dt
from odm.odm_fi_bd_exchange_rate_s_d
where dt = '2021-01-01'
    and '2020-12-31' = '{TX_DATE}'
;

--时间区间表
drop table if exists dmf_tmp.tmp_dmfalm_alm_rpt_01_group_pss_acct_new_s_d_date;
create table dmf_tmp.tmp_dmfalm_alm_rpt_01_group_pss_acct_new_s_d_date as
select 
    data_time 
from dmf_alm.dmfalm_alm_rpt_01_group_pss_acct_new_s_d 
where data_time >= date_add('{TX_DATE}', -39) 
    and data_time < '{TX_DATE}' 
group by data_time
union all
select '{TX_DATE}' as data_time
;

--本币资金调拨,每日金融收到集团的钱
--特殊处理20200629号人民币调拨数据。因为人民币调拨很少改动，故这次在程序中修改。
drop table if exists dmf_tmp.tmp_dmfalm_alm_rpt_01_group_pss_acct_new_s_d_01_1;
create table if not exists dmf_tmp.tmp_dmfalm_alm_rpt_01_group_pss_acct_new_s_d_01_1 as
select 
    data_time as data_time,
    sum(case when data_time = '2020-09-30' then 0   -- 20200930 rmb 调拨写死为0 
             when data_time = tradeDate and data_time <> '2020-09-30' then nvl(txamount_xb, 0) - nvl(txamount_ss, 0) 
        else 0 end
       ) as item_valn
from
(
    select 
        dt as data_time,
        substr(tradeDate, 1, 10) as tradeDate,
        currencytype, --对账单备注
        sum(case when txtype = '-1' then txamount else 0 end) as txamount_ss, --金融向集团偿还借款(贷)
        sum(case when txtype = '1' then txamount else 0 end) as txamount_xb  --集团向金融支付借款(借)
    from odm.odm_fi_zj_fss_balance_bank_account_20200101_i_d --资金系统银行对账单表
    where dt >= date_add('{TX_DATE}', -39) 
        and dt <= '{TX_DATE}'
        and substr(tradeDate, 1, 10) >= date_add('{TX_DATE}', -39) 
        and substr(tradeDate, 1, 10) <= '{TX_DATE}' --银行交易日期
        and parteraccount = '110907100910711' --对账账户
        and bankcode not in ('1620','6885')   -- updated by  wanglixin16  20200916 新增删除 6885银行账户
        AND valid = 1 --是否有效
    group by dt, substr(tradeDate, 1, 10), currencytype
) t 
group by data_time
union all select data_time, item_valn from dmf_add.dmfadd_alm_add_01_rmb_cur_cannib_add_s_d
;

    """,

  "sql_02": """
use dmf_tmp;

--初始化起始日人民币调拨余额，然后并上后面所有天的调拨金额，最后通过窗口函数实现余额计算
drop table if exists dmf_tmp.tmp_dmfalm_alm_rpt_01_group_pss_acct_new_s_d_01_2;
create table if not exists dmf_tmp.tmp_dmfalm_alm_rpt_01_group_pss_acct_new_s_d_01_2 as
select
    data_time as data_time,
    '101' as item_key,
    'RMB资金调拨' as item_name,
    'CNY' as currency_id,
    sum(item_valn) as item_valn
from
( 
    select 
        data_time,
        sum(item_valn) as item_valn 
    from dmf_tmp.tmp_dmfalm_alm_rpt_01_group_pss_acct_new_s_d_01_1 
    where data_time = date_add('{TX_DATE}', -39) 
    group by data_time
    union all
    select  
        substr(date_sub(data_time, -1), 1, 10) as data_time,
        sum(nvl(rmb_cannib, 0)) as item_valn --RMB资金调拨
    from dmf_alm.dmfalm_alm_rpt_01_group_pss_acct_new_s_d
    where data_time = date_add('{TX_DATE}', -40) 
    group by substr(date_sub(data_time, -1), 1, 10)
) t2                             -- 起始日期的余额
group by data_time
union all
select
    data_time as data_time,
    '101' as item_key,
    'RMB资金调拨' as item_name,
    'CNY' as currency_id,
    sum(item_valn) as item_valn
from dmf_tmp.tmp_dmfalm_alm_rpt_01_group_pss_acct_new_s_d_01_1 
where data_time >= date_add('{TX_DATE}', -38) 
    and data_time <= '{TX_DATE}'  --起始日后的调拨金额
group by data_time
;
    """,

  "sql_03": """
use dmf_tmp;

--新建摊还临时表1，插入RMB资金调拨
drop table if exists dmf_tmp.tmp_dmfalm_alm_rpt_01_group_pss_acct_new_s_d_01;
create table if not exists dmf_tmp.tmp_dmfalm_alm_rpt_01_group_pss_acct_new_s_d_01 as
SELECT 
    data_time,
    item_key,
    item_name,
    currency_id,
    SUM(item_valn) OVER(PARTITION BY item_key ORDER BY data_time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as item_valn
FROM 
(
    select 
        t.data_time as data_time,
        '101' as item_key,
        'RMB资金调拨' as item_name,
        'CNY' as currency_id,
        nvl(k.item_valn,0) as item_valn
    from dmf_tmp.tmp_dmfalm_alm_rpt_01_group_pss_acct_new_s_d_date t
    left join dmf_tmp.tmp_dmfalm_alm_rpt_01_group_pss_acct_new_s_d_01_2 k
        on t.data_time = k.data_time
) a
;

 """,

  "sql_04": """
use dmf_tmp;

drop table if exists dmf_tmp.tmp_dmfalm_alm_rpt_01_group_pss_acct_new_s_d_02_2;
create table if not exists dmf_tmp.tmp_dmfalm_alm_rpt_01_group_pss_acct_new_s_d_02_2 as
select 
    k3.*
from
(
    select 
        k2.*,
        row_number() over(partition by k2.data_time,k2.project_name,k2.currency_id order by effective_date desc) as rn1
    from 
    (
        select 
            data_time,
            effective_date,
            project_name,
            currency_id,
            balance
        from dmf_tmp.tmp_dmfalm_alm_rpt_01_group_pss_acct_new_s_d_date k --时间区间表
        left join
        (
            select 
                to_date(effective_date) as effective_date, --生效时间
                project_name, --项目名称
                currency_id, --币种id
                balance --余额
            from
            (
                select
                    t.*,
                    row_number() over(partition by project_name,currency_id,effective_date order by dt desc,effective_date desc) as rn
                from
                (
                    select
                        *,
                        row_number() over(partition by id order by dt desc) as rni
                    from odm.odm_fi_js_group_occupancy_i_d
                ) t --集团占款手工录入数据
                where last_modified_user not in ('chaijianhai', 'renjing37')
                    and deleted = 0
                    and rni = 1
            ) t1 
            where rn = 1 
        ) k1
    ) k2 
    where effective_date <= data_time
) k3  
where k3.rn1 = 1
;

 """,

  "sql_05": """
use dmf_tmp;

-- 建临时表，存放补录数据，按汇率转换
drop table if exists dmf_tmp.tmp_dmfalm_alm_rpt_01_group_pss_acct_new_s_d_02;
create table if not exists dmf_tmp.tmp_dmfalm_alm_rpt_01_group_pss_acct_new_s_d_02 as
select
    a.data_time as data_time
    ,case 
        when project_name = '1' then '105'
        when project_name = '2' then '106'
        when project_name = '3' then '107'
        when project_name = '4' then '108'
        when project_name = '5' then '109'
        when project_name = '6' then '110'
        when project_name = '7' then '113'
        when project_name = '8' then '114'
     end as item_key
    ,case 
        when project_name = '1' then '集团代垫费用'
        when project_name = '2' then '实收资本占用'
        when project_name = '3' then '长期未结算内部往来'
        when project_name = '4' then '京农贷泗洪县放款额+水电煤保证金'
        when project_name = '5' then '战投RMB资金调拨'
        when project_name = '6' then '其他外币调拨'
        when project_name = '7' then '180+回购'
        when project_name = '8' then '非计息占用'
     end as item_name
    ,'CNY' as currency_id
    ,case when a.currency = '137' then balance else balance * b.rate end as item_valn
from
(
    select
        k.*,
        case when currency_id = '1' then '137' when currency_id = '2' then '138' end as currency
    from dmf_tmp.tmp_dmfalm_alm_rpt_01_group_pss_acct_new_s_d_02_2 k
) a
left join
(
    select
        substr(to_date(rate_date), 1, 10) as data_time, --汇率日期
        base_currency_id, --基准币种id
        base_currency_name, --基准币种名称
        rate, --利率
        base_currency_code, --基础币种编码
        rate_date, --汇率日期
        rate_type --汇率类型
    from dmf_tmp.tmp_dmfalm_alm_rpt_01_group_pss_acct_new_s_d_bd_exchange_rate --资产负债系统-汇率换算表
    where dt = '{TX_DATE}'
        and to_date(rate_date) >= date_add('{TX_DATE}', -39) 
        and to_date(rate_date) <= '{TX_DATE}'
 ) b
    on a.currency = b.base_currency_id 
        and a.data_time = b.data_time
;
 """,
 
  "sql_06": """
use dmf_tmp;

-- 插入外币资金调拨
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_group_pss_acct_new_s_d_02
select
    data_time
    ,'112'
    ,'外币资金调拨'
    ,'CNY' as currency_id
    ,sum(ocup_amount)
from dmf_alm.dmfalm_alm_rpt_01_frgn_cur_cannib_s_d --外币调拨汇总表
where data_time >= date_add('{TX_DATE}', -39) 
    and data_time <= '{TX_DATE}'
    and dt <= '{TX_DATE}'
group by data_time
;
 """,
 
   "sql_07": """
use dmf_tmp;

drop table if exists dmf_tmp.tmp_dmfalm_alm_rpt_01_group_pss_acct_new_s_d_03;
create table if not exists dmf_tmp.tmp_dmfalm_alm_rpt_01_group_pss_acct_new_s_d_03 as
select
    data_time                                                                                -- 数据日期
    ,sum(case when item_key = '114' then item_valn else 0.0 end) as no_bear_interest_acct    -- 非计息占用
    ,sum(case when item_key = '101' then item_valn else 0.0 end) as rmb_cannib               -- RMB资金调拨
    ,sum(case when item_key = '105' then item_valn else 0.0 end) as grp_advance_fee          -- 集团代垫费用
    ,sum(case when item_key = '113' then item_valn else 0.0 end) as buyback                  -- 180+回购
    ,sum(case when item_key = '106' then item_valn else 0.0 end) as recv_cap_ocup            -- 实收资本占用余额
    ,sum(case when item_key = '110' then item_valn else 0.0 end) as other_frgn_cur_cannib    -- 其他外币调拨
    ,sum(case when item_key = '112' then item_valn else 0.0 end) as frgn_cur_cannib          -- 外币资金调拨
    ,default.sysdate()                                           as etl_date                 -- etl时间
from
(
     select * 
     from dmf_tmp.tmp_dmfalm_alm_rpt_01_group_pss_acct_new_s_d_01 
     where data_time <= '{TX_DATE}' 
        and data_time >= date_add('{TX_DATE}', -39)
     union all
     select * 
     from dmf_tmp.tmp_dmfalm_alm_rpt_01_group_pss_acct_new_s_d_02 
     where data_time <= '{TX_DATE}' 
        and data_time >= date_add('{TX_DATE}', -39)
) a
group by data_time
;
 """,
 
   "sql_08": """
-- 取报表有效数据，并去重
use dmf_tmp;
drop table IF EXISTS dmf_tmp.tmp_dmfalm_alm_rpt_01_group_pss_acct_new_s_d_04;
create table IF NOT EXISTS dmf_tmp.tmp_dmfalm_alm_rpt_01_group_pss_acct_new_s_d_04 AS
select 
    a.*,
    default.getmd5(data_time,
                   cast(no_bear_interest_acct as bigint),
                   cast(rmb_cannib as bigint),
                   cast(grp_advance_fee as bigint),
                   cast(buyback as bigint),
                   cast(recv_cap_ocup as bigint),
                   cast(other_frgn_cur_cannib as bigint),
                   cast(frgn_cur_cannib as bigint)
                   ) as join_key
from 
(
    select 
        *,
        row_number() over (partition by data_time order by dt desc) as rn
    from dmf_alm.dmfalm_alm_rpt_01_group_pss_acct_new_s_d 
    WHERE (data_type = 1 or data_type is null)  
        AND data_time < '{TX_DATE}' 
        and data_time >= date_add('{TX_DATE}', -39)
        and dt < '{TX_DATE}'
) a
where rn = 1
;
 """,
    
  "sql_09": """

set hive.auto.convert.join = false;
use dmf_alm;
alter table dmf_alm.dmfalm_alm_rpt_01_group_pss_acct_new_s_d drop if exists partition(dt = '{TX_DATE}'); --重跑数据删除重跑日期分区数据

--比较T-1日数据与T-2日（全部历史）数据，若数据发生变化则以T-1日数据为准
INSERT into TABLE dmf_alm.dmfalm_alm_rpt_01_group_pss_acct_new_s_d PARTITION (dt = '{TX_DATE}')
SELECT
       C.data_time
      ,C.no_bear_interest_acct
      ,C.rmb_cannib
      ,C.grp_advance_fee
      ,C.buyback
      ,C.recv_cap_ocup
      ,C.other_frgn_cur_cannib
      ,C.frgn_cur_cannib
      ,default.sysdate()             as etl_date                 -- etl时间
      ,1
FROM dmf_tmp.tmp_dmfalm_alm_rpt_01_group_pss_acct_new_s_d_04 H
FULL OUTER JOIN
(
    SELECT 
        t.*,
        default.getmd5(data_time,
                       cast(no_bear_interest_acct as bigint),
                       cast(rmb_cannib as bigint),
                       cast(grp_advance_fee as bigint),
                       cast(buyback as bigint),
                       cast(recv_cap_ocup as bigint),
                       cast(other_frgn_cur_cannib as bigint),
                       cast(frgn_cur_cannib as bigint)
                       ) as join_key 
    FROM dmf_tmp.tmp_dmfalm_alm_rpt_01_group_pss_acct_new_s_d_03 t
) C
    ON H.data_time = C.data_time
WHERE H.join_key <> C.join_key OR (H.data_time IS NULL);

--冲销T-2前报表变化数据
INSERT into TABLE dmf_alm.dmfalm_alm_rpt_01_group_pss_acct_new_s_d PARTITION (dt = '{TX_DATE}')
SELECT
       H.data_time
      ,-H.no_bear_interest_acct
      ,-H.rmb_cannib
      ,-H.grp_advance_fee
      ,-H.buyback
      ,-H.recv_cap_ocup
      ,-H.other_frgn_cur_cannib
      ,-H.frgn_cur_cannib
      ,default.sysdate()             as etl_date                 -- etl时间
      ,2
FROM dmf_tmp.tmp_dmfalm_alm_rpt_01_group_pss_acct_new_s_d_04 H
FULL OUTER JOIN
(
    SELECT 
        t.*,
        default.getmd5(data_time,
                       cast(no_bear_interest_acct as bigint),
                       cast(rmb_cannib as bigint),
                       cast(grp_advance_fee as bigint),
                       cast(buyback as bigint),
                       cast(recv_cap_ocup as bigint),
                       cast(other_frgn_cur_cannib as bigint),
                       cast(frgn_cur_cannib as bigint)
                       ) as join_key 
    FROM dmf_tmp.tmp_dmfalm_alm_rpt_01_group_pss_acct_new_s_d_03 t
) C
    ON H.data_time = C.data_time
WHERE H.join_key <> C.join_key
;
    """
}


# 以下部分无需改动，除非作业有特殊要求
sql_task = SqlTask()
sql_task.set_sql_runner(sql_runner)
sql_task.set_customized_items(get_customized_items())
return_code = sql_task.execute_sqls(sql_map)
exit(return_code)                                                             