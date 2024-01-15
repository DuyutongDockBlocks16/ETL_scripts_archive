#!/usr/bin/perl
########################################################################################################################
#  Creater        :wanglixin16
#  Creation Time  :2019-09-16
#  Description    :dmfalm_alm_rpt_01_frgn_cur_cannib_s_d 外币调拨汇总表
#  Modify By      :
#  Modify Time    :
#  Modify Content :
#  Script Version :1.0.3
########################################################################################################################
use strict;
use jrjtcommon;
use un_pswd;
use Common::Hive;
use zjcommon;

##############################################
#默认STINGER运行，失败后HIVE运行，可更改Runner和Retry_Runner
#修改最终生成表库名和表名
##############################################

my $Runner = "HIVE";
my $Retry_Runner = "HIVE";
my $DB = "";
my $TABLE = "";
##############################################

if ( $#ARGV < 0 ) { exit(1); }
my $CONTROL_FILE = $ARGV[0];
my $JOB = substr(${CONTROL_FILE}, 4, length(${CONTROL_FILE})-17);

#当日 yyyy-mm-dd
my $TX_DATE = substr(${CONTROL_FILE},length(${CONTROL_FILE})-12, 4).'-'.substr(${CONTROL_FILE},length(${CONTROL_FILE})-8, 2).'-'.substr(${CONTROL_FILE},length(${CONTROL_FILE})-6, 2);

my $TXDATE = substr($TX_DATE, 0, 4).substr($TX_DATE, 5, 2).substr($TX_DATE, 8, 2);                        #当日 yyyymmdd
my $TX_MONTH = substr($TX_DATE, 0, 4).'-'.substr($TX_DATE, 5, 2);                                          #当日所在月 yyyy-mm
my $TXMONTH = substr($TX_DATE, 0, 4).substr($TX_DATE, 5, 2);                                               #当日所在月 yyyymm
my $TX_PREV_DATE = getPreviousDate($TX_DATE);                                                               #前一天 yyyy-mm-dd
my $TX_NEXT_DATE = getNextDate($TX_DATE);                                                                   #下一天 yyyy-mm-dd
my $TXPDATE = substr(${TX_PREV_DATE},0,4).substr(${TX_PREV_DATE},5,2).substr(${TX_PREV_DATE},8,2);        #前一天 yyyymmdd
my $TXNDATE = substr(${TX_NEXT_DATE},0,4).substr(${TX_NEXT_DATE},5,2).substr(${TX_NEXT_DATE},8,2);        #下一天 yyyymmdd
my $CURRENT_TIME = getNowTime();
my $TX_YEAR = substr($TX_DATE, 0, 4);#当年 yyyy

#上个月第一天  yyyy-mm-dd
my $first_day_of_last_month = get_last_month_first_day($TX_DATE);
#上个月最后一天  yyyy-mm-dd
my $last_day_of_last_month  = get_last_month_last_day($TX_DATE);
#上个月 yyyy-mm
my $last_month  = substr($first_day_of_last_month,0,7);

########################################################################################################################
# Write SQL For Your APP
sub getsql
{
    my @SQL_BUFF=();
#########################################################################################
####################################以下为SQL编辑区######################################
#########################################################################################

$SQL_BUFF[0]=qq(
set mapred.job.name=dmfalm_alm_rpt_01_frgn_cur_cannib_s_d0;

use dmf_tmp;

--20201231分区有问题,跟业务方确认20210101没有做任何变动,可以使用20210101的数据来跑20201231的数据
drop table if exists dmf_tmp.tmp_dmfalm_alm_rpt_01_frgn_cur_cannib_s_d_bd_exchange_rate;
create table if not exists dmf_tmp.tmp_dmfalm_alm_rpt_01_frgn_cur_cannib_s_d_bd_exchange_rate as
select *
from odm.odm_fi_bd_exchange_rate_s_d
where dt <> '2020-12-31'
    and dt in(date_add('$TX_DATE', -39), '$TX_DATE')
union all
select
    etl_dt,rt_meta,id,foreign_currency_id,foreign_currency_name,base_currency_id,
    base_currency_name,rate,rate_type,rate_date,memo,newly,status,creator_pin,creator,
    create_date,last_update_user_pin,last_update_user,last_update_date,foreign_currency_code,
    base_currency_code,'2020-12-31' as dt
from odm.odm_fi_bd_exchange_rate_s_d
where dt = '2021-01-01'
    and '2020-12-31' in(date_add('$TX_DATE', -39), '$TX_DATE')
;

-- 新建临时表1，计算调拨明细汇总
drop table if exists dmf_tmp.tmp_dmfalm_alm_rpt_01_frgn_cur_cannib_s_d_01;
create table if not exists dmf_tmp.tmp_dmfalm_alm_rpt_01_frgn_cur_cannib_s_d_01 as
select
    data_time as data_time,
    a.rec_currency_type,
    sum(nvl(rec_amount, 0)) as amount
from (
SELECT m.pay_currency_type as rec_currency_type,substr(bank_rec_date,1,10) as data_time,sum(m.pay_amount) as rec_amount
    FROM
    (
        select
            *
            ,row_number() over(partition by id order by dt desc) as rn
        FROM odm.odm_fi_fcm_allocate_info_i_d k --资金调配管理
    ) m
    where rn = 1
        and allocate_type = '14' -- 下拨集团向金融支付借款
        and status = '4'
		and id != '453123'--逻辑来自王长浩，因为拨款主体变更，导致集团新主体给科技新主体，科技转一圈，再用科技的老主体还集团的老主体。因为单据数量只有这一笔，此处做单独剔除处理
        and pay_account_code not like '%0711%'
        and to_date(bank_rec_date) >= date_add('$TX_DATE', -39)
        and to_date(bank_rec_date) <= '$TX_DATE'
    group by pay_currency_type, substr(bank_rec_date,1,10)
    union all
    SELECT
        substr(bank_rec_date,1,10) as data_time,
        k.rec_currency_type,
        -sum(k.rec_amount) as rec_amount
    FROM
    (
        select
            *
            ,row_number() over(partition by id order by dt desc) as rn
        FROM odm.odm_fi_fcm_allocate_info_i_d t
    ) k
    where rn = 1
        and allocate_type = '13'            -- 上收金融向集团偿还借款
        and status = '4'
        and rec_account_code not like '%0711%'
        and to_date(bank_rec_date) >= date_add('$TX_DATE', -39)
        and to_date(bank_rec_date) <= '$TX_DATE'
    group by rec_currency_type, substr(bank_rec_date,1,10)
    union all
    select
        substr(data_time,1,10) as data_time,
        rec_currency_type,
        rec_amount
    from dmf_add.dmfadd_alm_add_01_frgn_cur_cannib_add_s_d
) a
group by data_time, a.rec_currency_type
;


);


$SQL_BUFF[2]=qq(
set mapred.job.name = tmp_dmfalm_alm_rpt_01_frgn_cur_cannib_s_d2;

use dmf_tmp;

--插入美元
drop table if exists dmf_tmp.tmp_dmfalm_alm_rpt_01_frgn_cur_cannib_s_d_02;
create table if not exists dmf_tmp.tmp_dmfalm_alm_rpt_01_frgn_cur_cannib_s_d_02 as
select
    data_time  as data_time
    ,'138' as rec_currency_type
    ,sum(usd_amount) as amount
from dmf_alm.dmfalm_alm_rpt_01_frgn_cur_cannib_s_d
where data_time = date_add('$TX_DATE', -40)
group by data_time
;

-- 插入港币
insert into dmf_tmp.tmp_dmfalm_alm_rpt_01_frgn_cur_cannib_s_d_02
select
    data_time  as data_time
    ,'228' as rec_currency_type
    ,sum(hkd_amount) as amount
from dmf_alm.dmfalm_alm_rpt_01_frgn_cur_cannib_s_d
where data_time = date_add('$TX_DATE', -40)
group by data_time
;

-- 插入新加坡币
insert into dmf_tmp.tmp_dmfalm_alm_rpt_01_frgn_cur_cannib_s_d_02
select
    data_time  as data_time
    ,'850' as rec_currency_type
    ,sum(sgd_amount) as amount
from dmf_alm.dmfalm_alm_rpt_01_frgn_cur_cannib_s_d
where data_time = date_add('$TX_DATE', -40)
group by data_time
;

--初始化起始日外币调拨余额，然后并上后面所有天的调拨金额，最后通过窗口函数实现余额计算。
drop table if exists dmf_tmp.tmp_dmfalm_alm_rpt_01_frgn_cur_cannib_s_d_02_01;
create table if not exists dmf_tmp.tmp_dmfalm_alm_rpt_01_frgn_cur_cannib_s_d_02_01 as
select
    a.data_time,
    sum(nvl(a.usd_amount, 0) + nvl(b.usd_amount, 0)) as usd_amount, -- 原币本金USD
    sum(nvl(a.hkd_amount, 0) + nvl(b.hkd_amount, 0)) as hkd_amount, -- 原币本金HKD
    sum(nvl(a.sgd_amount, 0) + nvl(b.sgd_amount, 0)) as sgd_amount,  -- 原币本金SGD
    sum((nvl(a.usd_amount, 0) + nvl(b.usd_amount, 0)) * t2.usd_rate
    + (nvl(a.hkd_amount, 0) + nvl(b.hkd_amount, 0)) * t2.hkd_rate
    + (nvl(a.sgd_amount, 0) + nvl(b.sgd_amount, 0)) * t2.sgd_rate) as ocup_amount --外币占用余额换算为RMB
from
(
    select
        substr(date_sub(data_time, -1), 1, 10) data_time, --39天前
        rec_currency_type,
        case when rec_currency_type = '138' then amount else 0 end as usd_amount,
        case when rec_currency_type = '228' then amount else 0 end as hkd_amount,
        case when rec_currency_type = '850' then amount else 0 end as sgd_amount
   from dmf_tmp.tmp_dmfalm_alm_rpt_01_frgn_cur_cannib_s_d_02
)  a
left join
(
    select
        data_time,
        rec_currency_type,
        case when rec_currency_type = '138' then amount else 0 end as usd_amount,
        case when rec_currency_type = '228' then amount else 0 end as hkd_amount,
        case when rec_currency_type = '850' then amount else 0 end as sgd_amount
   from dmf_tmp.tmp_dmfalm_alm_rpt_01_frgn_cur_cannib_s_d_01
) b
    on a.rec_currency_type = b.rec_currency_type
        and a.data_time = b.data_time
left join
(
    select
        to_date(rate_date) as data_time,
        max(case when base_currency_id = '138' then rate end) as usd_rate,
        max(case when base_currency_id = '228' then rate end) as hkd_rate,
        max(case when base_currency_id = '580' then rate end) as sgd_rate
    from dmf_tmp.tmp_dmfalm_alm_rpt_01_frgn_cur_cannib_s_d_bd_exchange_rate
    where dt = date_add('$TX_DATE', -39)
        and substr(rate_date, 1, 10) = date_add('$TX_DATE', -39)
    group by to_date(rate_date)
) t2
    on a.data_time = t2.data_time
group by a.data_time
union all
select
    substr(b.data_time, 1, 10) as data_time,
    sum(nvl(b.usd_amount, 0)) as usd_amount, -- 原币本金USD
    sum(nvl(b.hkd_amount, 0)) as hkd_amount, -- 原币本金HKD
    sum(nvl(b.sgd_amount, 0)) as sgd_amount,  -- 原币本金SGD
    sum(nvl(b.usd_amount, 0) * t2.rate + nvl(b.hkd_amount, 0) * t2.rate
        + nvl(b.sgd_amount, 0) * t2.rate) as ocup_amount --外币占用余额换算为RMB
from
(
    select
        data_time,
        rec_currency_type,
        sum(case when rec_currency_type = '138' then amount else 0 end) as usd_amount,
        sum(case when rec_currency_type = '228' then amount else 0 end) as hkd_amount,
        sum(case when rec_currency_type = '850' then amount else 0 end) as sgd_amount
    from dmf_tmp.tmp_dmfalm_alm_rpt_01_frgn_cur_cannib_s_d_01
    where data_time >= date_add('$TX_DATE', -38)
        and data_time <= '$TX_DATE'  --起始日后的调拨金额
    group by data_time, rec_currency_type
   ) b
left join
(
    select
        to_date(rate_date) as data_time,
        base_currency_id,
        base_currency_name,
        rate,
        base_currency_code,
        rate_date,
        rate_type
    from dmf_tmp.tmp_dmfalm_alm_rpt_01_frgn_cur_cannib_s_d_bd_exchange_rate
    where dt = '$TX_DATE'
        and to_date(rate_date) >= date_add('$TX_DATE', -38)
        and to_date(rate_date) <= '$TX_DATE'
) t2
    on b.rec_currency_type = t2.base_currency_id
        and b.data_time = t2.data_time
group by substr(b.data_time, 1, 10)
;

);

$SQL_BUFF[3]=qq(
set mapred.job.name=tmp_dmfalm_alm_rpt_01_frgn_cur_cannib_s_d3;

use dmf_tmp;

--时间区间表
drop table if exists dmf_tmp.tmp_dmfalm_alm_rpt_01_frgn_cur_cannib_s_d_date;
create table if not exists dmf_tmp.tmp_dmfalm_alm_rpt_01_frgn_cur_cannib_s_d_date as
select
    data_time
from dmf_alm.dmfalm_alm_rpt_01_frgn_cur_cannib_s_d
where data_time >= date_add('$TX_DATE', -39)
    and data_time < '$TX_DATE'
group by data_time
union all
select '$TX_DATE' as data_time 
;


--新建摊还临时表3，计算资金占用
drop table  if  exists dmf_tmp.tmp_dmfalm_alm_rpt_01_frgn_cur_cannib_s_d_03;
create table  dmf_tmp.tmp_dmfalm_alm_rpt_01_frgn_cur_cannib_s_d_03 as
select 
    k1.data_time,
    sum(k1.usd_amount) usd_amount,
    sum(k1.hkd_amount) hkd_amount,
    sum(k1.sgd_amount) sgd_amount,
    sum(k1.usd_amount * nvl(usd_rate, 0)
        + k1.hkd_amount * nvl(hkd_rate, 0)
        + k1.sgd_amount * nvl(sgd_rate, 0))  as ocup_amount
from
(
    SELECT
        data_time,
        SUM(usd_amount) OVER(PARTITION BY item_key ORDER BY data_time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as usd_amount,
        SUM(hkd_amount) OVER(PARTITION BY item_key ORDER BY data_time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as hkd_amount,
        SUM(sgd_amount) OVER(PARTITION BY item_key ORDER BY data_time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as sgd_amount
    FROM
    (
        select
            t.data_time as data_time,
            '101' as item_key,
            nvl(k.usd_amount,0) as usd_amount,
            nvl(k.hkd_amount,0) as hkd_amount,
            nvl(k.sgd_amount,0) as sgd_amount,
            nvl(k.ocup_amount,0) as ocup_amount
        from dmf_tmp.tmp_dmfalm_alm_rpt_01_frgn_cur_cannib_s_d_date t
        left join dmf_tmp.tmp_dmfalm_alm_rpt_01_frgn_cur_cannib_s_d_02_01 k
            on t.data_time = k.data_time
    ) a
) k1           
left join
(
    select
        to_date(rate_date) as data_time,
        max(case when base_currency_id = '138' then rate end) as usd_rate,
        max(case when base_currency_id = '228' then rate end) as hkd_rate,
        max(case when base_currency_id = '580' then rate end) as sgd_rate
    from dmf_tmp.tmp_dmfalm_alm_rpt_01_frgn_cur_cannib_s_d_bd_exchange_rate
    where dt = '$TX_DATE'
        and to_date(rate_date) >= date_add('$TX_DATE', -39)
        and to_date(rate_date) <= '$TX_DATE'
    group by to_date(rate_date)
) t2
    on k1.data_time = t2.data_time
group by k1.data_time        
;
);

$SQL_BUFF[4]=qq(
set mapred.job.name=tmp_dmfalm_alm_rpt_01_frgn_cur_cannib_s_d4;


use dmf_alm;
alter table dmf_alm.dmfalm_alm_rpt_01_frgn_cur_cannib_s_d drop partition(dt='$TX_DATE');

use dmf_tmp;

-- 取报表有效数据，并去重
drop table if exists dmf_tmp.tmp_dmfalm_alm_rpt_01_frgn_cur_cannib_s_d_04;
create table if not exists dmf_tmp.tmp_dmfalm_alm_rpt_01_frgn_cur_cannib_s_d_04 as
select
    a.*,
    default.getmd5(data_time,cast(ocup_amount as bigint)) as join_key
from
(
    select
        *,
        row_number() over (partition by data_time order by dt desc) as rn
    from dmf_alm.dmfalm_alm_rpt_01_frgn_cur_cannib_s_d
    WHERE dt < '$TX_DATE'
        and dt >= date_add('$TX_DATE', -39)
        and(data_type = 1 or data_type is null)
        AND data_time < '$TX_DATE'
        and data_time >= date_add('$TX_DATE', -39)
) a
where rn = 1 
;

);

$SQL_BUFF[5]=qq(
set mapred.job.name=tmp_dmfalm_alm_rpt_01_frgn_cur_cannib_s_d5;

use dmf_alm;

--比较T-1日数据与T-2日（全部历史）数据，若数据发生变化则以T-1日数据为准
INSERT into TABLE dmf_alm.dmfalm_alm_rpt_01_frgn_cur_cannib_s_d PARTITION (dt = '$TX_DATE')
SELECT
    C.data_time
    ,C.usd_amount
    ,C.hkd_amount
    ,C.sgd_amount
    ,C.ocup_amount
    ,1
FROM dmf_tmp.tmp_dmfalm_alm_rpt_01_frgn_cur_cannib_s_d_04 H
FULL OUTER JOIN
(
    SELECT
        t.*,
        default.getmd5(data_time,cast(ocup_amount as bigint)) as join_key
    FROM dmf_tmp.tmp_dmfalm_alm_rpt_01_frgn_cur_cannib_s_d_03 t
    where data_time <= '$TX_DATE'
        and data_time >= date_add('$TX_DATE', -39)
) C
    ON H.data_time = C.data_time
WHERE H.join_key <> C.join_key
    or (H.data_time is null);

--冲销T-2前报表变化数据
INSERT into TABLE dmf_alm.dmfalm_alm_rpt_01_frgn_cur_cannib_s_d PARTITION (dt = '$TX_DATE')
SELECT
    H.data_time
    ,-H.usd_amount
    ,-H.hkd_amount
    ,-H.sgd_amount
    ,-H.ocup_amount
    ,2
FROM dmf_tmp.tmp_dmfalm_alm_rpt_01_frgn_cur_cannib_s_d_04 H
FULL OUTER JOIN
(
    SELECT
        t.*,
        default.getmd5(data_time,cast(ocup_amount as bigint)) as join_key
    FROM dmf_tmp.tmp_dmfalm_alm_rpt_01_frgn_cur_cannib_s_d_03 t
    where data_time <= '$TX_DATE'
        and data_time >= date_add('$TX_DATE', -39)
) C
    ON H.data_time = C.data_time
WHERE H.join_key <> C.join_key
;
);

#############################################################################################
########################################以上为SQL编辑区######################################
#############################################################################################

    return @SQL_BUFF;
}

########################################################################################################################

sub main
{
    my $ret;

    my @sql_buff = getsql();

    for (my $i = 0; $i <= $#sql_buff; $i++) {
        $ret = Common::Hive::run_hive_sql($sql_buff[$i], ${Runner}, ${Retry_Runner});

        if ($ret != 0) {
            print getCurrentDateTime("SQL_BUFF[$i] Execute Failed");
            return $ret;
        }
        else {
            print getCurrentDateTime("SQL_BUFF[$i] Execute Success");
        }
    }

    return $ret;
}

########################################################################################################################
# program section
# To see if there is one parameter,
print getCurrentDateTime(" Startup Success ..");
print "JOB          : $JOB\n";
print "TX_DATE      : $TX_DATE\n";
print "TXDATE       : $TXDATE\n";
print "Target TABLE : $TABLE\n";

my $rc = main();
if ( $rc != 0 ) {
    print getCurrentDateTime("Task Execution Failed"),"\n";
} else{
    print getCurrentDateTime("Task Execution Success"),"\n";
}
exit($rc);
