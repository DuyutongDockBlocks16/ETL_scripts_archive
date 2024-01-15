#!/usr/bin/perl
########################################################################################################################
#  Creater        :wanglixin16
#  Creation Time  :2019-12-03
#  Description    :dmfalm_alm_rpt_01_balance_s_d 滚动预算口径余额表
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

my $Runner = "STINGER";
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
set mapred.job.name=dmfalm_alm_rpt_01_balance_s_d0;

use dmf_tmp;

drop table  if  exists dmf_tmp.tmp_dmfalm_alm_rpt_01_balance_s_d;

--创建临时表，先存放供应链数据
create table  dmf_tmp.tmp_dmfalm_alm_rpt_01_balance_s_d as
SELECT
       k1.dt as data_date,
       'CNY' as currency_cd,
       '1' as if_cap,
       case when biz_line='内部保理' and product_name like '%票据%' then '101'
            when biz_line='内部保理' and product_name not like '%票据%' then '102'
            when biz_line='订单池融资' and (cust_name like '%小米%')  then '103'
            when biz_line='订单池融资' and (cust_name like '%神州数码%')  then '119'
            when biz_line='订单池融资' and cust_name like '%华为%' then '104'
            when biz_line='订单池融资' and (cust_name not like '%华为%' or cust_name not like '%小米%' or cust_name not like '%神州数码%') then '105'
         else '991' end as item_key,
       case when biz_line='内部保理' and product_name like '%票据%' then '保理-票据'
            when biz_line='内部保理' and product_name not like '%票据%' then '保理-非票据'
            when biz_line='订单池融资' and (cust_name like '%小米%')  then '订单池-小米'
            when biz_line='订单池融资' and (cust_name like '%神州数码%')  then '订单池-神码'
            when biz_line='订单池融资' and cust_name like '%华为%' then '订单池-华为'
            when biz_line='订单池融资' and (cust_name not like '%华为%' or cust_name not like '%小米%' or cust_name not like '%神州数码%') then '订单池-其他'
         else '991' end as item_name,
       sum(CASE WHEN k1.currency='USD' THEN  k1.unret_pri*k2.exchg_rate ELSE k1.unret_pri END)  as   unret_pri
    FROM
       (select
               *
          from
               idm.idm_f02_sf_ordr_dtl_s_d
           where DT = '$TX_DATE' 
                 AND biz_line in('内部保理','订单池融资')
        ) k1
   left join
       (select * from dmf_dim.dmfdim_dim_exchg_rate_i_d where DT = '$TX_DATE') k2
    on k1.dt=k2.dt and k1.currency=k2.currency
    group by 
       k1.dt,
       case when biz_line='内部保理' and product_name like '%票据%' then '101'
            when biz_line='内部保理' and product_name not like '%票据%' then '102'
            when biz_line='订单池融资' and (cust_name like '%小米%')  then '103'
            when biz_line='订单池融资' and (cust_name like '%神州数码%')  then '119'
            when biz_line='订单池融资' and cust_name like '%华为%' then '104'
            when biz_line='订单池融资' and (cust_name not like '%华为%' or cust_name not like '%小米%' or cust_name not like '%神州数码%') then '105'
         else '991' end,
       case when biz_line='内部保理' and product_name like '%票据%' then '保理-票据'
            when biz_line='内部保理' and product_name not like '%票据%' then '保理-非票据'
            when biz_line='订单池融资' and (cust_name like '%小米%')  then '订单池-小米'
            when biz_line='订单池融资' and (cust_name like '%神州数码%')  then '订单池-神码'
            when biz_line='订单池融资' and cust_name like '%华为%' then '订单池-华为'
            when biz_line='订单池融资' and (cust_name not like '%华为%' or cust_name not like '%小米%' or cust_name not like '%神州数码%') then '订单池-其他'
         else '991' end
;  

-- 插入资产消费金融白条
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_balance_s_d
SELECT
       a.dt,
       'CNY' as currency_cd,
       '1' as if_cap,
       case when a.bizcode not in ('32') and c.jd_order_id is not null then '106'
           when a.bizcode not in ('32') and c.jd_order_id is null then '107'
            when a.bizcode ='32' then '108' else '992' end as item_key,
       case when a.bizcode not in ('32') and c.jd_order_id is not null then '普白-表内'
           when a.bizcode not in ('32') and c.jd_order_id is null then '普白-表外'
            when a.bizcode ='32' then '取现' else null end as item_name,
       sum(case when b.order_id is null then sdploanamt else 0 end ) + sum(case when b.order_id is not null then  sdploanamt*nvl(investment_ratio,0)/100 else 0 end ) as baitiaoyue--贷款余额
from
     (SELECT
           dt,
           ordr_id as order_id,
           biz_id as bizcode,
           sum(unpayoff_prin) as sdploanamt
       FROM sdm.sdm_f02_cf_xbt_ordr_dtl_s_d
       where DT = '$TX_DATE'
             and to_date(loan_time) <= DT
        group by dt,ordr_id,biz_id
     ) as a
left join
     (select  order_id,
              investor_id,
              investment_ratio,
              dt 
        from odm.odm_fi_investor_loan_s_d
       WHERE status='Success'--交易成功
             and investment_status='1'--已出资
             and DT = '$TX_DATE'
        group by order_id,investor_id,investment_ratio,dt

     ) as b 
on a.order_id=b.order_id and a.dt=b.dt
left join
     (select jd_order_id,dt from dmf_bc.dmfbc_alm_dm_01_bt_loan_info_s_d where  DT = '$TX_DATE'  and (under_ratio> 0 or  investor_type ='1') group by jd_order_id,dt) c
on a.order_id=c.jd_order_id and a.dt=c.dt
group by
a.dt,
case when a.bizcode not in ('32') and c.jd_order_id is not null then '106'
     when a.bizcode not in ('32') and c.jd_order_id is null then '107'
     when a.bizcode ='32' then '108' else '992' end,
case when a.bizcode not in ('32') and c.jd_order_id is not null then '普白-表内'
   when a.bizcode not in ('32') and c.jd_order_id is null then '普白-表外'
     when a.bizcode ='32' then '取现' else null end 
 
;

-- 插入资产消费金融大白条
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_balance_s_d
select
       dt,
       'CNY' as currency_cd,
       '1' as if_cap,
       case when A.merchant_code IN ('100099', '2160000001', '2680000000', '2680000001', '2680000005','2680000006','2920000001', '3170000001','3170000002', '3180000001', '3190000001', '3230000001','3270000001', '3270000002', '3270000003',
                                '3410000001','3550000001','3550000002','3550000003','3580000001','3580000002','3630000001','3630000002','3870000001','3910000001','3980000001','4050000001','4920000001','6000000001','8800000001',
                                '5980000001','5950000001') then '109' else '110' end as item_key,
       case when A.merchant_code IN ('100099', '2160000001', '2680000000', '2680000001', '2680000005','2680000006','2920000001', '3170000001','3170000002', '3180000001', '3190000001', '3230000001','3270000001', '3270000002', '3270000003',
                                '3410000001','3550000001','3550000002','3550000003','3580000001','3580000002','3630000001','3630000002','3870000001','3910000001','3980000001','4050000001','4920000001','6000000001','8800000001',
                                '5980000001','5950000001') then '汽车金融' else '大白其他' end as item_name,
       CAST(SUM(A.should_pay_amt) as bigint)
FROM
     dmf_bc.dmfbc_alm_dm_01_bt_plus_loan_info_s_d A
where
      DT = '$TX_DATE'
group by
  dt,
  case when A.merchant_code IN ('100099', '2160000001', '2680000000', '2680000001', '2680000005','2680000006','2920000001', '3170000001','3170000002', '3180000001', '3190000001', '3230000001','3270000001', '3270000002', '3270000003',
                                '3410000001','3550000001','3550000002','3550000003','3580000001','3580000002','3630000001','3630000002','3870000001','3910000001','3980000001','4050000001','4920000001','6000000001','8800000001',
                                '5980000001','5950000001') then '109' else '110' end,
  case when A.merchant_code IN ('100099', '2160000001', '2680000000', '2680000001', '2680000005','2680000006','2920000001', '3170000001','3170000002', '3180000001', '3190000001', '3230000001','3270000001', '3270000002', '3270000003',
                                '3410000001','3550000001','3550000002','3550000003','3580000001','3580000002','3630000001','3630000002','3870000001','3910000001','3980000001','4050000001','4920000001','6000000001','8800000001',
                                '5980000001','5950000001') then '汽车金融' else '大白其他' end 

;

-- 插入资产消费金融金条
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_balance_s_d
select
       dt
      ,'CNY' as currency_cd
      ,'1' as if_cap
      ,'111'
      ,'金条-全部'
      ,sum(should_pay_amt)
  from dmf_bc.dmfbc_alm_dm_01_jt_acct_info_s_d
  where dt='$TX_DATE'
  group by dt
;



);

$SQL_BUFF[1]=qq(
set mapred.job.name=tmp_dmfalm_alm_rpt_01_balance_s_d1;

use dmf_tmp;

-- 插入负债消费金融白条联合贷
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_balance_s_d
select 
        t1.dt
       ,'CNY' as currency_cd
       ,'2' as if_cap
       ,case when t2.biz_id='32' then  '112'  --取现
             when t2.biz_id<>'32' and under_ratio>0 then '113'
             else '114'
        end as item_key
       ,case when t2.biz_id='32' then  '白条取现联合贷'  --取现
             when t2.biz_id<>'32' and under_ratio>0 then '普白-表内联合贷'
             else '普白-表外联合贷'
        end as item_name
      ,CAST(SUM(t1.should_pay_amt)  AS BIGINT )
 from 
(select 
        dt,loan_id,should_pay_amt,under_ratio
   from 
        dmf_bc.dmfbc_alm_dm_01_lhd_acct_info_s_d 
   where dt='$TX_DATE' and type ='BT' and should_pay_amt >= 0 
) t1
inner join
(SELECT
      dt,
      loan_id,
      biz_id
  FROM sdm.sdm_f02_cf_xbt_ordr_dtl_s_d
  where dt='$TX_DATE'
        and to_date(loan_time) <= DT
   group by dt,loan_id,biz_id
) t2
on t1.loan_id=t2.loan_id and t1.dt=t2.dt
group by 
        t1.dt
       ,case when t2.biz_id='32' then  '112'  --取现
             when t2.biz_id<>'32' and under_ratio>0 then '113'
             else '114'
        end
       ,case when t2.biz_id='32' then  '白条取现联合贷'  --取现
             when t2.biz_id<>'32' and under_ratio>0 then '普白-表内联合贷'
             else '普白-表外联合贷'
        end
;

-- 插入负债消费金融大白条联合贷
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_balance_s_d
select 
       t1.dt
      ,'CNY'
      ,'2'
      ,t2.item_key
      ,t2.item_name
      ,sum(t1.should_pay_amt) should_pay_amt
from
(SELECT
        dt
       ,loan_id
       ,sum(A.should_pay_amt ) AS should_pay_amt
  FROM
       dmf_bc.dmfbc_alm_dm_01_lhd_acct_info_s_d A
  where dt='$TX_DATE'
        and union_type='3'
  group by dt,loan_id
) t1
inner join
(
  select
         dt,
         case when A.merchant_code IN ('100099', '2160000001', '2680000000', '2680000001', '2680000005','2680000006','2920000001', '3170000001','3170000002', '3180000001', '3190000001', '3230000001','3270000001', '3270000002', '3270000003',
                                       '3410000001','3550000001','3550000002','3550000003','3580000001','3580000002','3630000001','3630000002','3870000001','3910000001','3980000001','4050000001','4920000001','6000000001','8800000001',
                                       '5980000001','5950000001') then '115' else '116' end as item_key,
         case when A.merchant_code IN ('100099', '2160000001', '2680000000', '2680000001', '2680000005','2680000006','2920000001', '3170000001','3170000002', '3180000001', '3190000001', '3230000001','3270000001', '3270000002', '3270000003',
                                       '3410000001','3550000001','3550000002','3550000003','3580000001','3580000002','3630000001','3630000002','3870000001','3910000001','3980000001','4050000001','4920000001','6000000001','8800000001',
                                       '5980000001','5950000001') then '汽车金融' else '大白其他' end as item_name,
         loan_id
FROM
     dmf_bc.dmfbc_alm_dm_01_bt_plus_loan_info_s_d A
where
      dt='$TX_DATE'
group by
         dt,
         loan_id,
         case when A.merchant_code IN ('100099', '2160000001', '2680000000', '2680000001', '2680000005','2680000006','2920000001', '3170000001','3170000002', '3180000001', '3190000001', '3230000001','3270000001', '3270000002', '3270000003',
                                       '3410000001','3550000001','3550000002','3550000003','3580000001','3580000002','3630000001','3630000002','3870000001','3910000001','3980000001','4050000001','4920000001','6000000001','8800000001',
                                       '5980000001','5950000001') then '115' else '116' end,
         case when A.merchant_code IN ('100099', '2160000001', '2680000000', '2680000001', '2680000005','2680000006','2920000001', '3170000001','3170000002', '3180000001', '3190000001', '3230000001','3270000001', '3270000002', '3270000003',
                                       '3410000001','3550000001','3550000002','3550000003','3580000001','3580000002','3630000001','3630000002','3870000001','3910000001','3980000001','4050000001','4920000001','6000000001','8800000001',
                                       '5980000001','5950000001') then '汽车金融' else '大白其他' end
) t2
on t1.loan_id=t2.loan_id and t1.dt=t2.dt  
group by t1.dt,t2.item_key,t2.item_name     
; 

-- 插入负债消费金融金条联合贷
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_balance_s_d
select 
       '$TX_DATE' as etl_dt
      ,'CNY' as currency_cd
      ,'2'
      ,'117' as item_key -- 金条联合贷-全部
      ,'金条联合贷'
      ,sum(Should_Pay_Amt) item_valn
 FROM 
      dmf_bc.dmfbc_alm_dm_01_lhd_acct_info_s_d
 where dt = '$TX_DATE' AND UNION_TYPE = '1'
;


-- 插入负债普通白条（商城出资）
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_balance_s_d
select
       etl_dt
      ,'CNY'
      ,'2'
      ,'118'
      ,'普通白条(商城出资)'
      ,sum(case  when item_key in ('201','202','203') then item_valn else 0 end)
       - sum(case  when item_key in ('205') then item_valn else 0 end)
  from
      dmf_alm.dmfalm_alm_rpt_01_bt_cap_ocup_fee_s_d
   where dt='$TX_DATE'
         and item_key in ('201','202','203','205')
   group by etl_dt
;
);


$SQL_BUFF[2]=qq(
set mapred.job.name=tmp_dmfalm_alm_rpt_01_balance_s_d2;

use dmf_alm;
insert overwrite table dmf_alm.dmfalm_alm_rpt_01_balance_s_d partition(dt='$TX_DATE')
select
       A.data_date        -- 时间
      ,A.currency_cd      --币种
      ,A.if_cap           --是否资产 1资产 2负债
      ,A.item_key         -- 编码
      ,A.item_name        -- 名称
      ,A.unret_pri        -- 值
  from
       dmf_tmp.tmp_dmfalm_alm_rpt_01_balance_s_d A
;

);

$SQL_BUFF[3]=qq(
set mapred.job.name=tmp_dmfalm_alm_rpt_01_balance_s_d3;

use dmf_tmp;
drop table IF EXISTS dmf_tmp.tmp_dmfalm_alm_rpt_01_balance_s_d;

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
