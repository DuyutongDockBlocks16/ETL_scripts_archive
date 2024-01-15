#!/usr/bin/perl
########################################################################################################################
#  Creater        :wanglixin16
#  Creation Time  :2018-09-13
#  Description    :dmfalm_alm_rpt_01_for_week_s_d for周预测
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
#默认STINGER运行,失败后HIVE运行,可更改Runner和Retry_Runner
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

########################################################################################################################
# Write SQL For Your APP
sub getsql
{
    my @SQL_BUFF=();
#########################################################################################
####################################以下为SQL编辑区######################################
#########################################################################################
$SQL_BUFF[1]=qq(
set mapred.job.name=dmfalm_alm_rpt_01_for_week_s_d1;

use dmf_tmp;
drop table if exists dmf_tmp.tmp_dmfalm_alm_rpt_01_for_week_s_d;
create table if not exists dmf_tmp.tmp_dmfalm_alm_rpt_01_for_week_s_d
like dmf_alm.dmfalm_alm_rpt_01_for_week_s_d
;

);

$SQL_BUFF[2]=qq(
set mapred.job.name=tmp_dmfalm_alm_rpt_01_for_week_s_d2;

use dmf_tmp;

insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_for_week_s_d partition(dt='$TX_DATE') ---京农贷
select
        '$TX_DATE',
        '京农贷',
        CAST(SUM(should_pay_amt)  AS BIGINT )
from dmf_bc.dmfbc_alm_dm_01_jnd_acct_info_s_d
    WHERE
        DT='$TX_DATE'
;

insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_for_week_s_d partition(dt='$TX_DATE') ---京农贷(非金融出资)
select
        dt,
        '京农贷（非金融出资）',
        CAST(sum(SHOULD_PAY_AMT) as BIGINT )
FROM dmf_bc.dmfbc_alm_dm_01_lhd_acct_info_s_d 
    where
        dt='$TX_DATE'
        AND UNION_TYPE='4'
        AND Investment_Code IN (
            '30017842' ,'30025355' ,'30026632'
        )
    group by dt
;
);

$SQL_BUFF[3]=qq(
set mapred.job.name=tmp_dmfalm_alm_rpt_01_for_week_s_d3;


use dmf_tmp;

--插入dwb_bt_xbt_ordr_det_s_d表中的数据
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_for_week_s_d partition(dt='$TX_DATE') ---小白
select
        dt,
        case when prod_type<>'校园' then '小白'
             when prod_type='校园' then '校白'
           end as PROD_TYPE,
         CAST(sum(unpayoff_prin)  AS BIGINT) as tot_bal
from sdm.sdm_f02_cf_xbt_ordr_dtl_s_d
    where
        dt= '$TX_DATE'
        and to_date(
            loan_time
        ) <= DT
       group by dt,
       case when prod_type<>'校园' then '小白'
            when prod_type='校园' then '校白'
           end
;
--单独处理小白total，'小白total'='小白'+'校白'
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_for_week_s_d partition(dt='$TX_DATE') ---小白total
select
        '$TX_DATE',
        '小白total',
        sum(balance)  as tot_bal
from dmf_tmp.tmp_dmfalm_alm_rpt_01_for_week_s_d
    where
        dt= '$TX_DATE' and busi_name in ('小白','校白')
;
);



$SQL_BUFF[4]=qq(
set mapred.job.name=tmp_dmfalm_alm_rpt_01_for_week_s_d4;


use dmf_tmp;

-- 根据业务要求将金条口径从财务口径更新为经分口径wanglixin16 20190311
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_for_week_s_d partition(dt='$TX_DATE') ---金条余额
select
        t1.dt  as dim_day_id,
       '金条余额',
        CAST(sum(Should_Pay_Amt) as BIGINT  )  as tot_loan
        
from 
     dmf_bc.dmfbc_alm_dm_01_jt_acct_info_s_d t1
 where
       dt='$TX_DATE'
 group by
        t1.dt;
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_for_week_s_d partition(dt='$TX_DATE') ---金条余额(非金融出资)
select 
        t1.dt as dim_day_id, 
      '金条余额（非金融出资）', 
       CAST(sum(Should_Pay_Amt) as BIGINT  )   as tot_loan
FROM dmf_bc.dmfbc_alm_dm_01_lhd_acct_info_s_d t1
INNER JOIN odm.ODM_FI_PLAT_INVESTOR_S_D t2
   ON t1.Investment_Code = t2.investor_id 
  AND t2.DT='$TX_DATE' 
  AND t2.INVESTOR_TYPE='0' 
  AND t2.PLAT_ID = '1'
WHERE t1.dt = '$TX_DATE' AND t1.UNION_TYPE = '1'
GROUP BY t1.dt
;
);

$SQL_BUFF[5]=qq(
set mapred.job.name=tmp_dmfalm_alm_rpt_01_for_week_s_d5;


use dmf_tmp;

--插入ODM_CF_QY_IOU_RECEIPT_S_D表中的数据
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_for_week_s_d partition(dt='$TX_DATE') ---金采（商城）
select
      dt,
      '金采（商城）',
      sum(nvl(order_amt, 0) - nvl(refund_prcp_amt,0) - nvl(repay_prcp_amt, 0)) as tot_bal
    from
        ODM.ODM_CF_QY_IOU_RECEIPT_S_D
    WHERE
        dt= '$TX_DATE'
        and jd_order_type = '20096'
        and to_date(
            tx_time
        ) <= dt
    GROUP BY
        DT
;
);




$SQL_BUFF[6]=qq(
set mapred.job.name=tmp_dmfalm_alm_rpt_01_for_week_s_d6;

use dmf_tmp;

--插入dwd_actv_bt_plus_plan_s_d表中的数据
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_for_week_s_d partition(dt='$TX_DATE') ---分期业务
select
        a.dt,
        '分期业务',
        CAST(sum(a.shouldpayamount) as BIGINT ) as tot_bal
    from
        (select
            *
        from
            ODM.ODM_CF_PLUS_PLAN_S_D
        where
            dt='$TX_DATE'
            and to_date(
                createdate
            )<=dt ) a
    inner join
        (
            select
                *
            from
                ODM.ODM_CF_PLUS_LOAN_ORDER_S_D
            WHERE
                dt='$TX_DATE'
                and status =3
                and to_date(
                    completetime
                )<=dt
        ) c
            on a.loanno=c.loanid
    inner join
        (
            SELECT
                  merchantno as merchantCode
                 ,merchantnoname as merchantName
             FROM ODM.ODM_CF_PLUS_MERCHANTNO_S_D
              WHERE dt = '$TX_DATE'
             GROUP BY merchantno,merchantnoname
        )  b
            on a.merchantCode=b.merchantCode
      group by a.dt
;
);

$SQL_BUFF[7]=qq(
set mapred.job.name=tmp_dmfalm_alm_rpt_01_for_week_s_d7;

use dmf_tmp;

FROM
(SELECT
             k1.*,
             CASE WHEN k1.currency='USD' THEN  k1.unret_pri*k2.exchg_rate ELSE k1.unret_pri END  as   unret_pri_new
        FROM
             (select
                     *
                from
                     idm.idm_f02_sf_ordr_dtl_s_d
                 where DT = '$TX_DATE'
                       AND biz_line in('动产质押','外部小贷','内部小贷','外部保理','内部保理','快银','订单池融资','快银BOSS贷','车主易贷')
              ) k1
         left join
             (select * from dmf_dim.dmfdim_dim_exchg_rate_i_d where DT = '$TX_DATE') k2
          on k1.dt=k2.dt and k1.currency=k2.currency
) A
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_for_week_s_d partition(dt) ---保理-内部
SELECT
        dt,
        '保理-内部',
        CAST(SUM(unret_pri_new)  AS BIGINT )  AS unret_pri,
        '$TX_DATE'
    
    WHERE
        DT='$TX_DATE'
        AND biz_line='内部保理'
     group by dt
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_for_week_s_d partition(dt) ---保理-外部
SELECT  
        dt,
        '保理-外部',
        CAST(SUM(unret_pri_new)  AS BIGINT )  AS unret_pri,
        '$TX_DATE'
    WHERE
        DT='$TX_DATE'
        AND biz_line='外部保理'
     group by dt
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_for_week_s_d partition(dt) ---订单池融资
SELECT
        dt,
        case when cust_name like '%华为%' then '订单池融资-华为' else '订单池融资-非华为' end,
        CAST(SUM(unret_pri_new)  AS BIGINT )    AS unret_pri,
        '$TX_DATE'
 
    WHERE
        DT='$TX_DATE'
        AND biz_line='订单池融资'
     group by dt,
              case when cust_name like '%华为%' then '订单池融资-华为' else '订单池融资-非华为' end
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_for_week_s_d partition(dt) ---小贷业务-内部
select
        dt,
        '小贷业务-内部',
        round(sum(unret_pri_new),2),
       '$TX_DATE'
   WHERE
        dt='$TX_DATE'
        and biz_line='内部小贷'
    group by dt
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_for_week_s_d partition(dt) ---京小贷外部
select
        dt,
        '京小贷外部',
        CAST(sum(unret_pri_new)   AS BIGINT ),
       '$TX_DATE'
     WHERE
        dt ='$TX_DATE'
        AND biz_line IN ('外部小贷','快银','快银BOSS贷')
     group by dt
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_for_week_s_d partition(dt) ---动产质押
SELECT
        dt,
        '动产质押',
        CAST(SUM(unret_pri_new) AS BIGINT )    AS unret_pri,
        '$TX_DATE'
 
    WHERE
        DT='$TX_DATE'                                                                                                    
        AND biz_line like '%动产%'
    group by dt
;
);


$SQL_BUFF[8]=qq(
set mapred.job.name=tmp_dmfalm_alm_rpt_01_for_week_s_d8;

use dmf_tmp;

--插入DWB_LOAN_ORDR_DET_JF_S_D&ODM_CF_QY_IOU_RECEIPT_S_D表中的数据
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_for_week_s_d partition(dt='$TX_DATE') ---金采业务（新KA）
select
        '$TX_DATE',
        '金采业务（新KA）',
        unpayoff_prin-tot_bal
    from
        (SELECT
        CAST(SUM(jc_unret_pri )  AS BIGINT )    AS unpayoff_prin
    FROM
        sdm.sdm_f02_sf_jdjc_prod_tx_sum_a_d A
    WHERE
        A.etl_dt='$TX_DATE'
        )aa
    join
        (
            select
                sum(nvl(order_amt,
                0) - nvl(refund_prcp_amt,
                0) - nvl(repay_prcp_amt,
                0))     as tot_bal
            from
                ODM.ODM_CF_QY_IOU_RECEIPT_S_D b
            WHERE
                dt= '$TX_DATE'
                and jd_order_type='20096'
                and to_date(
                    tx_time
                ) <= dt
        )bb
            on 1=1

;
);

$SQL_BUFF[11]=qq(
set mapred.job.name=tmp_dmfalm_alm_rpt_01_for_week_s_d11;

use app;
insert overwrite table dmf_alm.dmfalm_alm_rpt_01_for_week_s_d partition(dt='$TX_DATE')
select   
       etl_dt   
      ,busi_name
      ,sum(balance) as balance  
from  dmf_tmp.tmp_dmfalm_alm_rpt_01_for_week_s_d
where dt='$TX_DATE'
group by etl_dt,busi_name
;
);

$SQL_BUFF[12]=qq(
set mapred.job.name=tmp_dmfalm_alm_rpt_01_for_week_s_d12;

use dmf_tmp;
drop table dmf_tmp.tmp_dmfalm_alm_rpt_01_for_week_s_d
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
