#!/usr/bin/perl
########################################################################################################################
#  Creater        :wanglixin16
#  Creation Time  :2019-04-17
#  Description    :dmfalm_alm_rpt_01_bt_asset_s_d 白条资产日报
#                 :report_key='BT_ASSETS',白条资产; operate_org_cd='JDJR',京东金融; currency_cd='CNY',人民币
#  Modify By      :
#  Modify Time    :
#                   20190924   wanglixin ODM_FI_INVESTOR_LOAN_S_D 表新增create_date进行时间限定，否则会有T+0数据
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

########################################################################################################################
# Write SQL For Your APP
sub getsql
{
    my @SQL_BUFF=();
#########################################################################################
####################################以下为SQL编辑区######################################
#########################################################################################

$SQL_BUFF[1]=qq(
set mapred.job.name=dmfalm_alm_rpt_01_bt_asset_s_d1;

use dmf_tmp;
drop table if exists dmf_tmp.tmp_dmfalm_alm_rpt_01_bt_asset_s_d;
create table if not exists dmf_tmp.tmp_dmfalm_alm_rpt_01_bt_asset_s_d
like dmf_alm.dmfalm_alm_rpt_01_bt_asset_s_d
;

);

$SQL_BUFF[2]=qq(
set mapred.job.name=tmp_dmfalm_alm_rpt_01_bt_asset_s_d2;

use dmf_tmp;

--校园、农村、普通白条
insert into TABLE dmf_tmp.tmp_dmfalm_alm_rpt_01_bt_asset_s_d partition(dt='$TX_DATE')
SELECT dt as etl_dt
      ,'BT_ASSETS' as report_key
      ,dt as date_id
      ,'JDJR' as operate_org_cd
      ,'CNY' as currency_cd
      ,CASE PROD_TYPE
            WHEN '普通' THEN
                case when  biz_nm4 = '大额订单'  then '101'
                     when  biz_nm4 = '到家'      then '102'
                     when  biz_nm4 = '扶贫专项'  then '103'
                     when  biz_nm4 = '商城'      then '104'
                     end
            WHEN '农村' THEN
                 case when  biz_nm4 = '到家'     then '201'
                      when  biz_nm4 = '扶贫专项' then '202'
                      when  biz_nm4 = '商城'     then '203'
                      end
            WHEN '校园' THEN
                 case when  biz_nm4 = '到家'     then '301'
                      when  biz_nm4 = '商城'     then '302'
                      end
       END as item_key
      ,CAST(tot_bal as bigint)   as item_valn
      ,''   as item_vals
  from (
   SELECT t1.DT, t1.PROD_TYPE,t2.biz_nm4, sum(unpayoff_prin) as tot_bal
     from sdm.sdm_f02_cf_xbt_ordr_dtl_s_d t1
          inner join dim.dim_bt_tran_jf_a_d t2
             on t1.biz_id = t2.biz_id4 and  t2.biz_nm4 in ('到家', '商城', '扶贫专项', '大额订单')
    where t1.dt = '$TX_DATE' and to_date(loan_time) <= '$TX_DATE' and t1.PROD_TYPE in ('校园', '农村', '普通')
    group by  t1.DT,  t1.PROD_TYPE, t2.biz_nm4
      )  U
;

);


$SQL_BUFF[3]=qq(
set mapred.job.name=tmp_dmfalm_alm_rpt_01_bt_asset_s_d3;

use dmf_tmp;

--金彩
insert into TABLE  dmf_tmp.tmp_dmfalm_alm_rpt_01_bt_asset_s_d partition(dt = '$TX_DATE')
SELECT '$TX_DATE' as etl_dt
      ,'BT_ASSETS' as report_key
      ,t1.dt
      ,'JDJR' as operate_org_cd
      ,'CNY' as currency_cd
      ,case
          when concat('供应链-旧金采-', t2.producttype) = '供应链-旧金采-MD'        THEN '401'
          when concat('供应链-旧金采-', t2.producttype) = '供应链-旧金采-分销系统'  THEN '402'
          when concat('供应链-旧金采-', t2.producttype) = '供应链-旧金采-金采KA'    THEN '403'
          when concat('供应链-旧金采-', t2.producttype) = '供应链-旧金采-金采SHP'   THEN '404'
          when concat('供应链-旧金采-', t2.producttype) = '供应链-旧金采-金采SMB'   THEN '405'
          ELSE ''
        end as item_key
      ,sum(order_amt-refund_prcp_amt-repay_prcp_amt) as item_val
      ,'' as item_vals
 FROM (SELECT * 
         from ODM.ODM_CF_QY_IOU_RECEIPT_S_D
        where dt='$TX_DATE' and jd_order_type='20096' and to_date( tx_time )<=dt) t1
         left join
        (SELECT a.*,
                case
                    when A.product_type='20072' then '金采KA'
                    when A.product_type='20073' then '金采SMB'
                    when A.product_type='20269' then '金采SHP'
                    when A.product_type='20236' then '找钢网'
                    when A.product_type='20288' then '分销系统'
                    when A.product_type='20302' then 'XLT'
                    when A.product_type='20307' then 'QL'
                    when A.product_type='20400' then 'MD'
                    when A.product_type='20409' then 'HXE'
                    when A.product_type='20436' then 'HBY'
                    ELSE ''
                END as producttype
            from    odm.ODM_CF_QY_CUST_ACCOUNT_S_D  a
            where   dt='$TX_DATE'
        )  t2
            on t1.jd_pin=t2.jd_pin
    group by t1.dt, t2.producttype
;

);


$SQL_BUFF[4]=qq(
set mapred.job.name=tmp_dmfalm_alm_rpt_01_bt_asset_s_d4;

use dmf_tmp;

--大白条
FROM (
SELECT  dt
       ,'CNY' AS CURRENCY_CD
       ,case
             when merchantCode ='2810000001'  THEN 'POP教育'
             when merchantCode ='3850000001'  THEN '分期用'
             when merchantCode ='3850000003'  THEN '分期用二期'
             when merchantCode ='3730000001'  THEN '手机租赁'
        end as item_key
       ,sum(nvl(shouldpayamount,0)) as tot_bal
    from
        (SELECT a.* from
            (SELECT * from odm.ODM_CF_PLUS_PLAN_S_D
              where dt= '$TX_DATE' and to_date(createdate) <= dt
            ) a
        inner join
            (SELECT * from odm.ODM_CF_PLUS_LOAN_ORDER_S_D
              where dt = '$TX_DATE' and status = 3
                    and to_date(completetime) <= dt 
            ) c
           on a.loanno=c.loanid and a.dt = c.dt
        inner join
            (SELECT * from dim.dim_whip_merchant_cate_a_d
              where producttype not in (4, 5, 6, 7)
                    and merchantno in ('2810000001' ,'3730000001' ,'3850000001' , '3850000003')
            ) b
           on a.merchantCode = b.merchantno
         ) t1 
     group by merchantCode, dt) T
insert into TABLE dmf_tmp.tmp_dmfalm_alm_rpt_01_bt_asset_s_d partition(dt = '$TX_DATE')
SELECT dt as  etl_dt
      ,'BT_ASSETS' as report_key
      ,dt
      ,'JDJR' as operate_org_cd
      ,CURRENCY_CD
      ,case item_key
            when 'POP教育'     THEN '501'
            when '分期用二期'  THEN '502'
            when '分期用'      THEN '503'
            when '手机租赁'    THEN '504'
            ELSE ''
       END as item_key
      ,CAST(tot_bal AS BIGINT ) as item_valn
      ,'' as item_valns
;

);

$SQL_BUFF[5]=qq(
set mapred.job.name=tmp_dmfalm_alm_rpt_01_bt_asset_s_d5;

use dmf_tmp;

--赊销转信贷
insert into TABLE dmf_tmp.tmp_dmfalm_alm_rpt_01_bt_asset_s_d partition(dt = '$TX_DATE')
SELECT
        '$TX_DATE'  as etl_dt
       ,'BT_ASSETS' as report_key
       ,'$TX_DATE'  as date_id
       ,'JDJR'      as operate_org_cd
       ,'CNY'       as currency_cd
       ,nvl(b.item_key, b1.item_key) as item_key
       ,nvl(loan_amount + Add_Val, 0.0)- nvl(PaidPrincipal+CancelPrincipal+ReverseUnpaidPrincipal, 0.0) as item_valn
       ,'' as item_vals
    FROM
        (SELECT CASE INVESTOR_ID 
                    WHEN 'inv_sdtoxd_cqxd' THEN '602' -- 内部出资
                    ELSE '601'                        -- 外部出资
                END AS ITEM_KEY,
                CASE INVESTOR_ID 
                    WHEN 'inv_sdtoxd_cqxd' THEN 17585880.71   -- 内部出资
                    ELSE 1973824277.13                        -- 外部出资
                END AS Add_Val,
                SUM(loan_amount) AS loan_amount  
           FROM odm.ODM_FI_INVESTOR_LOAN_S_D  B  
          WHERE B.DT = '$TX_DATE' 
                and to_date(create_time)<=dt           -- 去除T+0 数据
                and to_date(record_update_time)<=dt    -- 去除T+0 数据
                AND b.INVESTOR_ID IN ('inv_sdtoxd_shyh','inv_sdtoxd_xian','inv_sdtoxd_gsbank', 'inv_sdtoxd_cqxd','inv_sdtoxd_shyhfxmd', 'inv_sdtoxd_zyxffxmd')
                AND status in('Success','Cancel')
                AND TO_DATE(B.PLAT_CREATE_TIME) BETWEEN '2018-08-01' AND B.DT 
          GROUP BY CASE INVESTOR_ID 
                        WHEN 'inv_sdtoxd_cqxd' THEN '602' -- 内部出资
                        ELSE '601'                        -- 外部出资
                   END,
                   CASE INVESTOR_ID 
                        WHEN 'inv_sdtoxd_cqxd' THEN 17585880.71   -- 内部出资
                        ELSE 1973824277.13                        -- 外部出资
                    END
         ) B
    LEFT JOIN
        (SELECT CASE INVESTOR_ID 
                    WHEN 'inv_sdtoxd_cqxd' THEN '602' -- 内部出资
                    ELSE '601'                        -- 外部出资
                END AS ITEM_KEY,
                SUM(CASE WHEN a.business_type ='Repayment' AND a.money_type='PaidPrincipal'          THEN COALESCE(a.amount,0.0) ELSE 0.0 END) AS PaidPrincipal,
                SUM(CASE WHEN a.business_type ='Cancel'    AND a.money_type='CancelPrincipal'        THEN COALESCE(a.amount,0.0) ELSE 0.0 END) AS CancelPrincipal,
                SUM(CASE WHEN a.business_type ='Refund'    AND a.money_type='ReverseUnpaidPrincipal' THEN COALESCE(a.amount,0.0) ELSE 0.0 END) AS ReverseUnpaidPrincipal
           FROM (SELECT * FROM odm.ODM_FI_INVESTOR_FLOW_S_D A 
                  WHERE A.DT = '$TX_DATE' 
                        and TO_DATE(a.create_time) BETWEEN '2018-08-01' AND A.DT ) A
          WHERE a.business_type IN ('Interest','Repayment','Refund','Cancel','Loan','Reverse')
                AND a.investor_id  IN ('inv_sdtoxd_shyh','inv_sdtoxd_xian','inv_sdtoxd_gsbank', 'inv_sdtoxd_cqxd','inv_sdtoxd_shyhfxmd', 'inv_sdtoxd_zyxffxmd')
          GROUP BY CASE INVESTOR_ID 
                        WHEN 'inv_sdtoxd_cqxd' THEN '602' -- 内部出资
                        ELSE '601'                        -- 外部出资
                   END
        ) B1
        ON B.ITEM_KEY = B1.ITEM_KEY
;

);


$SQL_BUFF[6]=qq(
set mapred.job.name=tmp_dmfalm_alm_rpt_01_bt_asset_s_d6;

use dmf_alm;

insert overwrite table dmf_alm.dmfalm_alm_rpt_01_bt_asset_s_d partition(dt='$TX_DATE')
SELECT etl_dt
      ,report_key       -- '报表关键字'
      ,date_id          -- '日期'
      ,operate_org_cd   -- '部门'
      ,currency_cd      -- '币种'
      ,item_key         -- '报表项关键字'
      ,sum(item_valn)   -- '数值型值'
      ,'' as item_vals  -- '字符型值'
from  dmf_tmp.tmp_dmfalm_alm_rpt_01_bt_asset_s_d
where dt = '$TX_DATE'
group by etl_dt
      ,report_key       -- '报表关键字'
      ,date_id          -- '日期'
      ,operate_org_cd   -- '部门'
      ,currency_cd      -- '币种'
      ,item_key         -- '报表项关键字'
      -- ,item_vals        -- '字符型值'
;

);

$SQL_BUFF[7]=qq(
set mapred.job.name=tmp_dmfalm_alm_rpt_01_bt_asset_s_d7;

use dmf_tmp;
drop table dmf_tmp.tmp_dmfalm_alm_rpt_01_bt_asset_s_d
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