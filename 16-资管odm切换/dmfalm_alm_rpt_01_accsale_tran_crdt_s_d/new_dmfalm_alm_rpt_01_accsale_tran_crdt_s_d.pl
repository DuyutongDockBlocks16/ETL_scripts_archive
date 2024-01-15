#!/usr/bin/perl
########################################################################################################################
#  Creater        :wanglixin16
#  Creation Time  :2019-04-17
#  Description    :dmfalm_alm_rpt_01_accsale_tran_crdt_s_d 赊销转信贷汇总
#                 :operate_org_cd='JDJR',京东金融; currency_cd='CNY',人民币
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
 
########################################################################################################################
# Write SQL For Your APP
sub getsql
{
    my @SQL_BUFF=();
#########################################################################################
####################################以下为SQL编辑区######################################
#########################################################################################

$SQL_BUFF[1]=qq(
set mapred.job.name=dmfalm_alm_rpt_01_accsale_tran_crdt_s_d1;
 
use dmf_tmp;
drop table if exists dmf_tmp.tmp_dmfalm_alm_rpt_01_accsale_tran_crdt_s_d;
create table if not exists dmf_tmp.tmp_dmfalm_alm_rpt_01_accsale_tran_crdt_s_d
like dmf_alm.dmfalm_alm_rpt_01_accsale_tran_crdt_s_d
;
 
);
 
$SQL_BUFF[2]=qq(
set mapred.job.name=tmp_dmfalm_alm_rpt_01_accsale_tran_crdt_s_d2;
 
use dmf_tmp;
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_accsale_tran_crdt_s_d partition(dt='$TX_DATE') 
select '$TX_DATE' as etl_date,
       nvl(l.date_id, f.date_id) as data_id,
       'JDJR' as operate_org_cd,
       'CNY' as currency_cd,
       nvl(l.loan_type, f.loan_type) as loan_type,
       f.paid_amt,
       f.cacel_amt,
       f.reverse_unpaid_amt,
       case when l.loan_type=2 then l.loan_amt+17585881
            when l.loan_type=1 then l.loan_amt+1973824277.13
         end as loan_amt,
       0,
       case when l.loan_type=2  then 17585881+nvl(l.loan_amt, 0.0) - nvl(f.paid_amt,0.0) - nvl(f.cacel_amt, 0.0) - nvl(f.reverse_unpaid_amt, 0.0)
            when l.loan_type=1  then 1973824277.13+nvl(l.loan_amt, 0.0) - nvl(f.paid_amt,0.0) - nvl(f.cacel_amt, 0.0) - nvl(f.reverse_unpaid_amt, 0.0)
        end as loan_bal
  from (select
            dt as date_id,
            case when l.INVESTOR_ID IN ('inv_sdtoxd_shyh','inv_sdtoxd_xian','inv_sdtoxd_gsbank','inv_sdtoxd_shyhfxmd', 'inv_sdtoxd_zyxffxmd') then '1'
                 when l.INVESTOR_ID IN ('inv_sdtoxd_cqxd') then '2'
                 else null
            end as loan_type,
            SUM(loan_amount)AS loan_amt
        FROM
            ODM.ODM_AM_ABS_INVESTOR_LOAN_0000_S_D l ---ODM.ODM_FI_INVESTOR_LOAN_S_D l
        WHERE
            l.DT='$TX_DATE' and to_date(PLAT_CREATE_TIME) >= '2018-08-01' and  to_date(PLAT_CREATE_TIME)<=dt and
            status in('Success','Cancel') and 
            l.INVESTOR_ID IN ('inv_sdtoxd_shyh','inv_sdtoxd_xian','inv_sdtoxd_gsbank', 'inv_sdtoxd_cqxd','inv_sdtoxd_shyhfxmd', 'inv_sdtoxd_zyxffxmd')
        group by dt,
            case when l.INVESTOR_ID IN ('inv_sdtoxd_shyh','inv_sdtoxd_xian','inv_sdtoxd_gsbank','inv_sdtoxd_shyhfxmd', 'inv_sdtoxd_zyxffxmd') then '1'
                 when l.INVESTOR_ID IN ('inv_sdtoxd_cqxd') then '2'
                 else null
            END
        ) l
  full join 
       (select
            dt as date_id,
            case when a.INVESTOR_ID IN ('inv_sdtoxd_shyh','inv_sdtoxd_xian','inv_sdtoxd_gsbank','inv_sdtoxd_shyhfxmd', 'inv_sdtoxd_zyxffxmd') then '1'
                 when a.INVESTOR_ID IN ('inv_sdtoxd_cqxd') then '2'
                 else null
            end as loan_type,
            
            SUM(CASE WHEN a.business_type ='Repayment' AND a.money_type='PaidPrincipal'          THEN NVL(a.amount,0.0) ELSE 0.0 END) AS paid_amt,
            SUM(CASE WHEN a.business_type ='Cancel'    AND a.money_type='CancelPrincipal'        THEN NVL(a.amount,0.0) ELSE 0.0 END) AS cacel_amt,
            SUM(CASE WHEN a.business_type ='Refund'    AND a.money_type='ReverseUnpaidPrincipal' THEN NVL(a.amount,0.0) ELSE 0.0 END) AS reverse_unpaid_amt
        FROM
            ODM.ODM_AM_ABS_INVESTOR_FLOW_0000_S_D A ----ODM.ODM_FI_INVESTOR_FLOW_S_D A
        WHERE
            A.DT='$TX_DATE' and to_date(create_time) >= '2018-08-01' and  to_date(create_time)<=dt and 
            a.business_type IN ('Interest','Repayment','Refund','Cancel','Loan','Reverse') and 
            a.INVESTOR_ID IN ('inv_sdtoxd_shyh','inv_sdtoxd_xian','inv_sdtoxd_gsbank', 'inv_sdtoxd_cqxd','inv_sdtoxd_shyhfxmd', 'inv_sdtoxd_zyxffxmd')
        group by dt,
            case when a.INVESTOR_ID IN ('inv_sdtoxd_shyh','inv_sdtoxd_xian','inv_sdtoxd_gsbank','inv_sdtoxd_shyhfxmd', 'inv_sdtoxd_zyxffxmd') then '1'
                 when a.INVESTOR_ID IN ('inv_sdtoxd_cqxd') then '2'
                 else null
            END
        ) f
        on l.date_id = f.date_id and l.loan_type = f.loan_type
;
 
);
 
$SQL_BUFF[3]=qq(
set mapred.job.name=tmp_dmfalm_alm_rpt_01_accsale_tran_crdt_s_d9;
 
use dmf_alm;
insert overwrite table dmf_alm.dmfalm_alm_rpt_01_accsale_tran_crdt_s_d partition(dt='$TX_DATE') 
select etl_dt
      ,date_id            -- '日期'
      ,operate_org_cd     -- '部门'
      ,currency_cd        -- '币种'
      ,loan_type          -- '分类:1-外部,2-内部'
      ,paid_amt           -- '已还本金'
      ,cacel_amt          -- '取消消费-贷款本金'
      ,reverse_unpaid_amt -- '冲销未还本金'
      ,loan_amt           -- '贷款本金'
      ,undertake_amt      -- '兜底本金'
      ,net_amt            --'净增金额'
from  dmf_tmp.tmp_dmfalm_alm_rpt_01_accsale_tran_crdt_s_d
where dt='$TX_DATE'
;
 
);
 
$SQL_BUFF[4]=qq(
set mapred.job.name=tmp_dmfalm_alm_rpt_01_accsale_tran_crdt_s_d10;
 
use dmf_tmp;
drop table dmf_tmp.tmp_dmfalm_alm_rpt_01_accsale_tran_crdt_s_d
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
