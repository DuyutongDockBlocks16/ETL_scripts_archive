#!/usr/bin/perl
########################################################################################################################
#  Creater        :wanglixin16
#  Creation Time  :2019-03-04
#  Description    :dmfalm_alm_rpt_01_investor_s_d 出资方报表
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
set mapred.job.name=dmfalm_alm_rpt_01_investor_s_d1;

use dmf_tmp;
drop table if exists dmf_tmp.tmp_dmfalm_alm_rpt_01_investor_s_d;
create table if not exists dmf_tmp.tmp_dmfalm_alm_rpt_01_investor_s_d
like dmf_alm.dmfalm_alm_rpt_01_investor_s_d
;

);



$SQL_BUFF[2]=qq(
set mapred.job.name=tmp_dmfalm_alm_rpt_01_investor_s_d2;

use dmf_tmp;


insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_investor_s_d partition(dt='$TX_DATE')
select
       t1.dt as etl_dt
      ,t1.dt as date_id
      ,'JDJR' as operate_org_cd
      ,'CNY' as currency_cd
      ,investment_code
      ,t1.investor_name
      ,'金条'
      ,SUM(t1.Should_Pay_Amt) as item_valn
      ,case when under_ratio>0 then '兜底' else '非兜底' end as under_type
FROM dmf_bc.dmfbc_alm_dm_01_lhd_acct_info_s_d t1
WHERE t1.dt = '$TX_DATE' AND t1.UNION_TYPE = '1'
GROUP BY t1.dt
         ,investment_code
         ,t1.investor_name
         ,case when under_ratio>0 then '兜底' else '非兜底' end
;


-- 插入小白条&大白条
insert into table dmf_tmp.tmp_dmfalm_alm_rpt_01_investor_s_d partition(dt='$TX_DATE')
SELECT
       a.dt as etl_dt
      ,a.dt as date_id
      ,'JDJR' as operate_org_cd
      ,'CNY' as currency_cd
      ,investment_code
      ,A.investor_name
      ,'大白条&小白条'
      ,CAST(sum(A.should_pay_amt ) AS BIGINT ) AS item_valn
      ,case when under_ratio>0 then '兜底' else '非兜底' end
 FROM
      dmf_bc.dmfbc_alm_dm_01_lhd_acct_info_s_d A
  where dt='$TX_DATE'
        and union_type in ('2','3')
   group by a.dt,investment_code,A.investor_name,case when under_ratio>0 then '兜底' else '非兜底' end
;

);



$SQL_BUFF[3]=qq(
set mapred.job.name=tmp_dmfalm_alm_rpt_01_investor_s_d3;

use dmf_alm;
insert overwrite table dmf_alm.dmfalm_alm_rpt_01_investor_s_d partition(dt='$TX_DATE')
select
       etl_dt
      ,date_id
      ,operate_org_cd
      ,currency_cd
      ,investor_id
      ,investor_name
      ,investor_type
      ,item_valn
      ,under_type
from  dmf_tmp.tmp_dmfalm_alm_rpt_01_investor_s_d
where dt='$TX_DATE'
;

);

$SQL_BUFF[4]=qq(
set mapred.job.name=tmp_dmfalm_alm_rpt_01_investor_s_d4;

use dmf_tmp;
drop table dmf_tmp.tmp_dmfalm_alm_rpt_01_investor_s_d
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
