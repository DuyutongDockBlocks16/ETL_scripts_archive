#!/usr/bin/perl
########################################################################################################################
#  Creater        :wanglixin16
#  Creation Time  :2019-11-25
#  Description    :dmfbc_alm_dm_01_bt_gb_bal_s_d 小白管报余额表 
#                 :
#  Modify By      :
#  Modify Time    : zlc  20191125
#  Modify Content :
#  Script Version :1.0.0
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

$SQL_BUFF[0]=qq(
set mapred.job.name=dmfbc_alm_dm_01_bt_gb_bal_s_d0;

insert overwrite table dmf_bc.dmfbc_alm_dm_01_bt_gb_bal_s_d  partition (dt='$TX_DATE')
select 
       t1.plat_loan_id
       ,nvl(t3.investor_id,t1.investor_id) as investor_id  -- 出资方ID
       ,t3.investor_name --出资方NAME
       ,sum(case when t1.business_type = 'Loan' and t1.money_type ='Principal' then coalesce(t1.amount,0.0) else 0.0 end)  Loan_Principal -- 本金
       ,sum(case when t1.business_type = 'Repayment' and t1.money_type ='PaidPrincipal' then coalesce(t1.amount,0.0) else 0.0 end)  as repayment_amt     --已还本金
       ,sum(case when t1.business_type = 'Refund' and t1.money_type ='ReverseUnpaidPrincipal' then coalesce(t1.amount,0.0) else 0.0 end) Refund_ReverseUnpaidPrincipal --退款未还本金 
       ,sum(case when t1.business_type = 'Reverse' and t1.money_type ='OffPrincipal' then coalesce(t1.amount,0.0) else 0.0 end)  Reverse_OffPrincipal -- 冲销本金 
       ,sum(CASE WHEN t1.business_type ='Cancel' AND t1.money_type='CancelPrincipal' THEN COALESCE(t1.amount,0.0) ELSE 0.0 END)  Cancel_CancelPrincipal --取消消费 贷款本金
       ,sum(CASE WHEN t1.business_type ='UnderRepayment' AND  t1.money_type='Principal' THEN COALESCE(t1.amount,0.0) ELSE 0.0 END) AS UnderRepayment   --兜底补偿   -- 补充
       ,t4.config_doudi     --兜底标识
       ,sum(case when t1.business_type = 'Loan' and t1.money_type ='Principal' then coalesce(t1.amount,0.0) else 0.0 end) 
        - sum(case when t1.business_type = 'Refund' and t1.money_type ='ReverseUnpaidPrincipal' then coalesce(t1.amount,0.0) else 0.0 end)
        - sum(case when t1.business_type = 'Reverse' and t1.money_type ='OffPrincipal' then coalesce(t1.amount,0.0) else 0.0 end)
        - sum(case when t1.business_type = 'Repayment' and t1.money_type ='PaidPrincipal' then coalesce(t1.amount,0.0) else 0.0 end)
        - sum(CASE WHEN t1.business_type ='Cancel' AND t1.money_type='CancelPrincipal' THEN COALESCE(t1.amount,0.0) ELSE 0.0 END)   as should_pay_amt --应还本金
       ,t5.biz_id  as biz_id
from (select * 
      from odm.ODM_AM_ABS_INVESTOR_FLOW_0000_S_D----from odm.ODM_FI_INVESTOR_FLOW_S_D
      where dt='$TX_DATE'
          and plat_id in (select plat_id from dmf_dim.DMFDIM_OAR_DIM_CFS_PLAT_ID_I_D where dt = '$TX_DATE' and type =2 ) -- 19年0904新增
      ) t1 
left join (select * from odm.ODM_AM_ABS_INVESTOR_INSTALLMENT_0000_S_D -----from odm.ODM_FI_INVESTOR_INSTALLMENT_S_D
           where dt='$TX_DATE') t3 on t1.investor_installment_id = t3.id
left join
  (select * from dmf_dim.dmfdim_oar_dim_csf_investor_info_s_d where dt ='$TX_DATE') t4
on nvl(t3.investor_id,t1.investor_id) = t4.investor_id 
left join
  (select * from sdm.sdm_f02_cf_xbt_ordr_dtl_s_d where dt ='$TX_DATE') t5
on t1.plat_loan_id = t5.loan_id
where t1.business_type in ('Loan','Refund','Repayment','Cancel','UnderRepayment','ModifyNew','ModifyAbandoned')
group by t1.dt
        ,t1.plat_loan_id
        ,nvl(t3.investor_id,t1.investor_id)
        ,t3.investor_name
        ,t4.config_doudi
        ,t5.biz_id;


insert into table dmf_bc.dmfbc_alm_dm_01_bt_gb_bal_s_d  partition (dt='$TX_DATE')
select 
  t1.loanid
  ,'' as investor_id
  ,''    -- 出资方NAME
  ,null  --本金
  ,null  --已还本金
  ,null  --退款未还本金
  ,null  --冲销本金
  ,null  --取消消费 贷款本金
  ,null  --兜底补偿
  ,'兜底'    --兜底标识
  ,t3.unpayoff_prin as should_pay_amt  --应还本金
  ,t3.bizcode as biz_id
from (select *
        from odm.ODM_CF_LOAN_ORDER_S_D 
        where dt='$TX_DATE' 
        and bizcode not in ('12','13','26')
        and (inevtype ='' or inevtype =0  or inevtype  is null)--业务不接资管部分数据 
     ) t1
inner join
            (select 
                    loanid
                    ,bizcode
                    ,sum(shouldpayamount)    as unpayoff_prin  
             from odm.ODM_CF_PLAN_S_D                                                                                                               
             where dt='$TX_DATE'                                                                                                                         
             and bizcode not in ('12','13','26')
             group by 
                      loanid
                      ,bizcode
             ) t3
on t1.loanid = t3.loanid
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