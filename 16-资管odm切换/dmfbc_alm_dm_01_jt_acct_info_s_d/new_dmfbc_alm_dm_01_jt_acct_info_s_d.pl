#!/usr/bin/perl
########################################################################################################################
#  Creater        :wangli550
#  Creation Time  :2018-03-19
#  Description    :dmfbc_alm_dm_01_jt_acct_info_s_d 金条账户信息
#  Modify By      :
#  Modify Time    :20190318 zlc  修改逾期标识，当天到期，但是余额还不为0的也算逾期
#                  20190814  zlc   1.确认金条分期单的余额剔除了取消消费和冲销未还
#                                  2.关联资管系统贷款单表和出资方配置表，取到出资方、资方类型、兜底比例
#                                  3.修改待还本金的逻辑，改为资管系统贷款单表中有就从这儿出，分期单出
#                                  4.平台号改为从函数中出，不再手工维护平台号
#                                  5.资管的贷款单表和资方配置表关联从只用investor_id 关联 改为用investor_id和plat_id关联
#                                  6.加字段利息还款次数、本金还款次数
#                                  7.加字段investor_name和plat_id
#                 20190902 zlc     sdm_f02_cf_jt_ordr_dtl_s_d表中json串中存在统一个资方id，但资方名称不一样，因此investor_name改为从资方配置表中出\
#                 20190905 zlc     修改兜底比例的取数逻辑，且给不在资方系统的数据给默认值100
#                 20200303 zlc     打是否abs出表的标，加资方的固定收益率、固收率、按比分成比例
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
set mapred.job.name=dmfbc_alm_dm_01_jt_acct_info_s_d1;

use dmf_tmp;
drop table dmf_tmp.tmp_dmfbc_alm_dm_01_jt_acct_info_s_d_01;
create table dmf_tmp.tmp_dmfbc_alm_dm_01_jt_acct_info_s_d_01
as
select loanid
       ,max(case when substr(limitpaydate,1,10)<='$TX_DATE' and PAYAMOUNT<>0 then LIMITPAYDATE
                         else null end) as Last_PMT_AMT_Dt  --上次支付本金日        
       ,max(case when substr(limitpaydate,1,10)<='$TX_DATE' and (nvl(PAYEDOVERAMOUNT,0)+nvl(PAYEDDAYAMOUNT,0))<>0 then LIMITPAYDATE
                         else null end) as Last_PMT_Inst_Dt --上次支付利息日
       ,max(case when substr(limitpaydate,1,10)<='$TX_DATE'  and PAYEDPLANFEE<>0 then LIMITPAYDATE
                         else null end) as Last_PMT_FEE_Dt  --上次支付手续费日
       ,min(case when substr(limitpaydate,1,10)>'$TX_DATE' and AMOUNT<>0 then LIMITPAYDATE
                         else null end) as Next_PMT_AMT_Dt  --下次支付本金日
       ,min(case when substr(limitpaydate,1,10)>'$TX_DATE' and AMOUNT<>0 AND PLANFEE=0 then LIMITPAYDATE
                        else null end) as Next_PMT_Inst_Dt  --下次支付利息日  
       ,min(case when substr(limitpaydate,1,10)>'$TX_DATE' and PLANFEE<>0 then LIMITPAYDATE
                        else null end) as Next_PMT_FEE_Dt --下次支付手续费日
from odm.ODM_CF_JT_PLAN_0000_S_D
where dt='$TX_DATE'
      and yn=1
group by loanid;

use dmf_tmp;
drop table dmf_tmp.tmp_dmfbc_alm_dm_01_jt_acct_info_s_d_02;
create table dmf_tmp.tmp_dmfbc_alm_dm_01_jt_acct_info_s_d_02
as
select t.loanid
      ,t1.LAST_Pay_Amt  --上次支付金额
      ,t1.Last_PMT_AMT_Dt --上次支付本金日
      ,t2.LAST_Pay_Fee  --上次支付手续费
      ,t2.Last_PMT_FEE_Dt  --上次支付手续费日
      ,t3.LAST_Pay_Inst --上次支付利息
      ,t3.Last_PMT_Inst_Dt --上次支付利息日
from dmf_tmp.tmp_dmfbc_alm_dm_01_jt_acct_info_s_d_01 t
left join (
      select a.loanid
            ,PAYAMOUNT as  LAST_Pay_Amt  --上次支付金额
            ,b.Last_PMT_AMT_Dt --上次支付本金日
       from odm.ODM_CF_JT_PLAN_0000_S_D a
       inner join dmf_tmp.tmp_dmfbc_alm_dm_01_jt_acct_info_s_d_01 b
         on a.loanid = b.loanid
            and limitpaydate=b.Last_PMT_AMT_Dt
       where dt='$TX_DATE'
             and yn=1
             and substr(limitpaydate,1,10)<='$TX_DATE'
             and PAYAMOUNT<>0
          )t1
  on t.loanid = t1.loanid
left join (
      select a.loanid
            ,PAYEDPLANFEE  as  LAST_Pay_Fee  --上次支付手续费
            ,b.Last_PMT_FEE_Dt  --上次支付手续费日
       from odm.ODM_CF_JT_PLAN_0000_S_D a
       inner join dmf_tmp.tmp_dmfbc_alm_dm_01_jt_acct_info_s_d_01 b
         on a.loanid = b.loanid
            and limitpaydate=b.Last_PMT_FEE_Dt
       where dt='$TX_DATE'
             and yn=1
             and substr(limitpaydate,1,10)<='$TX_DATE'
             and PAYEDPLANFEE<>0
          )t2
  on t.loanid = t2.loanid 
left join (
      select a.loanid
            ,nvl(PAYEDOVERAMOUNT,0)+nvl(PAYEDDAYAMOUNT,0)  as  LAST_Pay_Inst --上次支付利息
            ,b.Last_PMT_Inst_Dt --上次支付利息日
       from odm.ODM_CF_JT_PLAN_0000_S_D a
       inner join dmf_tmp.tmp_dmfbc_alm_dm_01_jt_acct_info_s_d_01 b
         on a.loanid = b.loanid
            and limitpaydate=b.Last_PMT_Inst_Dt
       where dt='$TX_DATE'
             and yn=1
             and substr(limitpaydate,1,10)<='$TX_DATE'
             and (PAYEDDAYAMOUNT<>0 or PAYEDOVERAMOUNT<>0)
          )t3
  on t.loanid = t3.loanid;

use dmf_tmp;
drop table dmf_tmp.tmp_dmfbc_alm_dm_01_jt_acct_info_s_d_03;
create table dmf_tmp.tmp_dmfbc_alm_dm_01_jt_acct_info_s_d_03
as
select t.loanid
      ,t1.Next_Pay_Amt  --下次支付金额
      ,t1.Next_PMT_AMT_Dt  --下次支付本金日
      ,t2.Next_Pay_Fee  --下次支付手续费
      ,t2.Next_PMT_FEE_Dt --下次支付手续费日
      ,t3.Next_Pay_Inst --下次支付利息
      ,t3.Next_PMT_Inst_Dt  --下次支付利息日
from dmf_tmp.tmp_dmfbc_alm_dm_01_jt_acct_info_s_d_01 t
left join (
      select a.loanid
            ,SHOULDPAYAMOUNT as  Next_Pay_Amt  --下次支付金额
            ,Next_PMT_AMT_Dt  --下次支付本金日
       from odm.ODM_CF_JT_PLAN_0000_S_D a
       inner join dmf_tmp.tmp_dmfbc_alm_dm_01_jt_acct_info_s_d_01 b
         on a.loanid = b.loanid
            and limitpaydate=b.Next_PMT_AMT_Dt  --下次支付本金日
       where dt='$TX_DATE'
             and yn=1
             and AMOUNT<>0
             and substr(limitpaydate,1,10)>'$TX_DATE'
          )t1
  on t.loanid = t1.loanid
left join (
      select a.loanid
            ,PLANFEE as  Next_Pay_Fee  --下次支付手续费
            ,Next_PMT_FEE_Dt --下次支付手续费日
       from odm.ODM_CF_JT_PLAN_0000_S_D a
       inner join dmf_tmp.tmp_dmfbc_alm_dm_01_jt_acct_info_s_d_01 b
         on a.loanid = b.loanid
            and limitpaydate=b.Next_PMT_FEE_Dt
       where dt='$TX_DATE'
             and yn=1
             and substr(limitpaydate,1,10)>'$TX_DATE' 
             and PLANFEE<>0
          )t2
  on t.loanid = t2.loanid 
left join (
      select a.loanid
            ,SHOULDPAYDAYAMOUNT  as  Next_Pay_Inst --下次支付利息
            ,Next_PMT_Inst_Dt  --下次支付利息日
       from odm.ODM_CF_JT_PLAN_0000_S_D a
       inner join dmf_tmp.tmp_dmfbc_alm_dm_01_jt_acct_info_s_d_01 b
         on a.loanid = b.loanid
            and limitpaydate=b.Next_PMT_Inst_Dt
       where dt='$TX_DATE'
             and yn=1
             and substr(limitpaydate,1,10)>'$TX_DATE'
             and AMOUNT<>0 
             AND PLANFEE=0
          )t3
  on t.loanid = t3.loanid;

use dmf_tmp;
drop table IF EXISTS dmf_tmp.tmp_dmfbc_alm_dm_01_jt_acct_info_s_d_04;
create table dmf_tmp.tmp_dmfbc_alm_dm_01_jt_acct_info_s_d_04 as
select
 dt
,investor_id
,Investor_type
,INVESTOR_NAME
,config_doudi
from
(select
dt
,investor_id
,Investor_type
,INVESTOR_NAME
,case when investor_id in ('inv_zrxt6') then '100' -- 潍坊银行资方
when investor_id in ('inv_bhxt2') then '0' -- 金条渤海信托2期
when investor_id in ('inv_jt_bhxt5') then '0' -- 渤海信托5
when dt<'2019-04-17' and investor_id in ('inv_jt_bhxt3') then '100' -- 渤海信托3
when dt>='2019-04-17' and investor_id in ('inv_jt_bhxt3') then '0'
when dt<='2019-06-11' and investor_id in ('inv_qx_bhxt6') then '100'-- 渤海信托6
when dt>'2019-06-11' and investor_id in ('inv_qx_bhxt6') then '0' -- 渤海信托6
when investor_id in ('inv_jt_hnxt') and dt>='2019-09-12' then '0' -- 金条高息费私募一期（华能信托?惠沣治诚1号）
when investor_id in ('inv_qx_hnxt') and dt>='2019-10-29' then '0'
when investor_id in ('inv_qx_hnxtf') and dt>='2019-11-27' then '0'
when investor_id in('inv_qx_hnxt2') and dt>='2020-01-17' then '0'
when investor_id in('inv_jt_zhxt1') and dt>='2020-01-21' then '0'
when investor_id in('inv_jt_zhxt2') and dt>='2020-01-23' then '0'
when config_1 in ('Undertake') then '100'
when config_1 in ('NonUndertake') then '0'
when config_1 is null and Investor_type in ('1','2') then '100'
when config_1 is null and Investor_type in ('0') then '0'
   end as config_doudi    --兜底比例   
from (select
dt
,investor_id
,Investor_type
,INVESTOR_NAME
,get_json_object(config,'\$.investorUndertake.undertake') as config_1
----from odm.ODM_FI_PLAT_INVESTOR_S_D t1
from odm.ODM_AM_ABS_PLAT_INVESTOR_S_D t1
where
dt ='$TX_DATE'
and plat_id='1'
) k
) t1;

);

$SQL_BUFF[2]=qq(
set mapred.job.name=dmfbc_alm_dm_01_jt_acct_info_s_d2;

use dmf_bc;
alter table dmf_bc.dmfbc_alm_dm_01_jt_acct_info_s_d drop partition (dt<='2019-12-31'); --删除19年及以前数据
insert overwrite table dmf_bc.dmfbc_alm_dm_01_jt_acct_info_s_d partition(dt='$TX_DATE')
select
JT_ORDR_DET.loan_id as  Loan_Id --金条id，主键
,null   as  Jd_Order_Id --京东订单号  
,JT_ORDR_DET.DT as  Data_Dt --数据日期(切片日期)
,'JT' as  Data_Src  --业务来源
,JT_ORDR_DET.USER_PIN as  Cust_Id --客户号
,'9630011'  Product_Cd  --产品代码
,null as  Subject_Cd  --科目代码
,'CNY'  as  Currency_Cd --币种代码
,JT_ORDR_DET.invId  Investment_Code --出资方商户号 
,JT_LOAN.merchantcode   as  Merchant_Code --商家商户号
,null   as  Main_Code --主体编号
,JT_LOAN.MARKETINGUUID  as  Market_ID --营销活动ID
,null as  App_Code  --下单渠道
,null as  Acct_Org_Cd --核算机构代码
,null as  Operate_Org_Cd  --考核机构代码
,JT_ORDR_DET.IS_FIRST as  Is_First  --是否首单
,JT_ORDR_DET.IS_PAYED_FEE as  Is_FreeCharge --是否免息订单
,CASE WHEN JT_ORDR_DET.OVRD_DAYS<>0 THEN 1 
      when substr(JT_LOAN.LOANENDDATE,1,10) = JT_LOAN.dt and JT_ORDR_DET.UNPAYOFF_PRIN > 0 
      then 1
   ELSE 0 END   as  Is_Overdue  --是否逾期
,null as  Risk_Type --风险类型
,0    as  PV_Flag --是否复利
,'CUST_TYPE'  as  Member_Type --会员类型
,CASE WHEN JT_LOAN.STATUS IN ('1','2','5' ) THEN 0 
      WHEN JT_LOAN.STATUS IN ('3','4')  THEN 2  
  ELSE 1 END   as  Status_Cd --金条状态
,JT_ORDR_DET.LOAN_TIME  as  Create_Dt --创建日期
,nvl(JT_ORDR_DET.BANK_RECV_TIME,JT_ORDR_DET.LOAN_TIME) as  Start_Dt  --起息日期
,JT_LOAN.LOANENDDATE  as  Maturity_Dt --到期日
,JT_LOAN.FINISHPAYDATE  as  finish_date --完成日期
,case when JT_ORDR_DET.OVRD_DAYS>0 then date_sub(JT_ORDR_DET.DT,cast(JT_ORDR_DET.OVRD_DAYS as int)) 
   else null end as  Overdue_Dt  --逾期日
,JT_ORDR_DET.OVRD_DAYS  as  Overdue_days  --逾期天数
,JT_ORDR_DET.LOAN_TERM  as  Original_terms  --分期数
,JT_LOAN.PAYPLANNUM as  Original_iou_terms  --已结清期数
,JT_LOAN.OVERPLANNUM    as  Over_iou_terms  --逾期期数
,CASE WHEN JT_ORDR_DET.LOANTYPEFORJX IN ('2','8') THEN  JT_ORDR_DET.LOAN_TERM 
      else datediff(JT_LOAN.LOANENDDATE,nvl(JT_ORDR_DET.BANK_RECV_TIME,JT_ORDR_DET.LOAN_TIME))   
  END   as  Term  --期限
,CASE WHEN JT_ORDR_DET.LOANTYPEFORJX IN ('2','8') THEN  'M'  
      else 'D'  
   END  as  Term_Mult --期限单位
,case when JT_LOAN.FINISHPAYDATE is null then datediff(JT_LOAN.LOANENDDATE,JT_LOAN.DT) 
  else 0 end as  Residual_Maturity --剩余期限
,'D'  as  Residual_Maturity_Mult  --剩余期限单位
,1    as  Accr_Basis_Cd --计息基础
,JT_ORDR_DET.LOAN_PRIN  as  Order_Amt --订单金额
,JT_ORDR_DET.RECVBL_STAG_FEE  as  Order_Fee_Amt --总手续费
,0  as  Order_Ser_Amt --总服务费
,JT_ORDR_DET.DAY_INT
 -JT_ORDR_DET.DISCOUNT_DAY_INT
 +JT_ORDR_DET.PNSH_INT  as  Order_Inst_Amt  --总利息
,JT_ORDR_DET.PNSH_INT as  Order_Due_Inst  --总逾期利息
,0  as  Order_Due_PV_Inst --总逾期罚息利息
,JT_ORDR_DET.LOAN_PRIN-JT_ORDR_DET.UNPAYOFF_PRIN  as  Real_Pay_Amt  --实收本金
,JT_PLAN.Real_Ovr_Amt --实收逾期本金
,JT_ORDR_DET.PAYED_STAG_FEE as  Real_Fee_Amt  --实收手续费
,0  as  Real_Ser_Amt  --实收服务费
,JT_ORDR_DET.REPAY_OVRD_INT +JT_PLAN.PAYEDDAYAMOUNT as  Real_Pay_Inst --实收利息
,JT_ORDR_DET.REPAY_OVRD_INT as  Real_Pay_Due_Inst --实收逾期利息
,0  as  Real_Pay_Due_PV_Inst  --实收逾期罚息利息
,JT_ORDR_DET.UNPAYOFF_PRIN*amountRate/100  as  Should_Pay_Amt  --待还本金
,CASE WHEN JT_ORDR_DET.OVRD_DAYS<>0 THEN JT_ORDR_DET.UNPAYOFF_PRIN 
   ELSE 0 END as  Should_Ovr_Amt  --待还逾期本金
,JT_ORDR_DET.RECVBL_STAG_FEE-JT_ORDR_DET.PAYED_STAG_FEE as  Should_Pay_Fee  --待还手续费
,0  as  Should_Pay_Ser  --待还服务费
,JT_ORDR_DET.DAY_INT-JT_ORDR_DET.DISCOUNT_DAY_INT-JT_PLAN.PAYEDDAYAMOUNT
  +(JT_ORDR_DET.PNSH_INT-JT_ORDR_DET.REPAY_OVRD_INT) as Should_Pay_Inst --待还利息
,JT_ORDR_DET.PNSH_INT-JT_ORDR_DET.REPAY_OVRD_INT  as  Should_Pay_Due_Inst --待还逾期利息
,0  as  Should_Pay_Due_PV_Inst  --待还逾期罚息利息
,JT_ORDR_DET.STAG_FEE as  Old_Plan_Fee  --原始手续费  
,0  as  Old_Plan_Svc  --原始服务费
,JT_ORDR_DET.DAY_INT  as  Old_Plan_Inst --原始利息
,CASE WHEN JT_LOAN.LOANTYPEFORJX='2' THEN  JT_LOAN.DISCOUNT 
  ELSE NULL END as  Market_Plan_Fee_Dis --营销手续费优惠折扣
,CASE WHEN JT_LOAN.LOANTYPEFORJX='2' THEN  JT_LOAN.DISCOUNTSTARTDATE  
 ELSE NULL END Market_Fee_Start_DATE --营销手续费优惠起始日期
,CASE WHEN JT_LOAN.LOANTYPEFORJX='2' THEN  JT_LOAN.DISCOUNTENDDATE  
  ELSE NULL END as  Market_Fee_End_DATE --营销手续费优惠结束日期
,CASE WHEN JT_LOAN.LOANTYPEFORJX='2' THEN  JT_LOAN.dayamountmktdiscount  
  ELSE NULL END as Market_Plan_Fee --营销优惠日手续费
,null as  Fomer_Plan_Fee  --商家补贴日手续费
,null as  Market_Plan_Ser_Dis --营销服务费优惠折扣
,null as  Market_Ser_Start_DATE --营销服务费优惠起始日期
,null as  Market_Ser_End_DATE --营销服务费优惠结束日期
,null as  Market_Plan_Ser --营销优惠日服务费
,null as  Fomer_Plan_Ser  --商家补贴日服务费
,CASE WHEN JT_LOAN.LOANTYPEFORJX='1' THEN  JT_LOAN.DISCOUNT 
  ELSE NULL END as  Market_Plan_Inst_Dis  --营销利息优惠折扣
,CASE WHEN JT_LOAN.LOANTYPEFORJX='1' THEN  JT_LOAN.DISCOUNTSTARTDATE  
  ELSE NULL END as  Market_Plan_Inst_Start_DATE --营销利息优惠起始日期
,CASE WHEN JT_LOAN.LOANTYPEFORJX='1' THEN  JT_LOAN.DISCOUNTENDDATE  
 ELSE NULL END as  Market_Plan_Inst_End_DATE --营销利息优惠结束日期 
,CASE WHEN JT_LOAN.LOANTYPEFORJX='1' THEN  JT_LOAN.dayamountmktdiscount  
 ELSE NULL END as  Market_Plan_Inst  --营销优惠日利息
,null as  Fomer_Plan_Inst --商家补贴日利息
,null as  Fomer_Plan_Flag --商家补贴模式
,JT_ORDR_DET.PLANFEERATE*100  as  Fee_Rate  --手续费率(%)
--,DWD_FI_INVESTOR_LOAN_S_D.INCOME_RATIO_INTEREST   as  Fee_Plat_Proportion --手续费平台分成比例 ???
,null as  Fee_Plat_Proportion
,0  as  Fee_Fomer_Proportion  --手续费商家分成比例
--,DWD_FI_INVESTOR_LOAN_S_D.INCOME_RATIO_INTEREST   as  Fee_Investmen_Proportion  --手续费投资方分成比例 ???
,null as  Fee_Investmen_Proportion
,null as  Service_Rate  --服务费率
,null as  Service_Plat_Proportion --服务费平台分成比例  
,null as  Service_Fomer_Proportion  --服务费商家分成比例
,null as  Service_Investmen_Proportion  --服务费投资方分成比例  
,JT_ORDR_DET.ACTL_FEE_RATE*360*100  as  Inst_Rate --利息率
--,DWD_FI_INVESTOR_LOAN_S_D.INCOME_RATIO_INTEREST   as  Inst_Plat_Proportion  --利息平台分成比例 ???  
,null as  Inst_Plat_Proportion
,0  as  Inst_Fomer_Proportion --利息商家分成比例
--,DWD_FI_INVESTOR_LOAN_S_D.INCOME_RATIO_INTEREST   as  Inst_Investmen_Proportion --利息投资方分成比例 ???
,null as  Inst_Investmen_Proportion
,JT_PLAN.Due_Rate --逾期利率(%)
--,DWD_FI_INVESTOR_LOAN_S_D.INCOME_RATIO_OVERDUE  as  Inst_Plat_Proportion  --逾期利息平台分成比例 ???
,null  as  Inst_Plat_Overdue_Proportion
,0  as  Inst_Fomer_Overdue_Proportion --逾期利息商家分成比例
--,DWD_FI_INVESTOR_LOAN_S_D.INCOME_RATIO_OVERDUE  as  Inst_Investmen_Proportion --逾期利息投资方分成比例 ???
,null as  Inst_Investmen_Overdue_Proportion
,null as  Intst_Rate_Offset_Val --利率浮动值
,null as  Intst_Rate_Offset_Tp  --利率浮动方式
,CASE WHEN JT_ORDR_DET.LOANTYPEFORJX='1'  AND JT_ORDR_DET.LOAN_TERM=1  THEN 1   
      WHEN JT_ORDR_DET.LOANTYPEFORJX='1'  AND JT_ORDR_DET.LOAN_TERM<>1  THEN 3 
      WHEN JT_ORDR_DET.LOANTYPEFORJX IN ('2','4') THEN 8 
      WHEN JT_ORDR_DET.LOANTYPEFORJX ='8' THEN 4
  END   as  PMT_Type  --还款方式
,11 as  Intst_Rate_Adjst_Tp --利率调整方式
,CASE WHEN JT_ORDR_DET.LOANTYPEFORJX IN ('2','8') THEN  JT_ORDR_DET.LOAN_TERM
      WHEN JT_ORDR_DET.LOANTYPEFORJX='4' THEN JT_ORDR_DET.LOAN_TERM *7  
 ELSE JT_ORDR_DET.LOAN_TERM END   as  Repricing_Freq  --重定价频率
,CASE WHEN JT_ORDR_DET.LOANTYPEFORJX IN ('2','8') THEN  'M'
      ELSE 'D'  END  as  Repricing_Freq_Mult --重定价频率单位
,JT_LOAN.PLANNUM  as  PMT_COUNT --支付次数
,CASE WHEN JT_ORDR_DET.LOANTYPEFORJX IN ('1','2','8') THEN   1 
      WHEN JT_ORDR_DET.LOANTYPEFORJX='4' THEN 7  
  END   as  PMT_Freq  --支付频率
,CASE WHEN JT_ORDR_DET.LOANTYPEFORJX IN ('1','2','8') THEN  'M'
      WHEN JT_ORDR_DET.LOANTYPEFORJX='4' THEN 'D'  
  END   as  PMT_Freq_Mult --支付频率单位
,tb1.Last_PMT_AMT_Dt  --上次支付本金日
,tb1.Last_PMT_Inst_Dt --上次支付利息日
,null as Last_PMT_Srv_Dt  --上次支付服务费日
,tb1.Last_PMT_FEE_Dt  --上次支付手续费日
,tb2.Next_PMT_AMT_Dt  --下次支付本金日
,tb2.Next_PMT_Inst_Dt --下次支付利息日
,null as Next_PMT_Srv_Dt  --下次支付服务费日
,tb2.Next_PMT_FEE_Dt  --下次支付手续费日
,nvl(JT_ORDR_DET.BANK_RECV_TIME,JT_ORDR_DET.LOAN_TIME) as  Last_Repricing_Dt --上次重定价日
,JT_LOAN.LOANENDDATE  as  Next_Repricing_Dt --下次重定价日
,JT_ORDR_DET.LOAN_PRIN  as  Last_Repricing_Amt  --上次重定价金额
--,JT_PLAN.First_PMT_Dt --首次支付日
,JT_PLAN.First_PMT_AMT_Dt --首次本金支付日
,JT_PLAN.First_PMT_INST_Dt  --首次利息支付日
,NVL(JT_ORDR_DET.BANK_RECV_TIME,JT_ORDR_DET.LOAN_TIME) as First_PMT_SRV_Dt --首次服务费支付日
,JT_PLAN.First_PMT_FEE_Dt --首次手续费支付日
,JT_PLAN.First_PMT_AMT  --首次支付金额
,JT_PLAN.First_PMT_Fee  --首次支付手续费
,0  as  First_PMT_Srv --首次支付服务费
,JT_PLAN.First_PMT_Inst --首次支付利息
,tb1.LAST_Pay_Amt --上次支付金额
,tb1.LAST_Pay_Fee   --上次支付手续费
,0  as  LAST_Pay_Srv  --上次支付服务费
,tb1.LAST_Pay_Inst  --上次支付利息
,tb2.Next_Pay_Amt --下次支付金额
,tb2.Next_Pay_Fee --下次支付手续费
,0  as  Next_Pay_Srv  --下次支付服务费
,tb2.Next_Pay_Inst  --下次支付利息
,tb4.Accr_Fee_Amt/30 as  Day_Accr_Fee_Amt  --当日计提手续费
,0  as  Day_Accr_Ser_Amt  --当日计提服务费
,JT_ORDR_DET.DAY_INT+JT_ORDR_DET.PNSH_INT
  -tb3.DAY_INT-tb3.PNSH_INT  as  Day_Accr_Intst_Amt  --当日计提利息
,JT_ORDR_DET.PNSH_INT-tb3.PNSH_INT  as  Day_Accr_Due_Intst_Amt  --当日计提逾期利息
,0  as  Day_Accr_Due_PV_Amt --当日计提逾期利息罚息
,0 as  ABS_ID  --有ABS编号改为 是否ABS出表 1:出表,0:不出表   
--,DWD_FI_INVESTOR_LOAN_S_D.INVESTOR_ID as  UNION_LOAN_ID --对应联合贷编号
,null as  UNION_LOAN_ID --对应联合贷编号
,JT_PLAN.writeoffstatus  as    writeoffstatus    --坏账核销状态：0-否 1-是 
,concat(JT_LOAN.sourcecode,JT_LOAN.sourcelink)   as source   --来源代码 +	渠道号
,DIM_INVESTMEN.investor_type         as investor_type  --资方类型： 1：内部资方、0：外部资方
,case when DIM_INVESTMEN.INVESTOR_ID is null then 100 else DIM_INVESTMEN.config_doudi end           as Under_Ratio    --兜底比列
,CASE WHEN JT_ORDR_DET.PLANFEERATE<>0                                              
             THEN t2.repay_count2                                              
             else t2.repay_count3                                            
        end                    as  INT_PAY_CNT      --利息还款次数
,t2.repay_count4               as  PRIN_REPAY_CNT      --本金还款次数
,JT_ORDR_DET.amountRate        AS INVESTMENT_RATIO   --出资比例
,DIM_INVESTMEN.INVESTOR_NAME   as investor_name
,'1'                           as plat_id
,t5.cooperation           as cooperation        --合作方式
,t5.fixed_income_rate     as fixed_income_rate  --固定收益率
,t5.ratio_rate            as ratio_rate         --按笔分成比例
,t5.risk_selling_fixed_rate as risk_selling_fixed_rate  --固收利率
from 
(
    select t1.*,get_json_object(app,'\$.invId') as invId,get_json_object(app,'\$.invName') as invName,get_json_object(app,'\$.amountRate') as amountRate
    from
      (select
        t.*
        ,split(
        regexp_replace(
        regexp_extract(
        get_json_object(investor,"\$.[]") -- 获取data数组,格式[{json},{json}]
        ,'^\\\\\\\\[(.+)\\\\\\\\]\$'
        ,1
        ) -- 删除字符串前后的[],格式{json},{json}
        ,'\\\\\\\\}\\\\\\\\,\\\\\\\\{'
        , '\\\\\\\\}\\\\\\\\|\\\\\\\\|\\\\\\\\{'
        ) -- 将josn字符串中的分隔符代换成||,格式{json}||{json}
        ,'\\\\\\\\|\\\\\\\\|'
        ) as str -- 按||分隔符切割成一个hive数组
      from sdm.sdm_f02_cf_jt_ordr_dtl_s_d  t
      where dt='$TX_DATE' and investor is not  null 
      ) t1
      lateral view explode(t1.str) xx as app
    union all 
    select t1.*,array() as str, null as invId,null as invName,'100' as amountRate from sdm.sdm_f02_cf_jt_ordr_dtl_s_d  t1
    where dt='$TX_DATE' and investor is   null 
)JT_ORDR_DET
left join (select *
           from odm.ODM_CF_JT_LOAN_0000_S_D
           where dt='$TX_DATE'
            )JT_LOAN
  on JT_ORDR_DET.loan_id = JT_LOAN.loanid       
left join (select loanid
                 ,SUM(case when overduedays>0 then PAYAMOUNT else 0 end)  as  Real_Ovr_Amt  --实收逾期本金
                 ,SUM(PAYEDDAYAMOUNT) as PAYEDDAYAMOUNT
                 ,MAX(OVERRATE)*360*100 as  Due_Rate  --逾期利率(%)
                 ,MIN(LIMITPAYDATE) as  First_PMT_Dt  --首次支付日
                 ,MIN(case when AMOUNT<>0 then LIMITPAYDATE else null end) as First_PMT_AMT_Dt  --首次本金支付日
                 ,MIN(case when AMOUNT<>0 AND PLANFEE=0 then LIMITPAYDATE else null end) as First_PMT_INST_Dt --首次利息支付日
                 ,MIN(case when PLANFEE<>0 then LIMITPAYDATE else null end) as First_PMT_FEE_Dt --首次手续费支付日
                 ,sum(PLANFEEDISCOUNT)  as  Market_Plan_Fee --营销优惠日手续费
                 ,sum(case when curplannum=1 then AMOUNT  else 0 end) as  First_PMT_AMT --首次支付金额
                 ,sum(case when curplannum=1 then PLANFEE else 0 end) as  First_PMT_Fee --首次支付手续费
                 ,sum(case when curplannum=1 then PAYEDOVERAMOUNT+PAYEDDAYAMOUNT else 0 end)  as  First_PMT_Inst  --首次支付利息
                 ,max(case when writeoffstatus = 1 then 1 else 0 end)   as    writeoffstatus    --坏账核销状态：0-否 1-是 
           from odm.ODM_CF_JT_PLAN_0000_S_D
           where dt='$TX_DATE'
                 and yn=1
           group by loanid
            )JT_PLAN
  on JT_ORDR_DET.loan_id = JT_PLAN.loanid
left join dmf_tmp.tmp_dmfbc_alm_dm_01_jt_acct_info_s_d_02 tb1 --上次
  on JT_ORDR_DET.loan_id = tb1.loanid
left join dmf_tmp.tmp_dmfbc_alm_dm_01_jt_acct_info_s_d_03 tb2 --下次
  on JT_ORDR_DET.loan_id = tb2.loanid  
left join (select *
           from sdm.sdm_f02_cf_jt_ordr_dtl_s_d
           where dt='$TX_PREV_DATE'
             )tb3 --昨日
  on JT_ORDR_DET.loan_id = tb3.loan_id 
left join (select loanid
                 ,PLANFEE-PLANFEEDISCOUNT as Accr_Fee_Amt
           from (
                 select loanid
                       ,PLANFEE
                       ,PLANFEEDISCOUNT
                       ,row_number()over(partition by loanid order by limitpaydate desc) as rn
                 from odm.ODM_CF_JT_PLAN_0000_S_D
                 where dt='$TX_DATE'
                       and yn=1
                       and substr(limitpaydate,1,10)<='$TX_DATE'
                      )t
           where rn=1
            )tb4
  on JT_ORDR_DET.loan_id = tb4.loanid
left join dmf_tmp.tmp_dmfbc_alm_dm_01_jt_acct_info_s_d_04  DIM_INVESTMEN
on JT_ORDR_DET.invId =DIM_INVESTMEN.INVESTOR_ID 
left join (select loan_id
                  ,sum(case when PLAN_PMT_FEE <> 0 then 1 else 0 end) as repay_count2 
                  ,count(1)      as repay_count3
                  ,sum(case when PLAN_PMT_PRIN <> 0 then 1 else 0 end) as repay_count4
           from dmf_bc.dmfbc_alm_dm_01_jt_repay_plan_s_d 
           where dt ='$TX_DATE'  
           group by loan_id) t2
on JT_ORDR_DET.loan_id = t2.loan_id 
left join (select * from dmf_bc.dmfbc_alm_dm_01_investor_rate_s_d where dt ='${TX_DATE}' and business_plat_line ='金条') t5
on JT_ORDR_DET.invId = t5.INVESTOR_ID

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