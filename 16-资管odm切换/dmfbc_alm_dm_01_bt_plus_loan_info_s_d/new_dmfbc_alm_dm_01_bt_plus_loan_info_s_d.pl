#!/usr/bin/perl
########################################################################################################################
#  Creater        :wangli550
#  Creation Time  :2018-03-19
#  Description    :dmfbc_alm_dm_01_bt_plus_loan_info_s_d 大白条账户信息
#  Modify By      :
#  Modify Time    :20190403  zlc   因为产品有空的所以产品字段暂时做了特殊处理
#                  20190701  zlc   修改逾期日期和逾期天数
#                  20190808  zlc   放开 PLANTYPE<>'2'   的限制（经分和财务集市都没有限制）
#                  20190814  zlc   1.确认大白分期单的余额剔除了取消消费和冲销未还
#                                  2.关联资管系统贷款单表和出资方配置表，取到资方类型、兜底比例、出资比例
#                                     出资方从两个出资方组合到一块儿出改为出多条数据，一条只出一个
#                                  
#                                  4.平台号改为从函数中出，不再手工维护平台号
#                                  5.资管的贷款单表和资方配置表关联从只用investor_id 关联 改为用investor_id和plat_id关联
#                                  6.加字段利息还款次数、本金还款次数
#                                  7.加字段investor_name和plat_id
#                                  8.修改兜底比列的取数逻辑（与融资系统报表的取数逻辑一致）
#                 20190902   ZLC   资方配置表在like的时候 加了upper()函数，防止大小写不匹配
#                 20190910   zlc   有新的还款方式，plantype= 4,与shicaixu沟通的结果是等额本息
#                 20190911   zlc   平台号337已维护到dmf_bc.dmdict函数中，去掉对平台号的限制
#                 20190920   zlc   有了新的平台号338，函数还没维护进去，暂时限制下338
#                 20190920   zlc   取消限制平台号338，将函数dmf_bc.dmdict替换为dmf_bc.dmdictdesc
#                 20190923   zlc   发现有贷款单有尾款的现像，尾款的还款日期和最后一期的还款日期是一样的，导致去还款次数的时候多算一次，因此
#                                  取还款次数的时候按还款日期去了一下重
#                 20190924   wanglixin ODM_FI_INVESTOR_LOAN_S_D 表新增create_date进行时间限定，否则会有T+0数据
#                 20191017   wanglixin 有三个商户号匹配不到产品编码，做下特殊处理'4890000066','5940000001','4890000065'
#                                    
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
set mapred.job.name=dmfbc_alm_dm_01_bt_plus_loan_info_s_d1;

use dmf_tmp;
drop table dmf_tmp.tmp_dmfbc_alm_dm_01_bt_plus_loan_info_s_d_01;
create table dmf_tmp.tmp_dmfbc_alm_dm_01_bt_plus_loan_info_s_d_01
as
select loanNo
      ,CASE WHEN SUM(SHOULDPAYAMOUNT)<>0 AND SUM(OVERAMOUNT)<>0  THEN 1 
            when min(case when finishpaydate is not null then '2999-12-31' else substr(limitpaydate,1,10) end ) = dt and  SUM(SHOULDPAYAMOUNT)<>0  then 1
         ELSE 0 END     AS    Is_Overdue    --是否逾期 ???
      ,CASE WHEN SUM(OVERAMOUNT)=0 AND SUM(SHOULDPAYAMOUNT)<>0 THEN 1 
            WHEN SUM(OVERAMOUNT)<>0 THEN 2
        ELSE 0  END     AS    Status_Cd    --白条状态
      ,MIN(STARTPLANDATE)    AS    Start_Dt    --起息日期
      ,MAX(LIMITPAYDATE)     AS    Maturity_Dt --到期日
      ,CASE WHEN SUM(SHOULDPAYAMOUNT)=0 THEN MAX(FINISHPAYDATE) 
         END     AS    finish_date    --完成日期
      ,min(CASE WHEN SHOULDPAYAMOUNT>0 AND to_date(limitpaydate)<dt THEN LIMITPAYDATE  
        END )    AS    Overdue_Dt    --逾期日
      ,max(case when shouldpayamount>0 and to_date(limitpaydate)<dt  then  datediff(DT,limitpaydate)  else 0 end ) 
             AS    Overdue_days    --逾期天数
      ,COUNT(case when shouldpayamount=0 then CURPLANNUM
              else null end )    AS    Original_iou_terms    --已结清期数
      ,COUNT(case when OVERAMOUNT>0 then CURPLANNUM
               else null end)    AS    Over_iou_terms    --逾期期数
      ,datediff(MAX(LIMITPAYDATE),MIN(STARTPLANDATE))    AS    Term    --期限
      ,MAX(datediff(LIMITPAYDATE,DT))    AS    Residual_Maturity   --剩余期限
      ,SUM(PLANFEE)+SUM(PAYEDPLANFEE)    AS    Order_Fee_Amt    --总手续费
      ,SUM(INTERESTAMOUNT)+SUM(OVERAMOUNT)
       +SUM(INTERESTPAYEDAMOUNT)+SUM(OVERPAYAMOUNT)     AS    Order_Inst_Amt    --总利息
      ,SUM(OVERAMOUNT)+SUM(OVERPAYAMOUNT)    AS    Order_Due_Inst    --总逾期利息
      ,SUM(PAYAMOUNT)    AS    Real_Pay_Amt    --实收本金 ???
      ,SUM(case when OVERAMOUNT>0 then PAYAMOUNT
            else 0 end)    AS    Real_Ovr_Amt    --实收逾期本金 ???
      ,SUM(PAYEDPLANFEE)    AS    Real_Fee_Amt    --实收手续费
      ,SUM(OVERPAYAMOUNT)    AS    Real_Pay_Inst    --实收利息 ???
      ,SUM(OVERPAYAMOUNT)    AS    Real_Pay_Due_Inst    --实收逾期利息 ???
      ,SUM(SHOULDPAYAMOUNT)    AS    Should_Pay_Amt    --待还本金
      ,SUM(case when OVERAMOUNT>0 then SHOULDPAYAMOUNT
            else 0 end)    AS    Should_Ovr_Amt    --待还逾期本金
      ,SUM(PLANFEE)    AS    Should_Pay_Fee    --待还手续费
      ,SUM(INTERESTAMOUNT)+SUM(OVERAMOUNT)    AS    Should_Pay_Inst    --待还利息
      ,SUM(OVERAMOUNT)    AS    Should_Pay_Due_Inst    --待还逾期利息
      ,SUM(PLANFEE)+SUM(PAYEDPLANFEE)+SUM(SPECIALAMOUNT)    AS    Old_Plan_Fee    --原始手续费  
      ,SUM(INTERESTAMOUNT)+SUM(INTERESTPAYEDAMOUNT)   AS    Old_Plan_Inst    --原始利息
      ,MAX(BEFOREPLANRATE)*100    AS    Old_Plan_Fee_Rate    --原始手续费率
      ,MIN(case when specialamount<>0 then LIMITPAYDATE
            else null end)    AS    Market_Fee_Start_DATE    --营销手续费优惠起始日期
      ,MAX(case when specialamount<>0 then LIMITPAYDATE
            else null end)    AS    Market_Fee_End_DATE    --营销手续费优惠结束日期
      ,SUM(SPECIALLOANAMOUNT)    AS    Market_Plan_Fee    --营销优惠手续费
      ,SUM(SPECIALLOANAMOUNT)    AS    Fomer_Plan_Fee    --商家补贴手续费
      ,MAX(PLANRATE)    AS    Fee_Rate    --手续费率(%)
     ,datediff(MAX(LIMITPAYDATE),MIN(STARTPLANDATE))    AS    Repricing_Freq    --重定价频率
     ,MIN(STARTPLANDATE)    AS    Last_Repricing_Dt    --上次重定价日
     ,MAX(LIMITPAYDATE)    AS    Next_Repricing_Dt    --下次重定价日
     ,MIN(LIMITPAYDATE)    AS    First_PMT_Dt    --首次支付日
     ,MAX(INTERESTRATE)   AS INTERESTRATE --执行利率
     ,MAX(OVERRATE) AS OVERRATE   --逾期利率
     --,MAX(case when curplannum = 1 then AMOUNT else 0 end  )  AS    First_PMT_AMT    --首次支付金额
     --,MAX(case when curplannum = 1 then PLANFEE+SPECIALAMOUNT else 0 end)    AS    First_PMT_Fee    --首次支付手续费
     ,max(case when PAYAMOUNT<>0 and substr(LIMITPAYDATE,1,10)<='$TX_DATE' then LIMITPAYDATE
           else null end) as Last_PMT_AMT_Dt  --上次支付本金日
       ,max(case when INTERESTPAYEDAMOUNT<>0 and substr(LIMITPAYDATE,1,10)<='$TX_DATE' then LIMITPAYDATE
           else null end) as Last_PMT_Inst_Dt --上次支付利息日
       --,max(case when substr(LIMITPAYDATE,1,10)<='$TX_DATE' then LIMITPAYDATE
       --    else null end) as Last_PMT_Srv_Dt  --上次支付服务费日
       ,max(case when PAYEDPLANFEE<>0 and substr(LIMITPAYDATE,1,10)<='$TX_DATE' then LIMITPAYDATE
           else null end) as Last_PMT_FEE_Dt  --上次支付手续费日
       ,min(case when AMOUNT<>0 and substr(LIMITPAYDATE,1,10)>'$TX_DATE' then LIMITPAYDATE
           else null end) as Next_PMT_AMT_Dt  --下次支付本金日
       ,min(case when (INTERESTAMOUNT<>0 or OVERAMOUNT<>0) and substr(LIMITPAYDATE,1,10)>'$TX_DATE' then LIMITPAYDATE
           else null end) as Next_PMT_Inst_Dt --下次支付利息日
       --,min(case when substr(LIMITPAYDATE,1,10)>'$TX_DATE' then LIMITPAYDATE
       --    else null end) as Next_PMT_Srv_Dt  --下次支付服务费日
       ,min(case when PLANFEE<>0 and substr(LIMITPAYDATE,1,10)>'$TX_DATE' then LIMITPAYDATE
           else null end) as Next_PMT_FEE_Dt  --下次支付手续费日
       ,min(case when amount<>0 then LIMITPAYDATE
           else null end) as First_PMT_AMT_Dt --首次本金支付日
       ,min(case when INTERESTPAYEDAMOUNT+INTERESTAMOUNT<>0 then LIMITPAYDATE
           else null end) as First_PMT_INST_Dt  --首次利息支付日
       ,min(STARTPLANDATE) as First_PMT_SRV_Dt  --首次服务费支付日
       ,min(case when planfee<>0 OR PAYEDPLANFEE<>0 then LIMITPAYDATE
           else null end) as First_PMT_FEE_Dt --首次手续费支付日
FROM odm.ODM_CF_PLUS_PLAN_S_D
WHERE DT='$TX_DATE'
GROUP BY loanNo,dt;

use dmf_tmp;
drop table dmf_tmp.tmp_dmfbc_alm_dm_01_bt_plus_loan_info_s_d_02;
create table dmf_tmp.tmp_dmfbc_alm_dm_01_bt_plus_loan_info_s_d_02
as
select t.loanNo
      ,t1.LAST_Pay_Amt --上次支付金额
      ,t1.Last_PMT_AMT_Dt  --上次支付本金日
      ,t2.LAST_Pay_Fee --上次支付手续费
      ,t2.Last_PMT_FEE_Dt  --上次支付手续费日
      ,t3.LAST_Pay_Inst --上次支付利息
      ,t3.Last_PMT_Inst_Dt --上次支付利息日
from dmf_tmp.tmp_dmfbc_alm_dm_01_bt_plus_loan_info_s_d_01 t
left join (select a.loanNo
                 ,sum(a.PAYAMOUNT)     as LAST_Pay_Amt --上次支付金额
                 ,b.Last_PMT_AMT_Dt  --上次支付本金日
           FROM odm.ODM_CF_PLUS_PLAN_S_D a
           inner join dmf_tmp.tmp_dmfbc_alm_dm_01_bt_plus_loan_info_s_d_01 b
             on a.loanNo = b.loanNo
                and a.LIMITPAYDATE = b.Last_PMT_AMT_Dt  --上次支付本金日
           WHERE DT='$TX_DATE'
                 and substr(LIMITPAYDATE,1,10)<='$TX_DATE'
                 and PAYAMOUNT<>0
           group by a.loanNo,b.Last_PMT_AMT_Dt )t1
 on t.loanNo = t1.loanNo
left join (select a.loanNo
                 ,sum(a.PAYEDPLANFEE)  as LAST_Pay_Fee --上次支付手续费
                 ,b.Last_PMT_FEE_Dt  --上次支付手续费日
           FROM odm.ODM_CF_PLUS_PLAN_S_D a
           inner join dmf_tmp.tmp_dmfbc_alm_dm_01_bt_plus_loan_info_s_d_01 b
             on a.loanNo = b.loanNo
                and a.LIMITPAYDATE = b.Last_PMT_FEE_Dt  --上次支付手续费日
           WHERE DT='$TX_DATE'
                 and substr(LIMITPAYDATE,1,10)<='$TX_DATE'
                 and PAYEDPLANFEE<>0
           group by a.loanNo,b.Last_PMT_FEE_Dt )t2
 on t.loanNo = t2.loanNo
left join (select a.loanNo
                 ,sum(nvl(a.INTERESTPAYEDAMOUNT,0)+nvl(a.OVERPAYAMOUNT,0)) as LAST_Pay_Inst --上次支付利息
                 ,b.Last_PMT_Inst_Dt --上次支付利息日
           FROM odm.ODM_CF_PLUS_PLAN_S_D a
           inner join dmf_tmp.tmp_dmfbc_alm_dm_01_bt_plus_loan_info_s_d_01 b
             on a.loanNo = b.loanNo
                and a.LIMITPAYDATE = b.Last_PMT_Inst_Dt --上次支付利息日
           WHERE DT='$TX_DATE'
                 and substr(LIMITPAYDATE,1,10)<='$TX_DATE'
                 and INTERESTPAYEDAMOUNT<>0
           group by a.loanNo,b.Last_PMT_Inst_Dt )t3
 on t.loanNo = t3.loanNo; 
 

use dmf_tmp;
drop table dmf_tmp.tmp_dmfbc_alm_dm_01_bt_plus_loan_info_s_d_03;
create table dmf_tmp.tmp_dmfbc_alm_dm_01_bt_plus_loan_info_s_d_03
as
select t.loanNo
       ,t1.Next_Pay_Amt --下次支付金额
       ,t1.Next_PMT_AMT_Dt  --下次支付本金日
       ,t2.Next_Pay_Fee --下次支付手续费
       ,t2.Next_PMT_FEE_Dt  --下次支付手续费日
       ,t3.Next_Pay_Inst --下次支付利息
       ,t3.Next_PMT_Inst_Dt --下次支付利息日
from dmf_tmp.tmp_dmfbc_alm_dm_01_bt_plus_loan_info_s_d_01 t
left join (select a.loanNo
                 ,sum(a.AMOUNT)     as Next_Pay_Amt --下次支付金额
                 ,b.Next_PMT_AMT_Dt  --下次支付本金日
           FROM odm.ODM_CF_PLUS_PLAN_S_D a
           inner join dmf_tmp.tmp_dmfbc_alm_dm_01_bt_plus_loan_info_s_d_01 b
             on a.loanNo = b.loanNo
                and a.LIMITPAYDATE = b.Next_PMT_AMT_Dt  --下次支付本金日
           WHERE DT='$TX_DATE'
                 and substr(LIMITPAYDATE,1,10)>'$TX_DATE'
                 and AMOUNT<>0
           group by a.loanNo,b.Next_PMT_AMT_Dt )t1
 on t.loanNo = t1.loanNo
left join (select a.loanNo
                 ,sum(a.PLANFEE)  as Next_Pay_Fee --下次支付手续费
                 ,b.Next_PMT_FEE_Dt  --下次支付手续费日
           FROM odm.ODM_CF_PLUS_PLAN_S_D a
           inner join dmf_tmp.tmp_dmfbc_alm_dm_01_bt_plus_loan_info_s_d_01 b
             on a.loanNo = b.loanNo
                and a.LIMITPAYDATE = b.Next_PMT_FEE_Dt  --下次支付手续费日
           WHERE DT='$TX_DATE'
                 and substr(LIMITPAYDATE,1,10)>'$TX_DATE'
                 and PLANFEE<>0
           group by a.loanNo,b.Next_PMT_FEE_Dt )t2
 on t.loanNo = t2.loanNo
left join (select a.loanNo
                 ,sum(nvl(a.INTERESTAMOUNT,0)+nvl(a.OVERAMOUNT,0))   as Next_Pay_Inst --下次支付利息
                 ,b.Next_PMT_Inst_Dt --下次支付利息日
           FROM odm.ODM_CF_PLUS_PLAN_S_D a
           inner join dmf_tmp.tmp_dmfbc_alm_dm_01_bt_plus_loan_info_s_d_01 b
             on a.loanNo = b.loanNo
                and a.LIMITPAYDATE = b.Next_PMT_Inst_Dt --下次支付利息日
           WHERE DT='$TX_DATE'
                 and substr(LIMITPAYDATE,1,10)>'$TX_DATE'
                 and (INTERESTAMOUNT<>0
                     or OVERAMOUNT<>0)
            group by a.loanNo,b.Next_PMT_Inst_Dt )t3
 on t.loanNo = t3.loanNo;

use dmf_tmp;
drop table dmf_tmp.tmp_dmfbc_alm_dm_01_bt_plus_loan_info_s_d_04;
create table dmf_tmp.tmp_dmfbc_alm_dm_01_bt_plus_loan_info_s_d_04
as
select t.loanNo
      ,t1.First_PMT_AMT    --首次支付金额
      ,t1.First_PMT_AMT_Dt --首次本金支付日
      ,t2.First_PMT_Fee    --首次支付手续费
      ,t2.First_PMT_FEE_Dt --首次手续费支付日
      ,t3.First_PMT_Inst    --首次支付利息
      ,t3.First_PMT_INST_Dt  --首次利息支付日
from dmf_tmp.tmp_dmfbc_alm_dm_01_bt_plus_loan_info_s_d_01 t
left join (select a.loanNo
                 ,sum(a.amount) AS    First_PMT_AMT    --首次支付金额
                 ,b.First_PMT_AMT_Dt --首次本金支付日
           from odm.ODM_CF_PLUS_PLAN_S_D a
           inner join dmf_tmp.tmp_dmfbc_alm_dm_01_bt_plus_loan_info_s_d_01 b
             on a.loanNo = b.loanNo
                and a.LIMITPAYDATE = b.First_PMT_AMT_Dt --首次本金支付日
           where DT='$TX_DATE'
                 and amount<>0
           group by a.loanNo,b.First_PMT_AMT_Dt
                )t1
 on t.loanNo = t1.loanNo
left join (select a.loanNo
                 ,sum(nvl(a.PLANFEE,0)+nvl(a.PAYEDPLANFEE,0)) AS    First_PMT_Fee    --首次支付手续费 
                 ,b.First_PMT_FEE_Dt --首次手续费支付日
           from odm.ODM_CF_PLUS_PLAN_S_D a
           inner join dmf_tmp.tmp_dmfbc_alm_dm_01_bt_plus_loan_info_s_d_01 b
             on a.loanNo = b.loanNo
                and a.LIMITPAYDATE = b.First_PMT_FEE_Dt --首次手续费支付日
           where DT='$TX_DATE'
                 and (planfee<>0 OR PAYEDPLANFEE<>0)
           group by a.loanNo,b.First_PMT_FEE_Dt
              )t2
 on t.loanNo = t2.loanNo
left join (select a.loanNo
                 ,sum(nvl(a.INTERESTPAYEDAMOUNT,0)+nvl(a.INTERESTAMOUNT,0)) AS    First_PMT_Inst    --首次支付利息
                 ,b.First_PMT_INST_Dt  --首次利息支付日
           from odm.ODM_CF_PLUS_PLAN_S_D a
           inner join dmf_tmp.tmp_dmfbc_alm_dm_01_bt_plus_loan_info_s_d_01 b
             on a.loanNo = b.loanNo
                and a.LIMITPAYDATE = b.First_PMT_INST_Dt  --首次利息支付日
           where DT='$TX_DATE'
                 and (INTERESTPAYEDAMOUNT<>0 OR INTERESTAMOUNT<>0)
           group by a.loanNo,b.First_PMT_INST_Dt
              )t3
 on t.loanNo = t3.loanNo;
);

$SQL_BUFF[2]=qq(
set mapred.job.name=dmfbc_alm_dm_01_bt_plus_loan_info_s_d2;
set hive.auto.convert.join=false;

use dmf_bc;
alter table dmf_bc.dmfbc_alm_dm_01_bt_plus_loan_info_s_d drop if exists partition(dt='$TX_DATE');
insert overwrite table dmf_bc.dmfbc_alm_dm_01_bt_plus_loan_info_s_d partition(dt='$TX_DATE')
select
LOAN_ORDER.LOANID      AS    Loan_Id        --白条id，主键
,LOAN_ORDER.ORDERID    AS    Jd_Order_Id    --京东订单号  
,LOAN_ORDER.DT         AS    Data_Dt        --数据日期(切片日期)
,'DBT'    AS    Data_Src       --业务来源
,LOAN_ORDER.JDPIN      AS    Cust_Id        --客户号
,CASE WHEN dim_dbt.BIZ_ID3 in ('45','46') THEN '9620008' 
      WHEN dim_dbt.BIZ_ID3='36' THEN '9630001'
      when LOAN_ORDER.MERCHANTCODE in ('4890000064','4890000065','4890000066') then '9630037'  --现在先做特殊处理
      when LOAN_ORDER.MERCHANTCODE in ('5940000001') then '9630000' -- 暂时做其他处理
  ELSE dim_dbt.BIZ_ID3  END     AS    Product_Cd   -- 产品代码
,null     AS    Subject_Cd     --科目代码
,'CNY'    AS    Currency_Cd    --币种代码
,INVESTOR_LOAN.investor_id    --出资方商户号
-- Modified by machunliang@20180802 for 修改正确的商家商户号
-- ,LOAN_ORDER.MERCHANTUSERID  AS    Merchant_Code      --商家商户号
,LOAN_ORDER.MERCHANTCODE  AS    Merchant_Code      --商家商户号
,null    AS    Main_Code    --主体编号
,LOAN_ORDER.SPECIALMKTID    AS    Market_ID    --营销活动ID
,PLUS_MCHANT_ORDER.App_Code         --下单渠道
,null    AS    Acct_Org_Cd      --核算机构代码
,null    AS    Operate_Org_Cd   --考核机构代码
,CASE WHEN LOAN_ORDER.PLANRATE=0 THEN 1 
   ELSE 0 END     AS    Is_FreeCharge    --是否免息订单
,PLUS_PLAN.Is_Overdue    --是否逾期
,LOAN_ORDER.RISKTAKING    AS    Risk_Type    --风险类型
,0    AS    PV_Flag    --是否复利
,'CUST_TYPE'    AS    Member_Type    --会员类型
,PLUS_PLAN.Status_Cd    --白条状态
,LOAN_ORDER.CREATEDATE      AS    Create_Dt   --创建日期
,PLUS_PLAN.Start_Dt    --起息日期
,PLUS_PLAN.Maturity_Dt --到期日
,PLUS_PLAN.finish_date    --完成日期
,PLUS_PLAN.Overdue_Dt    --逾期日
,PLUS_PLAN.Overdue_days    --逾期天数
,LOAN_ORDER.PLAN    AS    Original_terms    --分期数
,PLUS_PLAN.Original_iou_terms    --已结清期数
,PLUS_PLAN.Over_iou_terms    --逾期期数
,PLUS_PLAN.Term    --期限
,'D'    AS    Term_Mult   -- 期限单位
,PLUS_PLAN.Residual_Maturity   --剩余期限
,'D'    AS    Residual_Maturity_Mult    --剩余期限单位
,5    AS    Accr_Basis_Cd    --计息基础
,LOAN_ORDER.AMOUNT    AS    Order_Amt    --订单金额
,LOAN_ORDER.DEPOSITAMOUNT    AS    Deposit_Amt      --备付金金额
,LOAN_ORDER.DEPOSITPROPORTION*100    AS    Deposit_Proportion   --备付金比例
,PLUS_PLAN.Order_Fee_Amt    --总手续费
,LOAN_ORDER.SERVICEAMOUNT    AS    Order_Ser_Amt    --总服务费
,PLUS_PLAN.Order_Inst_Amt    --总利息
,PLUS_PLAN.Order_Due_Inst    --总逾期利息
,0    AS    Order_Due_PV_Inst    --总逾期罚息利息
,PLUS_PLAN.Real_Pay_Amt    --实收本金
,PLUS_PLAN.Real_Ovr_Amt    --实收逾期本金
,PLUS_PLAN.Real_Fee_Amt    --实收手续费
,LOAN_ORDER.SERVICEAMOUNT    AS    Real_Ser_Amt    --实收服务费
,PLUS_PLAN. Real_Pay_Inst    --实收利息
,PLUS_PLAN.Real_Pay_Due_Inst    --实收逾期利息
,0    AS    Real_Pay_Due_PV_Inst    --实收逾期罚息利息
,PLUS_PLAN.Should_Pay_Amt*NVL(INVESTOR_LOAN.INVESTMENT_RATIO,100)/100   as   Should_Pay_Amt    --待还本金
,PLUS_PLAN.Should_Ovr_Amt    --待还逾期本金
,PLUS_PLAN.Should_Pay_Fee    --待还手续费
,0    AS    Should_Pay_Ser    --待还服务费
,PLUS_PLAN.Should_Pay_Inst    --待还利息
,PLUS_PLAN.Should_Pay_Due_Inst    --待还逾期利息
,0    AS    Should_Pay_Due_PV_Inst    --待还逾期罚息利息
,PLUS_PLAN.Old_Plan_Fee    --原始手续费  
,LOAN_ORDER.SERVICEAMOUNT    AS    Old_Plan_Svc    --原始服务费
,PLUS_PLAN.Old_Plan_Inst    --原始利息
,PLUS_PLAN.Old_Plan_Fee_Rate    --原始手续费率
,null    AS    Market_Plan_Fee_Dis    --营销手续费优惠折扣
,PLUS_PLAN.Market_Fee_Start_DATE    --营销手续费优惠起始日期
,PLUS_PLAN.Market_Fee_End_DATE    --营销手续费优惠结束日期
,PLUS_PLAN.Market_Plan_Fee    --营销优惠手续费
,PLUS_PLAN.Fomer_Plan_Fee    --商家补贴手续费
,null    AS    Market_Plan_Ser_Dis    --营销服务费优惠折扣
,null    AS    Market_Ser_Start_DATE    --营销服务费优惠起始日期
,null    AS    Market_Ser_End_DATE    --营销服务费优惠结束日期
,null    AS    Market_Plan_Ser    --营销优惠日服务费
,null    AS    Fomer_Plan_Ser    --商家补贴日服务费
,null    AS    Market_Plan_Inst_Dis    --营销利息优惠折扣
,null    AS    Market_Plan_Inst_Start_DATE    --营销利息优惠起始日期
,null    AS    Market_Plan_Inst_End_DATE    --营销利息优惠结束日期
,null    AS    Market_Plan_Inst    --营销优惠日利息
,null    AS    Fomer_Plan_Inst    --商家补贴日利息
,null    AS    Fomer_Plan_Flag    --商家补贴模式
,PLUS_PLAN.Fee_Rate*100    --手续费率(%)
,LOAN_ORDER.PLANFORMERCHANTPROPORTION      AS    Fee_Plat_Proportion    --手续费平台分成比例  
,null    AS    Fee_Fomer_Proportion    --手续费商家分成比例
,LOAN_ORDER.PLANFORINVESTMENTPROPORTION    AS    Fee_Investmen_Proportion    --手续费投资方分成比例  
,LOAN_ORDER.SERVICERATE*100    AS    Service_Rate    --服务费率
,LOAN_ORDER.SERVICEFORPLATPROPORTION      AS    Service_Plat_Proportion    --服务费平台分成比例  
,null    AS    Service_Fomer_Proportion    --服务费商家分成比例
,LOAN_ORDER.SERVICEFORINVESTMENTPROPORTION      AS    Service_Investmen_Proportion    --服务费投资方分成比例  
,CASE WHEN BIZTYPE='MDX' THEN 15 
  ELSE    PLUS_PLAN.INTERESTRATE  *100 END      AS    Inst_Rate    --利息率
,LOAN_ORDER.PLANFORMERCHANTPROPORTION      AS    Inst_Plat_Proportion    --利息平台分成比例  
,null    AS    Inst_Fomer_Proportion    --利息商家分成比例
,LOAN_ORDER.PLANFORINVESTMENTPROPORTION    AS    Inst_Investmen_Proportion    --利息投资方分成比例  
,CASE WHEN BIZTYPE='MDX' THEN 15 
  WHEN     PLUS_PLAN.OVERRATE IN (0.003,0.0045,0.005) THEN    PLUS_PLAN.OVERRATE*100 ELSE PLUS_PLAN.OVERRATE*360*100  END    AS    Due_Rate    --逾期利率(%)
,LOAN_ORDER.PLANFORMERCHANTPROPORTION      AS    Overdue_Inst_Plat_Proportion    --逾期利息平台分成比例  
,null    AS    Overdue_Inst_Fomer_Proportion    --逾期利息商家分成比例
,LOAN_ORDER.PLANFORINVESTMENTPROPORTION    AS    Overdue_Inst_Investmen_Proportion    --逾期利息投资方分成比例  
,null    AS    Intst_Rate_Offset_Val    --利率浮动值
,null    AS    Intst_Rate_Offset_Tp    --利率浮动方式
,CASE WHEN LOAN_ORDER.PLANTYPE in('0','4') THEN 7 
      WHEN LOAN_ORDER.PLANTYPE='1' THEN 5 
      WHEN LOAN_ORDER.PLANTYPE IN ('2','3') THEN 4  
   END  AS   PMT_Type    --还款方式
,11    AS    Intst_Rate_Adjst_Tp    --利率调整方式
,PLUS_PLAN.Repricing_Freq    --重定价频率
,'D'    AS    Repricing_Freq_Mult    --重定价频率单位
,LOAN_ORDER.PLAN    AS    PMT_COUNT    --支付次数
,1    AS    PMT_Freq    --支付频率
,'M'    AS    PMT_Freq_Mult    --支付频率单位
--,tb1.Last_PMT_Dt    --上次还款日
--,tb2.Next_PMT_Dt    --下次还款日
,tb1.Last_PMT_AMT_Dt  --上次支付本金日
,tb1.Last_PMT_Inst_Dt --上次支付利息日
,null as Last_PMT_Srv_Dt  --上次支付服务费日
,tb1.Last_PMT_FEE_Dt  --上次支付手续费日
,tb2.Next_PMT_AMT_Dt  --下次支付本金日
,tb2.Next_PMT_Inst_Dt --下次支付利息日
,null as Next_PMT_Srv_Dt  --下次支付服务费日
,tb2.Next_PMT_FEE_Dt  --下次支付手续费日
,PLUS_PLAN.Last_Repricing_Dt    --上次重定价日
,PLUS_PLAN.Next_Repricing_Dt    --下次重定价日
,LOAN_ORDER.AMOUNT    AS    Last_Repricing_Amt    --上次重定价金额
--,PLUS_PLAN.First_PMT_Dt    --首次支付日
,PLUS_PLAN.First_PMT_AMT_Dt --首次本金支付日
,PLUS_PLAN.First_PMT_INST_Dt  --首次利息支付日
,PLUS_PLAN.First_PMT_SRV_Dt --首次服务费支付日
,PLUS_PLAN.First_PMT_FEE_Dt --首次手续费支付日
,tb5.First_PMT_AMT    --首次支付金额
,tb5.First_PMT_Fee    --首次支付手续费
,0    AS    First_PMT_Srv    --首次支付服务费
,tb5.First_PMT_Inst  AS    First_PMT_Inst    --首次支付利息
,tb1.LAST_Pay_Amt    --上次支付金额
,tb1.LAST_Pay_Fee    --上次支付手续费
,0    AS    LAST_Pay_Srv    --上次支付服务费
,tb1.LAST_Pay_Inst    --上次支付利息
,tb2.Next_Pay_Amt    --下次支付金额
,tb2.Next_Pay_Fee    --下次支付手续费
,0    AS    Next_Pay_Srv    --下次支付服务费
,tb2.Next_Pay_Inst    --下次支付利息
,tb2.Next_Pay_Fee/datediff(tb2.Next_PMT_AMT_Dt,tb1.Last_PMT_AMT_Dt)    AS    Day_Accr_Fee_Amt    --当日计提手续费
,0    AS    Day_Accr_Ser_Amt    --当日计提服务费
,tb3.Day_Accr_Intst_Amt    --当日计提利息
,tb3.Day_Accr_Due_Intst_Amt    --当日计提逾期利息
,0    AS    Day_Accr_Due_PV_Amt    --当日计提逾期利息罚息
,0    AS    ABS_ID   --对应ABS编号   有ABS编号改为是否ABS出表 1:出表,0:不出表
,null    as    UNION_LOAN_ID    --对应联合贷编号
,tb3.writeoffstatus   as    writeoffstatus    --坏账核销状态：0-否 1-是
,DIM_INVESTMEN.investor_type         as investor_type  --资方类型： 1：内部资方、0：外部资方
,case when DIM_INVESTMEN.INVESTOR_ID is null then 100  
 else  DIM_INVESTMEN.Under_Ratio end  as Under_Ratio    --兜底比列(里面的空值默认为不兜底)
,t2.repay_count            as  INT_PAY_CNT      --利息还款次数
,t2.repay_count            as  PRIN_REPAY_CNT      --本金还款次数
,NVL(INVESTOR_LOAN.INVESTMENT_RATIO,100) as  INVESTMENT_RATIO          --出资比例
,DIM_INVESTMEN.INVESTOR_NAME  as INVESTOR_NAME
,INVESTOR_LOAN.plat_id        as plat_id
,t5.cooperation           as cooperation        --合作方式
,t5.fixed_income_rate     as fixed_income_rate  --固定收益率
,t5.ratio_rate            as ratio_rate         --按笔分成比例
,t5.risk_selling_fixed_rate as risk_selling_fixed_rate  --固收利率
from (select a.*
      from (
             select *
             from odm.ODM_CF_PLUS_LOAN_ORDER_S_D
             where dt='$TX_DATE'
                   and status =3
                 --and PLANTYPE<>'2'
                   and to_date(completetime)<=dt
                    )a
              left join (
                         SELECT MERCHANTNO 
                         FROM dim.DIM_WHIP_MERCHANT_CATE_A_D 
                         WHERE PRODUCTTYPE_CODE IN (4,5,6,7)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
                          )b
        on a.MERCHANTCODE = b.MERCHANTNO
       where b.MERCHANTNO is null      
         )LOAN_ORDER
left join dmf_tmp.tmp_dmfbc_alm_dm_01_bt_plus_loan_info_s_d_01 PLUS_PLAN
on LOAN_ORDER.loanid = PLUS_PLAN.loanNo
left join dmf_tmp.tmp_dmfbc_alm_dm_01_bt_plus_loan_info_s_d_02 tb1
on LOAN_ORDER.loanid =tb1.loanNo
left join dmf_tmp.tmp_dmfbc_alm_dm_01_bt_plus_loan_info_s_d_03 tb2
on LOAN_ORDER.loanid =tb2.loanNo
left join (select t.loanNo
                 ,t.writeoffstatus as writeoffstatus    --坏账核销状态
                 ,nvl(AMOUNT_today,0)-nvl(AMOUNT_yestoday,0) as Day_Accr_Intst_Amt    --当日计提利息
                 ,nvl(OVERAMOUNT_today,0)-nvl(OVERAMOUNT_yestoday,0) as Day_Accr_Due_Intst_Amt    --当日计提逾期利息
           from (
                 select loanNo
                       ,SUM(INTERESTAMOUNT)+SUM(OVERAMOUNT)+SUM(INTERESTPAYEDAMOUNT)+SUM(OVERPAYAMOUNT) as AMOUNT_today
                       ,SUM(OVERAMOUNT)+SUM(OVERPAYAMOUNT) as OVERAMOUNT_today
                       ,MAX(case when badloanbalancestatus = 1 then 1 else 0 end) AS  writeoffstatus    --坏账核销状态：0-否 1-是
                 FROM odm.ODM_CF_PLUS_PLAN_S_D
                 WHERE DT='$TX_DATE'
                       and substr(limitpaydate,1,10)<='$TX_DATE'
                 group by loanNo
                   )t
           left join (select loanNo
                            ,SUM(INTERESTAMOUNT)+SUM(OVERAMOUNT)+SUM(INTERESTPAYEDAMOUNT)+SUM(OVERPAYAMOUNT) as AMOUNT_yestoday
                            ,SUM(OVERAMOUNT)+SUM(OVERPAYAMOUNT) as OVERAMOUNT_yestoday
                      FROM odm.ODM_CF_PLUS_PLAN_S_D
                      WHERE DT='$TX_PREV_DATE'
                            and substr(limitpaydate,1,10)<='$TX_PREV_DATE'
                      group by loanNo
                       )t1
             on t.loanNo = t1.loanNo
           )tb3
on LOAN_ORDER.loanid =tb3.loanNo
left join dmf_tmp.tmp_dmfbc_alm_dm_01_bt_plus_loan_info_s_d_04 tb5
on LOAN_ORDER.loanid =tb5.loanNo
left join (select orderid
                 ,CASE WHEN SOURCETYPE='1' THEN 'JRPC'  
                       WHEN SOURCETYPE IN ('2','3') THEN 'JRMO'  
                       WHEN SOURCETYPE='4' THEN 'WX' 
                       WHEN SOURCETYPE='5' THEN 'H5'  
                   ELSE '-9999' END AS    App_Code         --下单渠道
           from odm.ODM_CF_PLUS_MCHANT_ORDER_S_D
           where dt='$TX_DATE'
            )PLUS_MCHANT_ORDER
on LOAN_ORDER.orderid =PLUS_MCHANT_ORDER.orderid
left join(select distinct bt_plus_merchantno.merchantno
                ,DIM_DBT_TRAN.BIZ_ID3
          from (select distinct productType,merchantno
                from odm.ODM_CF_PLUS_MERCHANTNO_S_D
                where dt='$TX_DATE'
                 )bt_plus_merchantno
          inner join dim.DIM_DBT_TRAN_JF_A_D DIM_DBT_TRAN
             on bt_plus_merchantno.productType = DIM_DBT_TRAN.biz_id5
            )dim_dbt
 on LOAN_ORDER.merchantcode =dim_dbt.merchantno
left join              
   ( SELECT *FROM  (
   select A.*,row_number()over(partition                                 
           by
               PLAT_LOAN_ID,
               A.INVESTOR_ID                                 
           order by
               RECORD_UPDATE_TIME desc ) as rn 
            from odm.ODM_AM_ABS_INVESTOR_LOAN_0000_S_D  A  ----from odm.ODM_FI_INVESTOR_LOAN_S_D A   
         where dt='$TX_DATE'
               and to_date(create_time)<=dt           -- 去除T+0 数据
               and to_date(record_update_time)<=dt    -- 去除T+0 数据
               AND (
                    STATUS IN ('Success')                                                                                                                                                                                
                    and plat_id in (select plat_id from dmf_dim.DMFDIM_OAR_DIM_CFS_PLAT_ID_I_D where dt = '$TX_DATE' and type =1 )                                                     
                    AND PLAT_LOAN_ID NOT IN (
                        '2017030729943919' ,'2017030729945023' ,'2017030729956657' ,'2017030729942468' ,'2017030729955491' ,'2017030729952135'                                                                      
                    )                                                             
            )) M
   WHERE M.RN='1'
              )  INVESTOR_LOAN
  on LOAN_ORDER.LOANID = INVESTOR_LOAN.plat_loan_id
left join (     SELECT
        A.INVESTOR_ID,
        A.INVESTOR_NAME,
        A.plat_id,
        A.investor_type,
        regexp_extract(config,
        'undertakePrincipalRate".(.*?)(,"undertakeQuarter)',
        1),
        CASE 
            WHEN (((UPPER(A.CONFIG)   LIKE '%INVESTORUNDERTAKE%') AND UPPER(A.CONFIG)   LIKE '%"UNDERTAKE":"UNDERTAKE"%'                                                    
            ) OR A.INVESTOR_TYPE='1')  
            THEN 100  
            ELSE regexp_extract(config,
            'undertakePrincipalRate".(.*?)(,"undertakeQuarter)',
            1)  
        END   AS      Under_Ratio
    FROM ODM.ODM_AM_ABS_PLAT_INVESTOR_S_D A         -----------FROM ODM.ODM_FI_PLAT_INVESTOR_S_D A                                                                                                          
    WHERE
        A.DT=CASE WHEN '$TX_DATE'<='2018-07-10' THEN '2018-07-10'  ELSE  '$TX_DATE' END                                                                                         
        AND plat_id in (select plat_id from dmf_dim.DMFDIM_OAR_DIM_CFS_PLAT_ID_I_D where dt = '$TX_DATE' and type =1 )  )  DIM_INVESTMEN
on INVESTOR_LOAN.investor_id =DIM_INVESTMEN.INVESTOR_ID 
   and INVESTOR_LOAN.plat_id =DIM_INVESTMEN.plat_id
left join (select loan_id,count(1) as repay_count from 
             (
                SELECT loan_id,plan_pmt_cnt,plan_pay_dt,row_number()over(partition by loan_id,substr(plan_pay_dt,1,10) order by substr(plan_pay_dt,1,10) ) rn  
                from dmf_bc.dmfbc_alm_dm_01_dbt_repay_plan_s_d
                WHERE dt ='$TX_DATE' and   (PLAN_PMT_PRIN<>0 or ACTU_PMT_PRIN>0 or ACCUM_PMT_PRIN>0) and plan_pmt_cnt<>0
             ) t
         where rn = 1
         group by loan_id ) t2
on LOAN_ORDER.LOANID =t2.loan_id
left join (select * from dmf_bc.dmfbc_alm_dm_01_investor_rate_s_d where dt ='${TX_DATE}' and business_plat_line ='大白') t5
on DIM_INVESTMEN.INVESTOR_ID = t5.INVESTOR_ID ;
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