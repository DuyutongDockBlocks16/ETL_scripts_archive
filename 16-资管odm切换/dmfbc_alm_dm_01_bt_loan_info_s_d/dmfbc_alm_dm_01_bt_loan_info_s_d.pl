#!/usr/bin/perl
########################################################################################################################
#  Creater        :wangli550
#  Creation Time  :2018-03-19
#  Description    :dmfbc_alm_dm_01_bt_loan_info_s_d 小白条账户信息
#  Modify By      :
#  Modify Time    :
#      20190812    zlc      放开sdm_f02_cf_xbt_ordr_dtl_s_d 表中 DT='2018-01-01' and ((ABS(unpayoff_prin)< 0.1 OR unpayoff_prin= 0)  这个条件的限制
#                           因为历史分区的数据是早已被清空，这个条件已经失效
#      20190814    zlc      1.关联资管系统贷款单表和出资方配置表，取到出资方、资方类型、兜底比例、出资比例
#                           2.修改待还本金的逻辑，改为资管系统贷款单表中有就从这儿出，没有就从sdm_f02_cf_xbt_ordr_dtl_s_d出
#                           3.平台号改为从函数中出，不再手工维护平台号
#                           4.资管的贷款单表和资方配置表关联从只用investor_id 关联 改为用investor_id和plat_id关联
#                           5.加字段本金还款次数、利息还款次数
#                           6.加字段investor_name和plat_id
#     20190910     zlc     有些修改了账期当月不出帐，出现他的下一还款日对应下一个账期的还款日的账期，还款方式就用根据还款计划来
#     20190911     zlc     平台好337已维护到dmf_bc.dmdict函数中，去掉对平台号的限制
#     20190920     zlc     有了新的平台号338，函数还没维护进去，暂时限制下338
#     20190920     zlc     取消限制平台号338，将函数dmf_bc.dmdict替换为dmf_bc.dmdictdesc
#     20190924   wanglixin ODM_FI_INVESTOR_LOAN_S_D 表新增create_date进行时间限定，否则会有T+0数据
#     20200303     zlc     打是否abs出表的标，加资方的固定收益率、固收率、按比分成比例
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
$SQL_BUFF[0]=qq(
set mapred.job.name=dmfbc_alm_dm_01_bt_loan_info_s_d0;

use dmf_tmp;
drop table dmf_tmp.tmp_jdw_dmfbc_alm_dm_01_bt_loan_info_s_d_01_step1;
create table dmf_tmp.tmp_jdw_dmfbc_alm_dm_01_bt_loan_info_s_d_01_step1
as
select  loan_id
       ,MIN(plan_limit_pay_time) as First_PMT_Dt --首次支付日
       ,sum(case when curr_plan_num=1 then nvl(paid_prin,0)-nvl(refund_amt,0) else 0 end) as First_PMT_AMT  --首次支付金额
       ,sum(case when curr_plan_num=1 then nvl(plan_fee_amt,0)-nvl(wrtoff_bill_plan_amt,0)-nvl(plan_fee_prefr_amt,0)
              else 0 end) as First_PMT_Fee --首次支付手续费
       ,COUNT(case when plan_stat_code='1' then curr_plan_num else null end) as Original_iou_terms --已结清期数
       ,COUNT(case when over_stat_code<>0 then curr_plan_num else null end ) as Over_iou_terms  --逾期期数
       ,sum(case when over_stat_code<>0 then paid_prin else 0 end) as Real_Ovr_Amt --实收逾期本金
       ,max(case when substr(plan_limit_pay_time,1,10)<='$TX_DATE' and paid_prin<>0 then plan_limit_pay_time
              else null end) as Last_PMT_AMT_Dt  --上次支付本金日
       ,min(consm_time) as Last_PMT_Inst_Dt  --上次支付利息日
       ,min(consm_time) as Last_PMT_Srv_Dt --上次支付服务费日
       ,max(case when substr(plan_limit_pay_time,1,10)<='$TX_DATE' and paid_plan_fee_amt<>0 then plan_limit_pay_time
              else null end) as Last_PMT_FEE_Dt  --上次支付手续费日
       ,min(case when substr(plan_limit_pay_time,1,10)>'$TX_DATE' and loan_amt<>0 then plan_limit_pay_time
              else null end) as Next_PMT_AMT_Dt  --下次支付本金日
       ,min(case when substr(plan_limit_pay_time,1,10)>'$TX_DATE' and plan_fee_amt<>0 then plan_limit_pay_time
              else null end) as Next_PMT_FEE_Dt  --下次支付手续费日
       ,min(case when loan_amt<>0 then plan_limit_pay_time else null end) as First_PMT_AMT_Dt  --首次本金支付日
       ,min(consm_time) as First_PMT_SRV_Dt  --首次服务费支付日
       ,min(case when plan_fee_amt<>0 then plan_limit_pay_time else null end) as First_PMT_FEE_Dt --首次手续费支付日
from idm.idm_f02_cf_xbt_plan_dtl_s_d
where dt='$TX_DATE'
group by loan_id;


use dmf_tmp;
drop table dmf_tmp.tmp_jdw_dmfbc_alm_dm_01_bt_loan_info_s_d_01_step2;
create table dmf_tmp.tmp_jdw_dmfbc_alm_dm_01_bt_loan_info_s_d_01_step2
as
select t.loan_id
      ,t1.LAST_Pay_Amt  --上次支付金额
      ,t1.Last_PMT_AMT_Dt  --上次支付本金日
      ,t2.LAST_Pay_Fee  --上次支付手续费
      ,t2.Last_PMT_FEE_Dt  --上次支付手续费日
from (select loan_id
      from dmf_tmp.tmp_jdw_dmfbc_alm_dm_01_bt_loan_info_s_d_01_step1
      )t
left join (select a.loan_id
                 ,sum(a.paid_prin) as LAST_Pay_Amt  --上次支付金额
                 ,b.Last_PMT_AMT_Dt  --上次支付本金日
           from (select loan_id,paid_prin,plan_limit_pay_time 
                 from idm.idm_f02_cf_xbt_plan_dtl_s_d  
                 where dt='$TX_DATE'
                 and substr(plan_limit_pay_time,1,10)<='$TX_DATE' 
                 and paid_prin<>0) a
           inner join dmf_tmp.tmp_jdw_dmfbc_alm_dm_01_bt_loan_info_s_d_01_step1 b
             on a.loan_id = b.loan_id
                and a.plan_limit_pay_time = b.Last_PMT_AMT_Dt  --上次支付本金日
           group by a.loan_id,b.Last_PMT_AMT_Dt )t1
 on t.loan_id = t1.loan_id
left join (select a.loan_id
                 ,sum(a.paid_plan_fee_amt) as LAST_Pay_Fee  --上次支付手续费
                 ,b.Last_PMT_FEE_Dt  --上次支付手续费日
           from (select loan_id,paid_plan_fee_amt,plan_limit_pay_time 
                 from idm.idm_f02_cf_xbt_plan_dtl_s_d 
                 where  dt='$TX_DATE'
                 and substr(plan_limit_pay_time,1,10)<='$TX_DATE' 
                 and paid_plan_fee_amt<>0) a
           inner join dmf_tmp.tmp_jdw_dmfbc_alm_dm_01_bt_loan_info_s_d_01_step1 b
             on a.loan_id = b.loan_id
                and a.plan_limit_pay_time = b.Last_PMT_FEE_Dt  --上次支付手续费日        
           group by a.loan_id,b.Last_PMT_FEE_Dt )t2
 on t.loan_id = t2.loan_id;

use dmf_tmp;
drop table dmf_tmp.tmp_jdw_dmfbc_alm_dm_01_bt_loan_info_s_d_01_step3;
create table dmf_tmp.tmp_jdw_dmfbc_alm_dm_01_bt_loan_info_s_d_01_step3
as
select t.loan_id
      ,t1.Next_Pay_Amt  --下次支付金额
      ,t1.Next_PMT_AMT_Dt  --下次支付本金日
      ,t2.Next_Pay_Fee  --下次支付手续费
      ,t2.Next_PMT_FEE_Dt  --下次支付手续费日
from (select loan_id
      from dmf_tmp.tmp_jdw_dmfbc_alm_dm_01_bt_loan_info_s_d_01_step1
      )t
left join (select a.loan_id
                 ,sum(a.loan_amt-a.refund_amt) as Next_Pay_Amt  --下次支付金额
                 ,b.Next_PMT_AMT_Dt  --下次支付本金日
           from (select loan_id,loan_amt,refund_amt,plan_limit_pay_time 
                 from idm.idm_f02_cf_xbt_plan_dtl_s_d 
                 where dt='$TX_DATE'
                 and substr(plan_limit_pay_time,1,10)>'$TX_DATE' 
                 and loan_amt<>0 )a
           inner join dmf_tmp.tmp_jdw_dmfbc_alm_dm_01_bt_loan_info_s_d_01_step1 b
             on a.loan_id = b.loan_id
                and a.plan_limit_pay_time = b.Next_PMT_AMT_Dt
           group by a.loan_id,b.Next_PMT_AMT_Dt )t1
 on t.loan_id = t1.loan_id
left join (select a.loan_id
                 ,sum(a.plan_fee_amt-a.wrtoff_bill_plan_amt-a.plan_fee_prefr_amt) as Next_Pay_Fee  --下次支付手续费
                 ,b.Next_PMT_FEE_Dt  --下次支付手续费日
           from (select loan_id,plan_fee_amt,wrtoff_bill_plan_amt,plan_fee_prefr_amt,plan_limit_pay_time
                 from idm.idm_f02_cf_xbt_plan_dtl_s_d 
                 where dt='$TX_DATE'
                 and substr(plan_limit_pay_time,1,10)>'$TX_DATE' 
                 and plan_fee_amt<>0)a
           inner join dmf_tmp.tmp_jdw_dmfbc_alm_dm_01_bt_loan_info_s_d_01_step1 b
             on a.loan_id = b.loan_id
                and a.plan_limit_pay_time = b.Next_PMT_FEE_Dt
           group by a.loan_id,b.Next_PMT_FEE_Dt )t2
 on t.loan_id = t2.loan_id; 

use dmf_tmp;
drop table dmf_tmp.tmp_jdw_dmfbc_alm_dm_01_bt_loan_info_s_d_01;
create table dmf_tmp.tmp_jdw_dmfbc_alm_dm_01_bt_loan_info_s_d_01
as
select distinct tb1.loan_id
      ,tb1.Day_Accr_Intst_Amt --当日计提利息
      ,tb1.Day_Accr_Due_Intst_Amt --当日计提逾期利息
      ,tb4.Day_Accr_Fee_Amt --当日计提手续费
from (select t.loan_id
            ,(nvl(t.DAY_INT,0)-nvl(t.DISCOUNT_DAY_INT,0))
              -(t1.DAY_INT-t1.DISCOUNT_DAY_INT)     as Day_Accr_Intst_Amt --当日计提利息
            ,nvl(t.PNSH_INT,0)-nvl(t1.PNSH_INT,0)   as Day_Accr_Due_Intst_Amt --当日计提逾期利息
      from (select *
            from sdm.sdm_f02_cf_xbt_ordr_dtl_s_d

            where dt='$TX_DATE'
              )t
      left join (select *
                 from sdm.sdm_f02_cf_xbt_ordr_dtl_s_d

                 where dt='$TX_PREV_DATE'
                  )t1
        on t.loan_id = t1.loan_id
         )tb1
left join (select dim_day_id
                 ,loan_id
                 ,plan_fee_amt-wrtoff_bill_plan_amt-plan_fee_prefr_amt as Day_Accr_Fee_Amt  --当日计提手续费
           from (
                 select dt as dim_day_id
                       ,loan_id
                       ,curr_plan_num
                       ,plan_fee_amt
                       ,wrtoff_bill_plan_amt
                       ,plan_fee_prefr_amt
                       ,row_number()over(partition by loan_id order by plan_limit_pay_time asc) as rn
                from idm.idm_f02_cf_xbt_plan_dtl_s_d
                where dt='$TX_DATE'
                      and substr(plan_limit_pay_time,1,10)>='$TX_DATE'
                  )t
           where t.rn=1
            )tb4
  on tb1.loan_id = tb4.loan_id;
  
  

);

$SQL_BUFF[1]=qq(
set mapred.job.name=dmfbc_alm_dm_01_bt_loan_info_s_d1;

use dmf_tmp;
drop table if exists dmf_tmp.tmp_dmfbc_alm_dm_01_bt_loan_info_s_d2;
create table dmf_tmp.tmp_dmfbc_alm_dm_01_bt_loan_info_s_d2
as
select 
  INVESTOR_LOAN.INVESTOR_ID   as INVESTOR_ID
  ,DIM_INVESTMEN.INVESTOR_NAME  as INVESTOR_NAME
  ,INVESTOR_LOAN.plat_id        as plat_id
  ,NVL(INVESTOR_LOAN.INVESTMENT_RATIO,100)   as INVESTMENT_RATIO
  ,INVESTOR_LOAN.plat_loan_id   as plat_loan_id
  ,DIM_INVESTMEN.investor_type  as investor_type
  ,DIM_INVESTMEN.Under_Ratio    as Under_Ratio          
from
 ( SELECT *FROM  (
   select A.*,row_number()over(partition                                 
           by
               PLAT_LOAN_ID,
               A.INVESTOR_ID                                 
           order by
               RECORD_UPDATE_TIME desc ) as rn 
         from odm.ODM_FI_INVESTOR_LOAN_S_D A   
         where dt='$TX_DATE'
               and to_date(create_time)<=dt           -- 去除T+0 数据
               and to_date(record_update_time)<=dt    -- 去除T+0 数据
               AND STATUS IN ('Success')
               and plat_id in (select plat_id from dmf_dim.DMFDIM_OAR_DIM_CFS_PLAT_ID_I_D where dt = '$TX_DATE' and type =2 )) M
   WHERE M.RN='1'
              )  INVESTOR_LOAN
left join (SELECT
        A.INVESTOR_ID,
        A.INVESTOR_NAME,
        A.plat_id,
        A.investor_type,
        regexp_extract(config,
        'undertakePrincipalRate".(.*?)(,"undertakeQuarter)',
        1),
        CASE 
            WHEN (((UPPER(A.CONFIG)   LIKE '%INVESTORUNDERTAKE%') AND A.CONFIG  NOT   LIKE '%"undertake":"NonUndertake"%'                                                    
            ) OR A.INVESTOR_TYPE='1')  
            THEN 100  
            ELSE regexp_extract(config,
            'undertakePrincipalRate".(.*?)(,"undertakeQuarter)',
            1)  
        END   AS      Under_Ratio
    FROM
        odm.ODM_FI_PLAT_INVESTOR_S_D A                                                                                                          
    WHERE
        A.DT=CASE WHEN '$TX_DATE'<='2018-07-10' THEN '2018-07-10'  ELSE  '$TX_DATE' END                                                                                       
        AND plat_id in (select plat_id from dmf_dim.DMFDIM_OAR_DIM_CFS_PLAT_ID_I_D where dt = '$TX_DATE' and type =2 ) )  DIM_INVESTMEN
on INVESTOR_LOAN.investor_id =DIM_INVESTMEN.INVESTOR_ID
   and INVESTOR_LOAN.plat_id =DIM_INVESTMEN.plat_id

);


$SQL_BUFF[2]=qq(
set mapred.job.name=dmfbc_alm_dm_01_bt_loan_info_s_d2;

use dmf_bc;
alter table dmf_bc.dmfbc_alm_dm_01_bt_loan_info_s_d drop partition (dt<='2019-12-31'); --删除19年及以前数据
alter table dmf_bc.dmfbc_alm_dm_01_bt_loan_info_s_d drop if exists partition(dt='$TX_DATE');
insert overwrite table dmf_bc.dmfbc_alm_dm_01_bt_loan_info_s_d  partition(dt='$TX_DATE')
select
 XBT_ORDR_DET.LOAN_ID   AS Loan_Id  --白条id，主键
,XBT_ORDR_DET.ORDR_ID   AS Jd_Order_Id  --京东订单号  
,XBT_ORDR_DET.DT        AS Data_Dt  --数据日期(切片日期)
,'XBT'    AS Data_Src --业务来源
,XBT_ORDR_DET.USER_PIN  AS Cust_Id  --客户号
,CASE WHEN LENGTH(DIM_BT_TRAN.BIZ_ID3)<>'7' THEN '9630000' ELSE   NVL(DIM_BT_TRAN.BIZ_ID3,'9630000') END  AS Product_Cd --产品代码(7位以下的做了转换)
,NULL                   AS Subject_Cd --科目代码
,'CNY'                  AS Currency_Cd  --币种代码
,t2.INVESTOR_ID  AS Investment_Code  --出资方商户号
,XBT_ORDR_DET.MHT_NO  AS Merchant_Code  --商家商户号
,NULL AS Main_Code  --主体编号
,XBT_ORDR_DET.CAMP_UUID AS Market_ID  --营销活动ID
,XBT_ORDR_DET.CHNL_ID AS App_Code --下单渠道
,NULL AS Acct_Org_Cd  --核算机构代码
,NULL AS Operate_Org_Cd --考核机构代码
,XBT_ORDR_DET.IS_FIRST  AS Is_First --是否首单
,XBT_ORDR_DET.IS_PAYED_FEE  AS Is_FreeCharge  --是否免息订单
,CASE WHEN XBT_ORDR_DET.OVRD_DAYS<>0 THEN 1 
  ELSE 0 END  AS Is_Overdue --是否逾期
,NULL AS Risk_Type  --风险类型
,case when XBT_ORDR_DET.PROD_TYPE='普通' then 2 
      when XBT_ORDR_DET.PROD_TYPE='校园' then 3
      when XBT_ORDR_DET.PROD_TYPE='农村' then 4 
     end AS Member_Type  --会员类型
,0  AS PV_Flag  --是否复利
,CASE WHEN XBT_ORDR_DET.OVRD_DAYS<>0 THEN 2 
      WHEN XBT_ORDR_DET.UNPAYOFF_PRIN=0 THEN 0 
   ELSE 1 END   AS Status_Cd  --白条状态
,XBT_ORDR_DET.LOAN_TIME AS Create_Dt  --创建日期
,XBT_ORDR_DET.LOAN_TIME AS Start_Dt --起息日期
,XBT_ORDR_DET.LIMIT_TIME  AS Maturity_Dt  --到期日
,BT_LOAN_ORDER.finishpaydate AS finish_date  --完成日期
,case when XBT_ORDR_DET.OVRD_DAYS>0 then date_add(XBT_ORDR_DET.DT,-cast(XBT_ORDR_DET.OVRD_DAYS as int))
  else null end  AS Overdue_Dt --逾期日
,XBT_ORDR_DET.OVRD_DAYS AS Overdue_days --逾期天数
,XBT_ORDR_DET.LOAN_TERM AS Original_terms --分期数
,XBT_STAG_DET.Original_iou_terms  --已结清期数
,XBT_STAG_DET.Over_iou_terms  --逾期期数
,XBT_ORDR_DET.LOAN_TERM AS Term --期限
,'M'  AS Term_Mult  --期限单位
,case when BT_LOAN_ORDER.finishpaydate is null then datediff(XBT_ORDR_DET.LIMIT_TIME,XBT_ORDR_DET.DT)
  else 0 end AS Residual_Maturity  --剩余期限
,'D'  AS Residual_Maturity_Mult --剩余期限单位
,1  AS Accr_Basis_Cd  --计息基础
,nvl(XBT_ORDR_DET.LOAN_PRIN,0)-nvl(XBT_ORDR_DET.REFUND_PRIN,0)  AS Order_Amt  --订单金额
,0  AS Deposit_Amt  --备付金金额
,0  AS Deposit_Proportion --备付金比例
,nvl(XBT_ORDR_DET.RECVBL_STAG_FEE,0)+nvl(XBT_ORDR_DET.bill_plan_fee,0)  AS Order_Fee_Amt  --总手续费
,BT_LOAN_ORDER.SERVICEFEE AS Order_Ser_Amt  --总服务费
,nvl(XBT_ORDR_DET.DAY_INT,0)+nvl(XBT_ORDR_DET.PNSH_INT,0) AS Order_Inst_Amt --总利息
,XBT_ORDR_DET.PNSH_INT  AS Order_Due_Inst --总逾期利息
,0  AS Order_Due_PV_Inst  --总逾期罚息利息
,XBT_ORDR_DET.RECV_PRIN AS Real_Pay_Amt --实收本金
,XBT_STAG_DET.Real_Ovr_Amt --实收逾期本金
,BT_PLAN.Real_Fee_Amt --实收手续费
,BT_LOAN_ORDER.SERVICEFEE AS Real_Ser_Amt --实收服务费
,nvl(BT_PLAN.PAYEDDAYAMOUNT,0)+nvl(XBT_ORDR_DET.PAYED_PNSH_INT,0) AS Real_Pay_Inst  --实收利息
,XBT_ORDR_DET.PAYED_PNSH_INT  AS Real_Pay_Due_Inst  --实收逾期利息
,0  AS Real_Pay_Due_PV_Inst --实收逾期罚息利息
,XBT_ORDR_DET.UNPAYOFF_PRIN*NVL(t2.INVESTMENT_RATIO,100)/100 AS Should_Pay_Amt --待还本金
,case when XBT_ORDR_DET.OVRD_DAYS>0 then XBT_ORDR_DET.UNPAYOFF_PRIN else 0 end AS Should_Ovr_Amt --待还逾期本金
,nvl(XBT_ORDR_DET.RECVBL_STAG_FEE,0)-nvl(XBT_ORDR_DET.PAYED_STAG_FEE,0) AS Should_Pay_Fee --待还手续费
,0  AS Should_Pay_Ser --待还服务费
,nvl(XBT_ORDR_DET.DAY_INT,0)-nvl(XBT_ORDR_DET.DISCOUNT_DAY_INT,0)
 -NVL(BT_PLAN.PAYEDDAYAMOUNT,0)
 +(nvl(XBT_ORDR_DET.PNSH_INT,0)-nvl(XBT_ORDR_DET.PAYED_PNSH_INT,0)) AS Should_Pay_Inst  --待还利息
,nvl(XBT_ORDR_DET.PNSH_INT,0)-nvl(XBT_ORDR_DET.PAYED_PNSH_INT,0)  AS Should_Pay_Due_Inst  --待还逾期利息
,0 AS Should_Pay_Due_PV_Inst --待还逾期罚息利息 ???
,XBT_ORDR_DET.STAG_FEE  AS Old_Plan_Fee --原始手续费  
,BT_LOAN_ORDER.SERVICEFEE AS Old_Plan_Svc --原始服务费
,NULL AS Old_Plan_Inst  --原始利息
,xbt_user_det.plan_rate*100 AS Old_Plan_Fee_Rate  --原始手续费率???无此字段
--,case when COUPON_RECORD.type='折扣' then COUPON_RECORD.COUPONCOUNT
--   else 0 end AS Market_Plan_Fee_Dis  --营销手续费优惠折扣
,null as Market_Plan_Fee_Dis
,BT_PLAN.Market_Fee_Start_DATE  --营销手续费优惠起始日期
,BT_PLAN.Market_Fee_End_DATE  --营销手续费优惠结束日期
,XBT_ORDR_DET.DISCOUNT_STAG_FEE AS Market_Plan_Fee  --营销优惠手续费
,NULL AS Fomer_Plan_Fee --商家补贴手续费
,NULL AS Market_Plan_Ser_Dis  --营销服务费优惠折扣
,NULL AS Market_Ser_Start_DATE  --营销服务费优惠起始日期
,NULL AS Market_Ser_End_DATE  --营销服务费优惠结束日期
,NULL AS Market_Plan_Ser  --营销优惠日服务费
,NULL AS Fomer_Plan_Ser --商家补贴日服务费
--,COUPON_RECORD.COUPONCOUNT  AS Market_Plan_Inst_Dis --营销利息优惠折扣
,null as Market_Plan_Inst_Dis
,BT_PLAN.Market_Plan_Inst_Start_DATE  --营销利息优惠起始日期
,BT_PLAN.Market_Plan_Inst_End_DATE  --营销利息优惠结束日期
,XBT_ORDR_DET.DISCOUNT_DAY_INT  AS Market_Plan_Inst --营销优惠日利息
,NULL AS Fomer_Plan_Inst  --商家补贴日利息
,1  AS Fomer_Plan_Flag  --商家补贴模式
,XBT_ORDR_DET.ACTL_FEE_RATE*100 AS Fee_Rate --手续费率(%)
--,INVESTOR_LOAN.INCOME_RATIO_FEE AS Fee_Plat_Proportion  --手续费平台分成比例
,null AS Fee_Plat_Proportion  
,NULL AS Fee_Fomer_Proportion --手续费商家分成比例
--,INVESTOR_LOAN.INCOME_RATIO_FEE AS Fee_Investmen_Proportion --手续费投资方分成比例
,null AS Fee_Investmen_Proportion  
,BT_LOAN_ORDER.SERVICEFEERATE AS Service_Rate --服务费率
,1  AS Service_Plat_Proportion  --服务费平台分成比例  
,0  AS Service_Fomer_Proportion --服务费商家分成比例
,0  AS Service_Investmen_Proportion --服务费投资方分成比例  
,BT_PLAN.Inst_Rate  --利息率
--,INVESTOR_LOAN.INCOME_RATIO_INTEREST  AS Inst_Plat_Proportion --利息平台分成比例 
,null  AS Inst_Plat_Proportion
,NULL AS Inst_Fomer_Proportion  --利息商家分成比例
--,INVESTOR_LOAN.INCOME_RATIO_INTEREST  AS Inst_Investmen_Proportion  --利息投资方分成比例
,null AS Inst_Investmen_Proportion  
,BT_PLAN.Due_Rate --逾期利率(%)
--,INVESTOR_LOAN.INCOME_RATIO_OVERDUE AS Inst_Plat_Overdue_Proportion --逾期利息平台分成比例 
,null AS Inst_Plat_Overdue_Proportion 
,NULL AS Inst_Fomer_Overdue_Proportion  --逾期利息商家分成比例
--,INVESTOR_LOAN.INCOME_RATIO_OVERDUE AS Inst_Investmen_Overdue_Proportion  --逾期利息投资方分成比例 
,null AS Inst_Investmen_Overdue_Proportion 
,NULL AS Intst_Rate_Offset_Val  --利率浮动值
,NULL AS Intst_Rate_Offset_Tp --利率浮动方式
,CASE when  t3.ct2 < t3.ct1 then 6    -- 6：根据还款付息计划
      WHEN XBT_ORDR_DET.LOAN_TERM<>'1' THEN 8
 ELSE 1 END   AS PMT_Type --还款方式
,11 AS Intst_Rate_Adjst_Tp  --利率调整方式
,XBT_ORDR_DET.LOAN_TERM AS Repricing_Freq --重定价频率
,'M'  AS Repricing_Freq_Mult  --重定价频率单位
,XBT_ORDR_DET.LOAN_TERM AS PMT_COUNT  --支付次数
,1  AS PMT_Freq --支付频率
,'M'  AS PMT_Freq_Mult  --支付频率单位
--,tb1.Last_PMT_Dt  --上次还款日
--,tb1.Next_PMT_Dt  --下次还款日
,XBT_STAG_DET.Last_PMT_AMT_Dt --上次支付本金日
,XBT_STAG_DET.Last_PMT_Inst_Dt  --上次支付利息日
,XBT_STAG_DET.Last_PMT_Srv_Dt --上次支付服务费日
,XBT_STAG_DET.Last_PMT_FEE_Dt --上次支付手续费日
,XBT_STAG_DET.Next_PMT_AMT_Dt --下次支付本金日
,XBT_ORDR_DET.LIMIT_TIME as Next_PMT_Inst_Dt  --下次支付利息日
,null as Next_PMT_Srv_Dt  --下次支付服务费日
,XBT_STAG_DET.Next_PMT_FEE_Dt --下次支付手续费日
,XBT_ORDR_DET.LOAN_TIME AS Last_Repricing_Dt  --上次重定价日
,XBT_ORDR_DET.LIMIT_TIME  AS Next_Repricing_Dt  --下次重定价日
,XBT_ORDR_DET.LOAN_PRIN-XBT_ORDR_DET.REFUND_PRIN AS Last_Repricing_Amt --上次重定价金额
--,XBT_STAG_DET.First_PMT_Dt  --首次支付日
,XBT_STAG_DET.First_PMT_AMT_Dt  --首次本金支付日
,case WHEN XBT_ORDR_DET.LOAN_TERM<>'1' then XBT_STAG_DET.First_PMT_AMT_Dt 
 else  XBT_ORDR_DET.LIMIT_TIME  end as First_PMT_INST_Dt --首次利息支付日
,XBT_STAG_DET.First_PMT_SRV_Dt  --首次服务费支付日
,XBT_STAG_DET.First_PMT_FEE_Dt  --首次手续费支付日
,XBT_STAG_DET.First_PMT_AMT --首次支付金额
,XBT_STAG_DET.First_PMT_Fee --首次支付手续费
,0  AS First_PMT_Srv  --首次支付服务费
,0  AS First_PMT_Inst --首次支付利息
,tb2.LAST_Pay_Amt --上次支付金额
,tb2.LAST_Pay_Fee --上次支付手续费
,0  AS LAST_Pay_Srv --上次支付服务费
,0  AS LAST_Pay_Inst  --上次支付利息
,tb3.Next_Pay_Amt --下次支付金额
,tb3.Next_Pay_Fee --下次支付手续费
,0  AS Next_Pay_Srv --下次支付服务费
,XBT_ORDR_DET.DAY_INT as Next_Pay_Inst  --下次支付利息
,tb1.Day_Accr_Fee_Amt --当日计提手续费
,0  AS Day_Accr_Ser_Amt --当日计提服务费
,tb1.Day_Accr_Intst_Amt --当日计提利息
,tb1.Day_Accr_Due_Intst_Amt --当日计提逾期利息
,0  AS Day_Accr_Due_PV_Amt  --当日计提逾期利息罚息
,case when t4.loan_id is not null then 1 else 0 end  AS ABS_ID --对应ABS编号     有ABS编号改为是否ABS出表
,NULL AS UNION_LOAN_ID  --对应联合贷编号
,BT_PLAN.writeoffstatus   as    writeoffstatus    --坏账核销状态：0-否 1-是
,XBT_ORDR_DET.sub_mht_no  as    Sub_Merchant_Code  --子商户号
,NVL(DIM_BT_TRAN.BIZ_ID3,'9630000')   AS Product_Cd1_ --产品代码(7位以下的不做转换)
,t2.investor_type         as investor_type  --资方类型： 1：内部资方、0：外部资方
,nvl(t2.Under_Ratio,100)  as Under_Ratio    --兜底比列
,t3.PLAN_PMT_TOTAL_CNT    as   INT_PAY_CNT      --利息还款次数
,t3.PLAN_PMT_TOTAL_CNT    as   PRIN_REPAY_CNT      --本金还款次数
,t2.INVESTMENT_RATIO      AS INVESTMENT_RATIO   --出资比例
,t2.INVESTOR_NAME         as INVESTOR_NAME      --出资方名称
,t2.plat_id               as plat_id            --plat_id
,t5.cooperation           as cooperation        --合作方式
,t5.fixed_income_rate     as fixed_income_rate  --固定收益率
,t5.ratio_rate            as ratio_rate         --按笔分成比例
,t5.risk_selling_fixed_rate as risk_selling_fixed_rate  --固收利率
from (select *
            from sdm.sdm_f02_cf_xbt_ordr_dtl_s_d
            where dt='$TX_DATE' and (loan_id <>'170224117062417542' and USER_PIN <>'jd_5d294654dfc43' )
       )XBT_ORDR_DET
left join (select loanid
                  ,SUM(PAYEDPLANFEE)  AS Real_Fee_Amt --实收手续费
                  ,SUM(PAYEDDAYAMOUNT) as PAYEDDAYAMOUNT 
                 ,MIN(case when planfeediscount>0 then LIMITPAYDATE else null end)  AS Market_Fee_Start_DATE  --营销手续费优惠起始日期
                 ,MAX(case when planfeediscount>0 then LIMITPAYDATE else null end)  AS Market_Fee_End_DATE  --营销手续费优惠结束日期
                 ,MIN(LIMITPAYDATE) AS Market_Plan_Inst_Start_DATE  --营销利息优惠起始日期
                 ,MAX(LIMITPAYDATE) AS Market_Plan_Inst_End_DATE  --营销利息优惠结束日期
                 ,MAX(DAYAMOUNTRATE)*360*100  AS Inst_Rate  --利息率
                 ,MAX(OVERRATE)*360*100 AS Due_Rate --逾期利率(%)
                 ,SUM(DAYAMOUNT-PAYEDDAYAMOUNT)+SUM(OVERAMOUNT-PAYEDOVERAMOUNT) AS Next_Pay_Inst  --下次支付利息 ???
                 ,max(case when writeoffstatus = 1 then 1 else 0 end)     as    writeoffstatus    --坏账核销状态：0-否 1-是
           from ODM.ODM_CF_PLAN_S_D
           where dt='$TX_DATE'
           group by loanid
           )BT_PLAN
  on XBT_ORDR_DET.loan_id= BT_PLAN.loanid
left join dmf_tmp.tmp_jdw_dmfbc_alm_dm_01_bt_loan_info_s_d_01_step1 XBT_STAG_DET
  on XBT_ORDR_DET.loan_id= XBT_STAG_DET.loan_id
left join (select *
           from ODM.ODM_CF_LOAN_ORDER_S_D
           where dt='$TX_DATE'
                 and bizcode not in ('12','13','26')
            )BT_LOAN_ORDER
  on XBT_ORDR_DET.LOAN_ID = BT_LOAN_ORDER.loanid
     and XBT_ORDR_DET.USER_PIN = BT_LOAN_ORDER.pin
left join dmf_tmp.tmp_jdw_dmfbc_alm_dm_01_bt_loan_info_s_d_01 tb1
  on XBT_ORDR_DET.LOAN_ID = tb1.LOAN_ID
left join dmf_tmp.tmp_jdw_dmfbc_alm_dm_01_bt_loan_info_s_d_01_step2 tb2
  on XBT_ORDR_DET.LOAN_ID = tb2.LOAN_ID
left join dmf_tmp.tmp_jdw_dmfbc_alm_dm_01_bt_loan_info_s_d_01_step3 tb3
  on XBT_ORDR_DET.LOAN_ID = tb3.LOAN_ID
left join (select user_pin,plan_rate
           from idm.idm_f02_cf_xbt_acct_s_d
           where dt='$TX_DATE' and main_acct_ind = 0 )xbt_user_det
 on XBT_ORDR_DET.user_pin=xbt_user_det.user_pin
left join dim.DIM_BT_TRAN_JF_A_D DIM_BT_TRAN
  on XBT_ORDR_DET.BIZ_ID = DIM_BT_TRAN.biz_id4
left join   dmf_tmp.tmp_dmfbc_alm_dm_01_bt_loan_info_s_d2 t2
on XBT_ORDR_DET.LOAN_ID  =  t2.plat_loan_id
left join  (select loan_id,max(PLAN_PMT_TOTAL_CNT) as PLAN_PMT_TOTAL_CNT,count(1) as ct1 ,count(distinct plan_pay_dt)  as  ct2  
            from dmf_bc.dmfbc_alm_dm_01_xbt_repay_plan_s_d where dt = '${TX_DATE}' group by loan_id  ) t3
on XBT_ORDR_DET.LOAN_ID = t3.loan_id
left join (select * from dmf_bc.dmfbc_alm_dm_01_xj_abs_info_s_d where dt = '${TX_DATE}') t4
on XBT_ORDR_DET.LOAN_ID =  t4.loan_id
left join (select * from dmf_bc.dmfbc_alm_dm_01_investor_rate_s_d where dt ='${TX_DATE}' and business_plat_line ='小白') t5
on t2.INVESTOR_ID = t5.INVESTOR_ID  ;

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