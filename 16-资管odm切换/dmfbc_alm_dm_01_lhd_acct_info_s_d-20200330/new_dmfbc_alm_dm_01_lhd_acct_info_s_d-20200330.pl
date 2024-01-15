#!/usr/bin/perl
########################################################################################################################
#  Creater        :wangli550
#  Creation Time  :2018-04-02
#  Description    :dmfbc_alm_dm_01_lhd_acct_info_s_d 联合贷基本信息
#  Modify By      :
#  Modify Time    :
#   zlc    20190807    加字段支付频率
#   zlc    20190807    加字段本金还款次数和利息还款次数
#   zlc    20190807    加字段逾期标志
#  Modify Content :
#  Script Version :1.0.3
########################################################################################################################
use strict;
use jrjtcommon;
use un_pswd;
use Common::Hive;
use Parallel::ForkManager;

my $MAX_CONCURRENT = 12;
my %rc_hash = ();


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
my $SYS = substr(${CONTROL_FILE}, 0, 3);
my $JOB = substr(${CONTROL_FILE}, 4, length(${CONTROL_FILE})-17);




#当日 yyyy-mm-dd
my $TABLE_DATE = substr(${CONTROL_FILE},length(${CONTROL_FILE})-12, 8);
#当日yyyy-mm-dd
my $TX_DATE = substr(${CONTROL_FILE},length(${CONTROL_FILE})-12, 4).'-'.substr(${CONTROL_FILE},length(${CONTROL_FILE})-8, 2).'-'.substr(${CONTROL_FILE},length(${CONTROL_FILE})-6,2);
#前一天yyyy-mm-dd
my $TX_PREV_DATE =getPreviousDate($TX_DATE);
#下一天yyyy-mm-dd
my $TX_NEXT_DATE=getNextDate($TX_DATE);
#当日yyyymmdd
my $TXDATE = substr(${CONTROL_FILE},length(${CONTROL_FILE})-12, 4).substr(${CONTROL_FILE},length(${CONTROL_FILE})-8, 2).substr(${CONTROL_FILE},length(${CONTROL_FILE})-6,2);
#前一天yyyymmdd
my $TXPDATE =substr(${TX_PREV_DATE},0,4).substr(${TX_PREV_DATE},5,2).substr(${TX_PREV_DATE},8,2);
#下一天yyyymmdd
my $TXNDATE=substr(${TX_NEXT_DATE},0,4).substr(${TX_NEXT_DATE},5,2).substr(${TX_NEXT_DATE},8,2);
##-------------------------------------------------------------------------------------------------------
my $SQL_STR=qq(hive -i /soft/automation/PUB/UDF/udf/hive.config -e "select dim_week_id from dim.dim_day where dim_day_id='$TXDATE';");
#当年第几周 yyyyww
my $WEEK_ID=`$SQL_STR`;
chomp($WEEK_ID);
##-------------------------------------------------------------------------------------------------------
#当日所在月 yyyymm
my $TXMONTH = substr(${CONTROL_FILE},length(${CONTROL_FILE})-12, 4).substr(${CONTROL_FILE},length(${CONTROL_FILE})-8, 2);
#当日所在月yyyy-mm
my $TX_MONTH = substr(${CONTROL_FILE},length(${CONTROL_FILE})-12, 4).'-'.substr(${CONTROL_FILE},length(${CONTROL_FILE})-8, 2);
#当日所在月的第一天 yyyy-mm-dd
my $first_day_of_current_month=substr($TX_DATE,0,7)."-01";
##-------------------------------------------------------------------------------------------------------
#上个月第一天  yyyy-mm-dd
my $first_day_of_last_month = get_last_month_first_day($TX_DATE);
#上个月最后一天  yyyy-mm-dd
my $last_day_of_last_month  = get_last_month_last_day($TX_DATE);
#上个月 yyyy-mm
my $last_month  = substr($first_day_of_last_month,0,7);
##-------------------------------------------------------------------------------------------------------
#上上个月第一天  yyyy-mm-dd
my $first_day_of_last_last_month = get_last_month_first_day($first_day_of_last_month);
#上上个月最后一天  yyyy-mm-dd
my $last_day_of_last_last_month  = get_last_month_last_day($last_day_of_last_month);
#上上个月 yyyy-mm
my $last_last_month  = substr($first_day_of_last_last_month,0,7);
##-------------------------------------------------------------------------------------------------------
##-------------------------------------------------------------------------------------------------------
#上一年 yyyy
my $last_year = substr($TX_DATE,0,4)-1;
#上一年第一天 yyyy-mm-dd
my $first_day_of_last_year = $last_year."-01-01";
#上一年最后一天 yyyy-mm-dd
my $last_day_of_last_year  = $last_year."-12-31";
##-------------------------------------------------------------------------------------------------------
#向前三个月第一天(用户出季报数据)  yyyy-mm-dd
my $first_day_of_3_month_before = get_last_month_first_day(addMonth($TX_DATE,-2));
#向前三个月最后一天(用户出季报数据)  yyyy-mm-dd
my $first_day_of_3_month_end    = get_last_month_last_day(addMonth($TX_DATE,-2));

#向前三个月第一天(用户出半年报数据)  yyyy-mm-dd
my $first_day_of_6_month_before = get_last_month_first_day(addMonth($TX_DATE,-5));
#向前三个月最后一天(用户出半年报数据)  yyyy-mm-dd
my $first_day_of_6_month_end    = get_last_month_first_day(addMonth($TX_DATE,-5));


########################################################################################################################
# Write SQL For Your APP
sub getsql
{
    my @SQL_BUFF=();
    #########################################################################################
    ####################################以下为SQL编辑区######################################
    #########################################################################################
    
$SQL_BUFF[0]=qq(
set mapred.job.name=dmfbc_alm_dm_01_lhd_acct_info_s_d0;

use dmf_tmp;

drop table if exists dmf_tmp.tmp_odm_fi_investor_flow_s_d;
create table dmf_tmp.tmp_odm_fi_investor_flow_s_d as
select 
            plat_loan_id,
            plat_id,
            investor_id,
            sum(principal-paid_principal-reverse_unpaid_principal-off_principal-acc_undertake_principal) as unpaid_principal
      from 
      (select
            f.plat_loan_id
           ,investor_id
           ,plat_id
           ,case when f.business_type = 'Loan' and f.money_type ='Principal' then coalesce(f.amount,0.0) else 0.0 end as principal --累计-贷款本金(贷款单完成时间)
           ,case when f.business_type = 'Repayment' and f.money_type ='PaidPrincipal' then coalesce(f.amount,0.0) else 0.0 end as paid_principal --累计-已还本金
           ,case when f.business_type = 'Refund' and f.money_type ='ReverseUnpaidPrincipal' then coalesce(f.amount,0.0) else 0.0 end as reverse_unpaid_principal --累计-冲销未还本金
           ,CASE WHEN f.business_type ='Cancel' AND f.money_type='CancelPrincipal' THEN COALESCE(f.amount,0.0) ELSE 0.0 END as off_principal --累计-取消消费-贷款本金
           ,CASE WHEN f.business_type ='UnderRepayment' AND f.money_type='Principal' THEN COALESCE(f.amount,0.0) ELSE 0.0 END as acc_undertake_principal --累计-兜底代偿本金
       from
            odm.ODM_AM_ABS_INVESTOR_FLOW_0000_S_D f----odm.odm_fi_investor_flow_s_d f
       where  dt='$TX_DATE'
       ) t
       group by plat_loan_id,plat_id,investor_id
       
;

drop table if exists dmf_tmp.tmp_dmdictdesc_bt_jt;
create table         dmf_tmp.tmp_dmdictdesc_bt_jt as
select 
            plat_id,
            dmf_bc.dmdictdesc('zgs_plat_type',plat_id) as plat_id_type
      from 
           (select plat_id from dmf_tmp.tmp_odm_fi_investor_flow_s_d group by plat_id) t
;
);

$SQL_BUFF[1]=qq(
set mapred.job.name=dmfbc_alm_dm_01_lhd_acct_info_s_d1;

use dmf_bc;
alter table dmf_bc.dmfbc_alm_dm_01_lhd_acct_info_s_d drop partition (dt<='2019-12-31',type='BT'); --删除19年及以前数据

insert overwrite table dmf_bc.dmfbc_alm_dm_01_lhd_acct_info_s_d  partition(dt='$TX_DATE',type='BT')
select
 BT_LOAN_INFO.LOAN_ID  as Loan_Id  --资产业务id，主键
,BT_LOAN_INFO.JD_ORDER_ID as Jd_Order_Id  --京东订单号  
,BT_LOAN_INFO.DT as Data_Dt --数据日期(切片日期)
,'9620009' as Product_Cd  --产品代码
,BT_LOAN_INFO.SUBJECT_CD as Subject_Cd  --科目代码
,BT_LOAN_INFO.Currency_Cd as Currency_Cd  --币种代码
,BT_LOAN_INFO.Investment_Code as Investment_Code --出资方商户号
,'LHD' as Data_Src --业务来源
,BT_LOAN_INFO.ACCT_ORG_CD as Acct_Org_Cd  --核算机构代码
,BT_LOAN_INFO.OPERATE_ORG_CD as Operate_Org_Cd  --考核机构代码
,'2' as UNION_TYPE  --联合贷类型
,BT_LOAN_INFO.CREATE_DT as Create_Dt  --创建日期
,BT_LOAN_INFO.START_DT as Start_Dt  --起息日期
,BT_LOAN_INFO.MATURITY_DT as Maturity_Dt  --到期日
,BT_LOAN_INFO.FINISH_DATE as finish_date  --完成日期
,BT_LOAN_INFO.OVERDUE_DT as Overdue_Dt  --逾期日
,BT_LOAN_INFO.OVERDUE_DAYS as Overdue_days  --逾期天数
,BT_LOAN_INFO.TERM as Term  --期限
,BT_LOAN_INFO.TERM_MULT as Term_Mult  --期限单位
,BT_LOAN_INFO.RESIDUAL_MATURITY as Residual_Maturity  --剩余期限
,BT_LOAN_INFO.RESIDUAL_MATURITY_MULT as Residual_Maturity_Mult  --剩余期限单位
,BT_LOAN_INFO.ACCR_BASIS_CD as Accr_Basis_Cd  --计息基础
,BT_LOAN_INFO.ORDER_AMT as Order_Amt  --订单金额
,BT_LOAN_INFO.PMT_Type as PMT_Type --还款方式
,Investment_Ratio as Investment_Ratio --出资比例
,Under_Ratio as Under_Ratio --兜底比例
,case when unpaid_principal<0 then 0 else unpaid_principal end as Should_Pay_Amt  --待还本金
,(case when unpaid_principal<0 then 0 else unpaid_principal end)*NVL(UNDER_RATIO,0)/100 as Under_Should_Pay_Amt --兜底待还本金
,case when unpaid_principal<0 then 0 else unpaid_principal end as Investment_Should_Pay_Amt  --出资待还本金
, 0  as Pay_Rate  --付息率
, 0    as Day_Pay_Intst_Amt --当日计提付息利息
,case when NVL(BT_LOAN_INFO.Under_Ratio,0)=0 then '8' else '2' end as SBJ_TYP
,BT_LOAN_INFO.writeoffstatus     as writeoffstatus    --坏账核销状态：0-否 1-是
,BT_LOAN_INFO.Sub_Merchant_Code  as Sub_Merchant_Code   --子商户号
,BT_LOAN_INFO.Product_Cd1       as Product_Cd1 --产品代码(7位以下的不做转换)
,BT_LOAN_INFO.First_PMT_INST_Dt  as  FST_INT_PAY_DT      --首次付息日
,BT_LOAN_INFO.First_PMT_AMT_Dt   as  First_PMT_AMT_Dt    --首次还本日
,BT_LOAN_INFO.Last_Repricing_Dt  as  LAST_RPRC_DT        --上次重定价日期
,BT_LOAN_INFO.Next_Repricing_Dt  as  NEXT_RPRC_DT        --下次重定价日期
,CASE WHEN  BT_LOAN_INFO.PMT_TYPE in ('5','7','8') THEN BT_LOAN_INFO.Last_PMT_FEE_Dt ELSE BT_LOAN_INFO.Last_PMT_Inst_Dt END   as  LAST_INT_PAY_DT      --上次利息支付日
,BT_LOAN_INFO.Last_PMT_AMT_Dt    as  LAST_PRIN_REPAY_DT  --上次本金支付日
,CASE WHEN  BT_LOAN_INFO.PMT_TYPE in ('5','7','8') THEN BT_LOAN_INFO.Next_PMT_FEE_Dt ELSE BT_LOAN_INFO.Next_PMT_Inst_Dt END   as  NEXT_INT_PAY_DT      --下次利息支付日
,BT_LOAN_INFO.Next_PMT_AMT_Dt    as  NEXT_PRIN_REPAY_DT  --下次本金支付日
,case when BT_LOAN_INFO.Product_Cd='9630040'  
      then BT_LOAN_INFO.Inst_Rate else BT_LOAN_INFO.Fee_Rate 
   end                as  NET_INT_RATE      --执行利率-净利率
,concat(BT_LOAN_INFO.pmt_freq,BT_LOAN_INFO.pmt_freq_mult)  as pay_freq
,INT_PAY_CNT   as   INT_PAY_CNT      --利息还款次数
,PRIN_REPAY_CNT   as   PRIN_REPAY_CNT      --本金还款次数
,BT_LOAN_INFO.is_overdue    as is_overdue      --是否逾期
,BT_LOAN_INFO.investor_name                    --资方名称
,BT_LOAN_INFO.plat_id                          --平台号
,nvl(case when investor_type = 0 and  cooperation in('按比分成') then case when BT_LOAN_INFO.Product_Cd='9630040'  then BT_LOAN_INFO.Inst_Rate else BT_LOAN_INFO.Fee_Rate*24*Term/(1+Term) end *ratio_rate
      when investor_type = 0 and  cooperation in ('风险卖断') and Is_FreeCharge =0 then risk_selling_fixed_rate*100
      when investor_type = 0 and  cooperation in ('风险卖断') and Is_FreeCharge <> 0 then case when BT_LOAN_INFO.Product_Cd='9630040'  then BT_LOAN_INFO.Inst_Rate else BT_LOAN_INFO.Fee_Rate*24*Term/(1+Term) end *ratio_rate
      when investor_type = 0 and  cooperation in('固定收益','贷款余额固收') then fixed_income_rate*100
  else 0 end,0) as out_invertor_rate --外部资方利率
from
   (select *
              from dmf_bc.dmfbc_alm_dm_01_bt_loan_info_s_d
              where dt='$TX_DATE' and INVESTOR_TYPE='0'
               ) BT_LOAN_INFO
left join
    (
      select /*+mapjoin(t2)*/
            a.plat_loan_id,
            a.investor_id,
            a.unpaid_principal
      from 
           dmf_tmp.tmp_odm_fi_investor_flow_s_d a
        inner join 
           dmf_tmp.tmp_dmdictdesc_bt_jt b
         on a.plat_id=b.plat_id
      where b.plat_id_type=2
     ) k1
    on BT_LOAN_INFO.LOAN_ID=k1.plat_loan_id  and BT_LOAN_INFO.investment_code=k1.investor_id 
;
);



$SQL_BUFF[2]=qq(
set mapred.job.name=dmfbc_alm_dm_01_lhd_acct_info_s_d2;

use dmf_bc;
alter table dmf_bc.dmfbc_alm_dm_01_lhd_acct_info_s_d drop partition (dt<='2019-12-31',type='JT'); --删除19年及以前数据
insert overwrite table dmf_bc.dmfbc_alm_dm_01_lhd_acct_info_s_d  partition(dt='$TX_DATE',type='JT')
select
JT_LOAN_INFO.LOAN_ID as Loan_Id --资产业务id，主键
,JT_LOAN_INFO.JD_ORDER_ID as Jd_Order_Id  --京东订单号
,JT_LOAN_INFO.DT as Data_Dt --数据日期(切片日期)
,'9620010' as Product_Cd  --产品代码
,JT_LOAN_INFO.SUBJECT_CD as Subject_Cd  --科目代码
,JT_LOAN_INFO.Currency_Cd as Currency_Cd  --币种代码
,jt_loan_info.Investment_Code as Investment_Code --出资方商户号
,'LHD' as Data_Src --业务来源
,JT_LOAN_INFO.ACCT_ORG_CD as Acct_Org_Cd  --核算机构代码
,JT_LOAN_INFO.OPERATE_ORG_CD as Operate_Org_Cd  --考核机构代码
,'1' as UNION_TYPE  --联合贷类型
,JT_LOAN_INFO.CREATE_DT as Create_Dt  --创建日期
,JT_LOAN_INFO.START_DT as Start_Dt  --起息日期
,JT_LOAN_INFO.MATURITY_DT as Maturity_Dt  --到期日
,JT_LOAN_INFO.FINISH_DATE  as finish_date --完成日期
,JT_LOAN_INFO.OVERDUE_DT as Overdue_Dt  --逾期日
,JT_LOAN_INFO.OVERDUE_DAYS as Overdue_days  --逾期天数
,JT_LOAN_INFO.TERM as Term  --期限
,JT_LOAN_INFO.TERM_MULT as Term_Mult  --期限单位
,JT_LOAN_INFO.RESIDUAL_MATURITY as Residual_Maturity  --剩余期限
,JT_LOAN_INFO.RESIDUAL_MATURITY_MULT as Residual_Maturity_Mult  --剩余期限单位
,JT_LOAN_INFO.ACCR_BASIS_CD as Accr_Basis_Cd  --计息基础
,JT_LOAN_INFO.ORDER_AMT as Order_Amt  --订单金额
,JT_LOAN_INFO.PMT_Type as PMT_Type --还款方式
,Investment_Ratio  as Investment_Ratio --出资比例
,Under_Ratio as Under_Ratio  --兜底比例
,case when unpaid_principal<0 then 0 else unpaid_principal end as Should_Pay_Amt  --待还本金
,(case when unpaid_principal<0 then 0 else unpaid_principal end)*NVL(Under_Ratio ,0)/100  as Under_Should_Pay_Amt --兜底待还本金
,case when unpaid_principal<0 then 0 else unpaid_principal end as Investment_Should_Pay_Amt --出资待还本金
, 0  as Pay_Rate --付息率
, 0    as Day_Pay_Intst_Amt --当日计提付息利息
,case when NVL(jt_loan_info.Under_Ratio,0)=0 then '8' else '2' end as SBJ_TYP
,jt_loan_info.writeoffstatus     as writeoffstatus    --坏账核销状态：0-否 1-是
,null  as Sub_Merchant_Code   --子商户号
,null  as Product_Cd1 --产品代码(7位以下的不做转换)
,CASE WHEN  jt_loan_info.PMT_TYPE in ('5','7','8') THEN  jt_loan_info.First_PMT_FEE_Dt ELSE  jt_loan_info.First_PMT_INST_Dt END   as  FST_INT_PAY_DT      --首次付息日
,jt_loan_info.First_PMT_AMT_Dt                as  First_PMT_AMT_Dt    --首次还本日
,jt_loan_info.Last_Repricing_Dt               as  LAST_RPRC_DT        --上次重定价日期
,jt_loan_info.Next_Repricing_Dt               as  NEXT_RPRC_DT        --下次重定价日期
,CASE WHEN  jt_loan_info.PMT_TYPE in ('5','7','8') THEN  jt_loan_info.Last_PMT_FEE_Dt ELSE  jt_loan_info.Last_PMT_Inst_Dt END     as  LAST_INT_PAY_DT      --上次利息支付日
,jt_loan_info.Last_PMT_AMT_Dt                 as  LAST_PRIN_REPAY_DT  --上次本金支付日
,CASE WHEN  jt_loan_info.PMT_TYPE in ('5','7','8') THEN  jt_loan_info.Next_PMT_FEE_Dt ELSE  jt_loan_info.Next_PMT_Inst_Dt END     as  NEXT_INT_PAY_DT      --下次利息支付日
,jt_loan_info.Next_PMT_AMT_Dt                 as  NEXT_PRIN_REPAY_DT  --下次本金支付日
,case when jt_loan_info.Inst_Rate is null or jt_loan_info.Inst_Rate = 0  
             then jt_loan_info.Fee_Rate else jt_loan_info.Inst_Rate 
        end                as  NET_INT_RATE      --执行利率-净利率
,concat(jt_loan_info.pmt_freq,jt_loan_info.pmt_freq_mult)  as pay_freq
,INT_PAY_CNT                    as  INT_PAY_CNT      --利息还款次数
,PRIN_REPAY_CNT           as  PRIN_REPAY_CNT      --本金还款次数
,jt_loan_info.is_overdue       as is_overdue      --是否逾期
,jt_loan_info.investor_name                       --资方名称
,jt_loan_info.plat_id                             --平台号
,nvl(case when cooperation in('按比分成','风险卖断') then case when JT_LOAN_INFO.TERM_MULT = 'M' then JT_LOAN_INFO.Fee_Rate*24*JT_LOAN_INFO.Term/(1+JT_LOAN_INFO.Term) else JT_LOAN_INFO.Inst_Rate end *ratio_rate
      when investor_type = 0 and cooperation in('固定收益','贷款余额固收') then fixed_income_rate*100
      else 0 end,0) as out_invertor_rate --外部资方利率
from (select *
           from dmf_bc.dmfbc_alm_dm_01_jt_acct_info_s_d
           where dt='$TX_DATE' and INVESTOR_TYPE='0'
            )jt_loan_info
  left join
    (
      select 
            plat_loan_id,
            investor_id,
            unpaid_principal
      from 
           dmf_tmp.tmp_odm_fi_investor_flow_s_d
      where plat_id='1'
    ) k1
  on jt_loan_info.LOAN_ID=k1.plat_loan_id  and jt_loan_info.Investment_Code=k1.investor_id
;
);


$SQL_BUFF[3]=qq(
set mapred.job.name=dmfbc_alm_dm_01_lhd_acct_info_s_d3;

alter table dmf_bc.dmfbc_alm_dm_01_lhd_acct_info_s_d drop partition (dt<='2019-12-31',type='BT_PLUS'); --删除19年及以前数据
insert overwrite table dmf_bc.dmfbc_alm_dm_01_lhd_acct_info_s_d  partition(dt='$TX_DATE',type='BT_PLUS')
select
PLUS_LOAN_INFO.LOAN_ID as Loan_Id --资产业务id，主键
,PLUS_LOAN_INFO.JD_ORDER_ID as Jd_Order_Id  --京东订单号
,PLUS_LOAN_INFO.DT as Data_Dt --数据日期(切片日期)
,'9620011' as Product_Cd  --产品代码
,PLUS_LOAN_INFO.SUBJECT_CD as Subject_Cd  --科目代码
,PLUS_LOAN_INFO.Currency_Cd as Currency_Cd  --币种代码
,Investment_Code  as Investment_Code  --出资方商户号
,'LHD' as Data_Src --业务来源
,PLUS_LOAN_INFO.ACCT_ORG_CD as Acct_Org_Cd  --核算机构代码
,PLUS_LOAN_INFO.OPERATE_ORG_CD as Operate_Org_Cd  --考核机构代码
,'3' as UNION_TYPE  --联合贷类型
,PLUS_LOAN_INFO.CREATE_DT as Create_Dt  --创建日期
,PLUS_LOAN_INFO.START_DT as Start_Dt  --起息日期
,PLUS_LOAN_INFO.MATURITY_DT as Maturity_Dt  --到期日
,PLUS_LOAN_INFO.FINISH_DATE as finish_date  --完成日期
,PLUS_LOAN_INFO.OVERDUE_DT as Overdue_Dt  --逾期日
,PLUS_LOAN_INFO.OVERDUE_DAYS as Overdue_days  --逾期天数
,PLUS_LOAN_INFO.TERM as Term  --期限
,PLUS_LOAN_INFO.TERM_MULT as Term_Mult  --期限单位
,PLUS_LOAN_INFO.RESIDUAL_MATURITY  as Residual_Maturity --剩余期限
,PLUS_LOAN_INFO.RESIDUAL_MATURITY_MULT as Residual_Maturity_Mult  --剩余期限单位
,PLUS_LOAN_INFO.ACCR_BASIS_CD as Accr_Basis_Cd  --计息基础
,PLUS_LOAN_INFO.ORDER_AMT as Order_Amt  --订单金额
,PLUS_LOAN_INFO.PMT_Type as PMT_Type --还款方式
,Investment_Ratio as Investment_Ratio --出资比例
,Under_Ratio as Under_Ratio --兜底比例
,case when unpaid_principal<0 then 0 else unpaid_principal end as Should_Pay_Amt  --待还本金
,(case when unpaid_principal<0 then 0 else unpaid_principal end)*NVL(UNDER_RATIO,100)/100 as Under_Should_Pay_Amt  --兜底待还本金
,case when unpaid_principal<0 then 0 else unpaid_principal end as Investment_Should_Pay_Amt  --出资待还本金
,0 as Pay_Rate  --付息率
, 0    as Day_Pay_Intst_Amt --当日计提付息利息
,case when NVL(UNDER_RATIO,0)=0 then '8' else '2' end as SBJ_TYP
,plus_loan_info.writeoffstatus     as writeoffstatus    --坏账核销状态：0-否 1-是
,null  as Sub_Merchant_Code   --子商户号
,null  as Product_Cd1 --产品代码(7位以下的不做转换)
,CASE WHEN  plus_loan_info.PMT_TYPE in ('5','7','8') THEN  plus_loan_info.First_PMT_FEE_Dt ELSE  plus_loan_info.First_PMT_INST_Dt END   as  FST_INT_PAY_DT      --首次付息日
,plus_loan_info.First_PMT_AMT_Dt                as  First_PMT_AMT_Dt    --首次还本日
,plus_loan_info.Last_Repricing_Dt               as  LAST_RPRC_DT        --上次重定价日期
,plus_loan_info.Next_Repricing_Dt               as  NEXT_RPRC_DT        --下次重定价日期
,CASE WHEN  plus_loan_info.PMT_TYPE in ('5','7','8') THEN  plus_loan_info.Last_PMT_FEE_Dt ELSE  plus_loan_info.Last_PMT_Inst_Dt END      as  LAST_INT_PAY_DT      --上次利息支付日
,plus_loan_info.Last_PMT_AMT_Dt                 as  LAST_PRIN_REPAY_DT  --上次本金支付日
,CASE WHEN  plus_loan_info.PMT_TYPE in ('5','7','8') THEN  plus_loan_info.Next_PMT_FEE_Dt ELSE  plus_loan_info.Next_PMT_Inst_Dt END      as  NEXT_INT_PAY_DT      --下次利息支付日
,plus_loan_info.Next_PMT_AMT_Dt                 as  NEXT_PRIN_REPAY_DT  --下次本金支付日
,case when  plus_loan_info.Inst_Rate is null or  plus_loan_info.Inst_Rate = 0  
             then  plus_loan_info.Fee_Rate else  Inst_Rate 
        end                as  NET_INT_RATE      --执行利率-净利率
,concat(plus_loan_info.pmt_freq,plus_loan_info.pmt_freq_mult)  as pay_freq
,INT_PAY_CNT            as  INT_PAY_CNT      --利息还款次数
,PRIN_REPAY_CNT           as  PRIN_REPAY_CNT      --本金还款次数
,plus_loan_info.is_overdue       as is_overdue      --是否逾期
,plus_loan_info.investor_name   -- 资方名称
,plus_loan_info.plat_id         -- 平台号
,nvl(case when cooperation in('按比分成','风险卖断') then case when plus_loan_info.Inst_Rate is null or plus_loan_info.Inst_Rate = 0 then plus_loan_info.Fee_Rate else plus_loan_info.Inst_Rate end *(ratio_rate)
      when cooperation in('固定收益','贷款余额固收') then fixed_income_rate*100
  end,0) as out_invertor_rate --外部资方利率
from 
  (select *
           from dmf_bc.dmfbc_alm_dm_01_bt_plus_loan_info_s_d
           where dt='$TX_DATE' and INVESTOR_TYPE='0'
           )plus_loan_info
left join
    (
      select /*+mapjoin(t2)*/
            a.plat_loan_id,
            a.investor_id,
            a.unpaid_principal
      from 
           dmf_tmp.tmp_odm_fi_investor_flow_s_d a
        inner join 
           dmf_tmp.tmp_dmdictdesc_bt_jt b
         on a.plat_id=b.plat_id
      where b.plat_id_type=1
    ) k1
   on plus_loan_info.LOAN_ID=k1.plat_loan_id  and plus_loan_info.INVESTMENT_CODE=k1.investor_id
;
);


$SQL_BUFF[4]=qq(
set mapred.job.name=dmfbc_alm_dm_01_lhd_acct_info_s_d4;

alter table dmf_bc.dmfbc_alm_dm_01_lhd_acct_info_s_d drop partition (dt<='2019-12-31',type='JND'); --删除19年及以前数据
insert overwrite table dmf_bc.dmfbc_alm_dm_01_lhd_acct_info_s_d  partition(dt='$TX_DATE',type='JND')
select
 JND_LOAN_INFO.LOAN_ID  as Loan_Id --资产业务id，主键
,JND_LOAN_INFO.JD_ORDER_ID   as Jd_Order_Id --京东订单号
,JND_LOAN_INFO.DT as Data_Dt  --数据日期(切片日期)
,'9520007' as Product_Cd --产品代码
,JND_LOAN_INFO.SUBJECT_CD as Subject_Cd --科目代码
,JND_LOAN_INFO.Currency_Cd as Currency_Cd --币种代码
,Investment_Code as Investment_Code --出资方商户号
,'LHD' as Data_Src --业务来源
,JND_LOAN_INFO.ACCT_ORG_CD  as Acct_Org_Cd  --核算机构代码
,JND_LOAN_INFO.OPERATE_ORG_CD as Operate_Org_Cd --考核机构代码
,'4' as UNION_TYPE  --联合贷类型
,JND_LOAN_INFO.CREATE_DT    as Create_Dt  --创建日期
,JND_LOAN_INFO.START_DT as Start_Dt --起息日期
,JND_LOAN_INFO.MATURITY_DT as Maturity_Dt --到期日
,JND_LOAN_INFO.FINISH_DATE as finish_date --完成日期
,JND_LOAN_INFO.OVERDUE_DT as Overdue_Dt --逾期日
,JND_LOAN_INFO.OVERDUE_DAYS  as Overdue_days  --逾期天数
,JND_LOAN_INFO.TERM as Term --期限
,JND_LOAN_INFO.TERM_MULT  as Term_Mult  --期限单位
,JND_LOAN_INFO.RESIDUAL_MATURITY  as Residual_Maturity  --剩余期限
,JND_LOAN_INFO.RESIDUAL_MATURITY_MULT as Residual_Maturity_Mult --剩余期限单位
,JND_LOAN_INFO.ACCR_BASIS_CD  as Accr_Basis_Cd  --计息基础
,JND_LOAN_INFO.ORDER_AMT  as Order_Amt  --订单金额
,JND_LOAN_INFO.PMT_Type as PMT_Type --还款方式
,Investment_Ratio as Investment_Ratio --出资比例
,Under_Ratio  as Under_Ratio --兜底比例
,Should_Pay_Amt   as Should_Pay_Amt --待还本金
,CASE WHEN LOAN_FUND.sub_loan_amount IS NULL THEN SHOULD_PAY_AMT ELSE  JND_LOAN_INFO.SHOULD_PAY_AMT*UNDER_RATIO/100  END  as Under_Should_Pay_Amt  --兜底待还本金
,CASE WHEN LOAN_FUND.sub_loan_amount IS NULL THEN  JND_LOAN_INFO.SHOULD_PAY_AMT ELSE  JND_LOAN_INFO.SHOULD_PAY_AMT*LOAN_FUND.sub_loan_amount/JND_LOAN_INFO.ORDER_AMT  END  as Investment_Should_Pay_Amt  --出资待还本金
,0  as Pay_Rate  --付息率
,0 as Day_Pay_Intst_Amt --当日计提付息利息
,'2'  as SBJ_TYP
,null  as writeoffstatus    --坏账核销状态：0-否 1-是
,null  as Sub_Merchant_Code   --子商户号
,null  as Product_Cd1 --产品代码(7位以下的不做转换)
,jnd_loan_info.First_PMT_Inst_Dt   as  FST_INT_PAY_DT                  --首次付息日
,jnd_loan_info.First_PMT_AMT_Dt                as  First_PMT_AMT_Dt    --首次还本日
,jnd_loan_info.Last_Repricing_Dt               as  LAST_RPRC_DT        --上次重定价日期
,jnd_loan_info.Next_Repricing_Dt               as  NEXT_RPRC_DT        --下次重定价日期
,jnd_loan_info.Last_PMT_AMT_Dt                 as  LAST_INT_PAY_DT     --上次利息支付日
,jnd_loan_info.Last_PMT_Inst_Dt                as  LAST_PRIN_REPAY_DT  --上次本金支付日
,jnd_loan_info.Next_PMT_AMT_Dt                 as  NEXT_INT_PAY_DT     --下次利息支付日
,jnd_loan_info.Next_PMT_Inst_Dt                as  NEXT_PRIN_REPAY_DT  --下次本金支付日
,CASE WHEN jnd_loan_info.Roll_Flag=0 THEN jnd_loan_info.Inst_Rate ELSE  jnd_loan_info.Roll_Rate END                as  NET_INT_RATE      --执行利率-净利率
,concat(jnd_loan_info.pmt_freq,jnd_loan_info.pmt_freq_mult)  as pay_freq
,INT_PAY_CNT                 as  INT_PAY_CNT      --利息还款次数
,PRIN_REPAY_CNT                as  PRIN_REPAY_CNT      --本金还款次数
,Is_Overdue       as is_overdue      --是否逾期
,''                                  --资方名称
,''                                  --平台号
,jnd_loan_info.out_investor_rate                --外部资方执行利率
from  (select *
           from dmf_bc.dmfbc_alm_dm_01_jnd_acct_info_s_d
           where dt='$TX_DATE' and investor_type='0'
      )jnd_loan_info
left join (select *
           from odm.ODM_NCJR_JND_FD_LOAN_FUND_S_D 
           where dt='$TX_DATE' AND LOAN_STATUS IN ( 'LOAN_SUCCESS', 'REPAY_OVER')
            )LOAN_FUND
  on jnd_loan_info.LOAN_ID =LOAN_FUND.loan_no and jnd_loan_info.Investment_Code=LOAN_FUND.invest_no
; 
);

$SQL_BUFF[5]=qq(
set mapred.job.name=dmfbc_alm_dm_01_lhd_acct_info_s_d5;

alter table dmf_bc.dmfbc_alm_dm_01_lhd_acct_info_s_d drop partition (dt<='2019-12-31',type='JXD'); --删除19年及以前数据
insert overwrite table dmf_bc.dmfbc_alm_dm_01_lhd_acct_info_s_d  partition(dt='$TX_DATE',type='JXD')
select
 jxd_loan_info.LOAN_ID  as Loan_Id --资产业务id，主键
,null   as Jd_Order_Id --京东订单号
,jxd_loan_info.DT as Data_Dt  --数据日期(切片日期)
,'9920014' as Product_Cd --产品代码
,jxd_loan_info.SUBJECT_CD as Subject_Cd --科目代码
,jxd_loan_info.Currency_Cd as Currency_Cd --币种代码
,pay_account    as Investment_Code --出资方商户号
,'LHD' as Data_Src --业务来源
,jxd_loan_info.ACCT_ORG_CD  as Acct_Org_Cd  --核算机构代码
,jxd_loan_info.OPERATE_ORG_CD as Operate_Org_Cd --考核机构代码
,'5' as UNION_TYPE  --联合贷类型
,jxd_loan_info.CREATE_DT    as Create_Dt  --创建日期
,jxd_loan_info.START_DT as Start_Dt --起息日期
,jxd_loan_info.MATURITY_DT as Maturity_Dt --到期日
,jxd_loan_info.FINISH_DATE as finish_date --完成日期
,jxd_loan_info.OVERDUE_DT as Overdue_Dt --逾期日
,jxd_loan_info.OVERDUE_DAYS  as Overdue_days  --逾期天数
,jxd_loan_info.TERM as Term --期限
,jxd_loan_info.TERM_MULT  as Term_Mult  --期限单位
,jxd_loan_info.RESIDUAL_MATURITY  as Residual_Maturity  --剩余期限
,jxd_loan_info.RESIDUAL_MATURITY_MULT as Residual_Maturity_Mult --剩余期限单位
,jxd_loan_info.ACCR_BASIS_CD  as Accr_Basis_Cd  --计息基础
,jxd_loan_info.ORDER_AMT  as Order_Amt  --订单金额
,jxd_loan_info.PMT_Type as PMT_Type --还款方式
,100  as Investment_Ratio --出资比例
,Under_Ratio as Under_Ratio --兜底比例
,should_pay_amt  as Should_Pay_Amt --待还本金
,0  as Under_Should_Pay_Amt  --兜底待还本金
,should_pay_amt  as Investment_Should_Pay_Amt  --出资待还本金
,0  as Pay_Rate  --付息率
,0 as Day_Pay_Intst_Amt --当日计提付息利息
,case when pay_account<>'lhfdztwypt(sns)' then '2' else '8' end as SBJ_TYP
,null  as writeoffstatus    --坏账核销状态：0-否 1-是
,null  as Sub_Merchant_Code   --子商户号
,null  as Product_Cd1 --产品代码(7位以下的不做转换)
,jxd_loan_info.First_PMT_Inst_Dt   as  FST_INT_PAY_DT                  --首次付息日
,jxd_loan_info.First_PMT_AMT_Dt                as  First_PMT_AMT_Dt    --首次还本日
,jxd_loan_info.Last_Repricing_Dt               as  LAST_RPRC_DT        --上次重定价日期
,jxd_loan_info.Next_Repricing_Dt               as  NEXT_RPRC_DT        --下次重定价日期
,jxd_loan_info.Last_PMT_AMT_Dt                 as  LAST_INT_PAY_DT     --上次利息支付日
,jxd_loan_info.Last_PMT_Inst_Dt                as  LAST_PRIN_REPAY_DT  --上次本金支付日
,jxd_loan_info.Next_PMT_AMT_Dt                 as  NEXT_INT_PAY_DT     --下次利息支付日
,jxd_loan_info.Next_PMT_Inst_Dt                as  NEXT_PRIN_REPAY_DT  --下次本金支付日
,CASE WHEN jxd_loan_info.Roll_Flag=0 THEN jxd_loan_info.Inst_Rate ELSE  jxd_loan_info.Roll_Rate END                as  NET_INT_RATE      --执行利率-净利率
,concat(jxd_loan_info.pmt_freq,jxd_loan_info.pmt_freq_mult)  as pay_freq
,INT_PAY_CNT                 as  INT_PAY_CNT      --利息还款次数
,PRIN_REPAY_CNT              as  PRIN_REPAY_CNT      --本金还款次数
,Is_Overdue       as is_overdue      --是否逾期
,''                                  --资方名称
,''                                  --平台号
,jxd_loan_info.out_investor_rate                --外部资方执行利率
from  (select
              *
         from
              dmf_bc.dmfbc_alm_dm_01_jxd_acct_info_s_d
         where
               dt='$TX_DATE'
               and investor_type='0'
      )jxd_loan_info
;
);


$SQL_BUFF[6]=qq(
set mapred.job.name=dmfbc_alm_dm_01_lhd_acct_info_s_d6;

use dmf_tmp;

drop table if exists dmf_tmp.tmp_odm_fi_investor_flow_s_d;
);




    #############################################################################################
    ########################################以上为SQL编辑区######################################
    #############################################################################################

    return @SQL_BUFF;
}

sub main
{
    my @sql_buff = getsql();

=description
1.串行，数组不赋值
    my @run_idxs = (
    );
2.并行:先并行运行0,1,3,6,7,10，都成功后并行运行2,4,8，都成功后并行运行5,9
    my @run_idxs = (
        [0],
        [1,3,6,7,10],
        [2,4,8],
        [5,9]
    );

=cut

    ##############################################################################################
    ##############################以下为多进程idx编辑区###########################################
    ##############################################################################################
    my @run_idxs = (
        [0],
        [1,2,3,4,5],
        [6]
    );
    ##############################################################################################
    ##############################以上为多进程idx编辑区###########################################
    ##############################################################################################


    unless (@run_idxs) {
        for (my $i=0; $i <= $#sql_buff; $i++) {
            $run_idxs[$i][0] = $i;
        }
    }

    for my $i (0 ..$#run_idxs) {
        my @parallel_list = ();
        for my $j (0 .. $#{$run_idxs[$i]}) {
            push(@parallel_list, $run_idxs[$i][$j]);
        }
        &execute_sql_paralle(\@sql_buff, \@parallel_list);

        if (%rc_hash) {
            foreach my $key (keys %rc_hash) {
                print "$key Failed\n";
            }
            return 1;
        }
    }

    return 0;
}


########################################################################################################################

sub execute_sql_paralle
{
    my ($sqls, $idxs) = @_;

    my @sql_buff = @$sqls;
    my @run_idxs = @$idxs;

    my $pm = Parallel::ForkManager->new($MAX_CONCURRENT);
    $pm->run_on_start( sub {
        my ($pid, $ident)=@_;
        print "**** $ident started, pid: $pid\n";
    });
    $pm->run_on_wait(sub {
            #            print "***** wait for children ...\n"
        },
        0.5,
    );

    $pm->run_on_finish( sub {
        my ($pid, $exit_code, $ident) = @_;
        print "run_on_finish: $ident (pid: $pid) exited with code: [$exit_code]\n";
        if ($exit_code != 0) {
            $rc_hash{$ident} = $exit_code;
        }
    });

    for (my $i = 0; $i <= $#sql_buff; $i++) {
        unless (grep {$_ eq $i} @run_idxs) { next; }
        my $pid = $pm->start("SQL_BUFF[$i]") and next;
        my $ret = Common::Hive::run_hive_sql($sql_buff[$i], ${Runner}, ${Retry_Runner});
        $pm->finish($ret);
    }
    $pm->wait_all_childs;

}


########################################################################################################################
# program section
# To see if there is one parameter,
print getCurrentDateTime(" Startup Success ..");
print "SYS          : $SYS\n";
print "JOB          : $JOB\n";
print "TX_DATE      : $TX_DATE\n";
print "TXDATE       : $TXDATE\n";
print "Target TABLE : $TABLE\n";

my $rc = main();
if ( $rc != 0 ) {
    print getCurrentDateTime("Task Execution Failed"),"\n";
}
else{
    print getCurrentDateTime("Task Execution Success"),"\n";
}
exit($rc);