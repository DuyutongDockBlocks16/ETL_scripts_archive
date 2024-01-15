#!/usr/bin/perl
########################################################################################################################
#  Creater        :zhaolicheng
#  Creation Time  :2018-09-28
#  Description    :dmfalm_alm_pd_01_position_lhd_s_d 头寸表--联合贷
#                 :
#  Modify By      :
#  Modify Time    :
#  zlc     20190807   联合贷以前的利息支付频率、本金支付频率、结息频率就是期限+期限单位改为支付频率，（和金条、小白、大白、农贷保持一致）
#  zlc     20190807   联合贷的本金还款次数和利息还款次数从固定的1改为从联合贷整合层出（和金条、小白、大白、农贷保持一致）
#  zlc     20190807   联合贷的逾期标识从有逾期天数判断改为从整合层出（和金条、小白、大白、农贷保持一致）
#  zlc     20190829   联合贷改为直接从整合层出，不再关联资方配置表去取外部资方的，因为整合层已经这样做了
#  zlc     20190906   加字段  资方id,主要是为了给有联合贷业务线使用
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
set mapred.job.name=dmfalm_alm_pd_01_position_lhd_s_d0;

alter table dmf_alm.dmfalm_alm_pd_01_position_lhd_s_d drop if exists partition (dt='$TX_DATE');
insert overwrite table dmf_alm.dmfalm_alm_pd_01_position_lhd_s_d partition(dt='$TX_DATE')
select concat(t1.Loan_Id ,'LHD')     as  ACCOUNT_NO      --账号
       ,t1.Data_Src             as  SYS_SRC      --来源系统
       ,0                       as  INC_FLAG      --增量标志
       ,t1.Acct_Org_Cd          as  ORG_CODE      --机构ID
       ,null                    as  INDS_CODE      --行业编号
       ,t1.Investment_Code      as  CUST_CODE      --客户编号
       ,t1.Product_Cd           as  PROD_CODE      --产品编号
       ,t1.Subject_Cd           as  SBJ_CODE      --科目编号
       ,case when t1.under_ratio <> 0   then  '2' else '8' end      as  SBJ_TYP      --科目类型
       ,t1.Currency_Cd          as  CUR_CODE      --币种编号
       ,0                       as  AGMT_FLAG      --协定标志
       ,null                    as  LINE_CODE      --条线
       ,t1.Should_Pay_Amt     as  CURR_BAL      --当前余额
       ,t1.Should_Pay_Amt     as  LAST_RPRC_BAL      --上次重定价余额
       ,t1.Order_Amt*t1.investment_ratio/100                    as  ORGNL_PRIN      --原始余额
       ,t1.Order_Amt*t1.investment_ratio/100-Should_Pay_Amt     as  PAID_AMT      --支付金额
       ,null                    as  UNEXP_REPAY_AMT      --提前还款金额
       ,t1.Start_Dt             as  ST_INT_DT      --起息日
       ,t1.Maturity_Dt          as  DUE_DT      --到期日
       ,concat(t1.Term,t1.Term_Mult)     as  ORGNL_TERM      --原始期限
       ,1                       as  INSTRUMENT_TYPE      --金融工具类型
       ,t1.is_overdue      as  OVRD_FLAG      --逾期标志
       ,concat(t1.Overdue_days,'D')     as  OVRD_TERM      --逾期期限
       ,'Y'                     as  INT_RATE_CYCLE      --利率周期
       ,'F'                     as  INT_RATE_TYPE      --利率类型代码
       ,t1.NET_INT_RATE             as  NET_INT_RATE      --执行利率-净利率
       ,t1.NET_INT_RATE             as  GROSS_INT_RATE      --执行利率-毛利率
       ,0                       as  COMM_FEE_RATE      --手续费率
       ,0                       as  SERV_FEE_RATE      --服务费率
       ,0                       as  COMM_FEE_FLAG      --费率标识
       ,0                       as  SERV_FEE_FLAG      --服务率标示
       ,CASE WHEN T1.PMT_TYPE IN ('1','3','4') THEN 'D' 
             WHEN T1.PMT_TYPE IN ('2','5','7','8') THEN 'M' 
        END                     as  INT_PERIOD      --计息周期
       ,2                       as  DAY_CNT_BASIS      --计息基础代码
       ,null                    as  INT_RATE_FLT_TYPE      --浮动类型代码
       ,null                    as  INT_RATE_FLT_PROPTN      --利率浮动比例
       ,null                    as  INT_RATE_FLT_SPREAD      --利率浮动利差
       ,1                       as  RPRC_TYPE      --重定价类型代码
       ,t1.LAST_RPRC_DT         as  LAST_RPRC_DT      --上次重定价日期
       ,t1.NEXT_RPRC_DT         as  NEXT_RPRC_DT      --下次重定价日期
       ,concat(t1.Term,Term_Mult)     as  RPRC_FREQ      --重定价频率
       ,t1.NET_INT_RATE         as  LAST_RPRC_INT_RATE      --上次重订价执行净利率
       ,CASE WHEN t1.PMT_TYPE='1' THEN 'A' 
             WHEN t1.PMT_TYPE='2' THEN 'B'
             WHEN t1.PMT_TYPE='3' THEN 'C'
             WHEN t1.PMT_TYPE='4' THEN 'D'
             WHEN t1.PMT_TYPE='5' THEN 'E'
             WHEN t1.PMT_TYPE='6' THEN 'G'
             WHEN t1.PMT_TYPE='7' THEN 'J'
             WHEN t1.PMT_TYPE='8' THEN 'H' 
             WHEN t1.PMT_TYPE='9' THEN 'I'
        END                     as  PAY_TYPE                 --支付类型代码
       ,t1.FST_INT_PAY_DT       as  FST_INT_PAY_DT           --首次付息日
       ,t1.First_PMT_AMT_Dt     as  FST_PRIN_REPAY_DT        --首次还本日
       ,null                    as  FST_SERV_FEE_PAY_DT      --首次还服务费日
       ,null                    as  FST_COMM_FEE_PAY_DT      --首次还手续费日
       ,t1.pay_freq             as  INT_STL_FREQ    --结息频率
       ,t1.LAST_INT_PAY_DT      as  LAST_INT_PAY_DT          --上次利息支付日
       ,null                    as  LAST_SERV_FEE_PAY_DT     --上次服务费支付日
       ,null                    as  LAST_COMM_FEE_PAY_DT     --上次手续费支付日
       ,t1.LAST_PRIN_REPAY_DT   as  LAST_PRIN_REPAY_DT       --上次本金支付日
       ,t1.NEXT_INT_PAY_DT      as  NEXT_INT_PAY_DT          --下次利息支付日
       ,t1.NEXT_PRIN_REPAY_DT   as  NEXT_PRIN_REPAY_DT       --下次本金支付日
       ,null                    as  NEXT_SERV_FEE_PAY_DT     --下次服务费支付日
       ,null                    as  NEXT_COMM_FEE_PAY_DT     --下次手续费支付日
       ,t1.pay_freq             as  INT_PAY_FREQ    --利息支付频率
       ,t1.pay_freq             as  PRIN_REPAY_FREQ --本金支付频率
       ,0                       as  PLAN_FLAG                --还款计划表标志
       ,t1.Order_Amt            as  LAST_INT_PAY_AMT         --上次利息支付金额
       ,0                       as  LAST_COMM_FEE_PAY_AMT    --上次手续费支付金额
       ,0                       as  LAST_SERV_FEE_PAY_AMT    --上次服务费支付金额
       ,0                       as  LAST_PRIN_REPAY_AMT      --上次本金支付金额
       ,0                       as  NEXT_INT_PAY_AMT         --下次利息支付金额
       ,0                       as  NEXT_COMM_FEE_PAY_AMT    --下次手续费支付金额
       ,0                       as  NEXT_SERV_FEE_PAY_AMT    --下次服务费支付金额
       ,t1.Order_Amt            as  NEXT_PRIN_REPAY_AMT      --下次本金支付金额
       ,t1.INT_PAY_CNT          as  INT_PAY_CNT              --利息还款次数
       ,t1.PRIN_REPAY_CNT       as  PRIN_REPAY_CNT           --本金还款次数
       ,null                    as  AMORT_DT                 --摊还日期
       ,null                    as  CIRCLE_PERIOD            --循环期
       ,null                    as  BIZ_RAT                  --业务评级
       ,1                       as  CUST_CATE                --客户类别
       ,null                    as  BIZ_CHNL                 --业务渠道
       ,null                    as  ISSUR_CODE               --发行人代码
       ,0                       as  OFFSHORE_FLAG            --离岸标志
       ,null                    as  INTEREST                 --外部利息
       ,null                    as  FUND_POOL_CODE           --资金池编号
       ,null                    as  BREAK_FLAG               --提前终止标示
       ,null                    as  IFC_LOAN_ASSET           --IFC流贷资产标识
       ,null                    as  IFC_LOAN                 --IFC流贷标识
       ,CASE WHEN TO_DATE(t1.Start_Dt)  <'2018-01-01' THEN 1 ELSE 0 END      as  2017_RETN      --2017存量标识
       ,null                    as G_FLOAT_PROPORTION        --毛利率浮动比例
       ,null                    as G_BASE_FLT_VALUE          --毛利率浮动利差
       ,t1.Start_Dt             as    G_LATEST_RATE_CHANGE_DATE    --毛利率最新变更日期
       ,t1.Start_Dt             as    N_LATEST_RATE_CHANGE_DATE    --净利率最新变更日期
       ,1                       as MAT_FLAG                        --有无到期日标志
       ,2                       as CST_CTR_TYPE_CD                 --成本中心分类代码
       ,CASE WHEN t1.Overdue_days>0 THEN 0 ELSE NULL END  AS SUBJECT_OVERDUE_FLAG    --资产负债逾期标志
       ,null                    as SCALE_FLAG                      --规模标志
       ,t1.loan_rate             as LAST_REPRICE_G_INT_RATE        --上次重订价执行毛利率
       ,t1.loan_rate             as BILL_INT_RATE                  --票面利率
       ,t1.investment_code       as investor_id   --资方号
from (select * from dmf_bc.dmfbc_alm_dm_01_lhd_acct_info_s_d where dt ='$TX_DATE' and Should_Pay_Amt >0  ) t1
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